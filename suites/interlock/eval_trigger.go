package interlocksuite

import (
	"context"
	"fmt"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// TriggerModule handles trigger-type scenarios where no validation rules are
// configured. When a pipeline config exists but has no validation rules, this
// module checks whether sensors are still pending and emits VALIDATION_EXHAUSTED
// (sensors not ready) or JOB_TRIGGERED (all sensors ready).
//
// This module short-circuits when the ValidationModule has already emitted an
// event for the current pipeline — preventing duplicate terminal events.
type TriggerModule struct{}

// NewTriggerModule returns a new TriggerModule.
func NewTriggerModule() *TriggerModule { return &TriggerModule{} }

// Name returns the module identifier.
func (m *TriggerModule) Name() string { return "trigger" }

// Evaluate checks whether the pipeline config has a job section but no
// validation rules, then determines the appropriate event based on sensor
// readiness. Silently returns nil when the config has validation rules
// (handled by ValidationModule) or when no job section exists.
func (m *TriggerModule) Evaluate(ctx context.Context, p EvalParams) error {
	// Short-circuit if another module already emitted an event.
	existing, err := p.EventWriter.ReadEvents(ctx, p.Pipeline, "")
	if err != nil {
		return fmt.Errorf("trigger module: read existing events: %w", err)
	}
	if len(existing) > 0 {
		return nil
	}

	// Terminal trigger check: if trigger is already completed/failed,
	// exit silently to let PostRun/Recovery modules handle it.
	triggerKey := adapter.TriggerKey{Pipeline: p.Pipeline, Schedule: "default", Date: "default"}
	triggerStatus, err2 := p.Store.ReadTriggerStatus(ctx, triggerKey)
	if err2 != nil {
		return fmt.Errorf("trigger module: read trigger status: %w", err2)
	}
	if isTerminalStatus(triggerStatus) {
		return nil
	}

	// Check if config has validation rules — if so, skip (ValidationModule owns this).
	if hasValidationRules(p.Config) {
		return nil // has validation rules — ValidationModule handles this
	}

	// No validation rules. Check sensor readiness using keys from setup.
	// If any sensor is in a non-ready state, emit VALIDATION_EXHAUSTED.
	if len(p.SensorKeys) == 0 {
		// No sensors configured — emit VALIDATION_EXHAUSTED (nothing to validate).
		p.EventWriter.Emit(InterlockEventRecord{
			PipelineID: p.Pipeline,
			EventType:  "VALIDATION_EXHAUSTED",
			Timestamp:  p.Clock.Now(),
		})
		return nil
	}

	allReady := true
	for _, key := range p.SensorKeys {
		sd, err := p.Store.ReadSensor(ctx, p.Pipeline, key)
		if err != nil || sd.Key == "" {
			allReady = false
			break
		}
		if sd.Status != types.SensorStatusReady && sd.Status != types.SensorStatusComplete {
			allReady = false
			break
		}
	}

	if allReady {
		p.EventWriter.Emit(InterlockEventRecord{
			PipelineID: p.Pipeline,
			EventType:  "JOB_TRIGGERED",
			Timestamp:  p.Clock.Now(),
		})
	} else {
		p.EventWriter.Emit(InterlockEventRecord{
			PipelineID: p.Pipeline,
			EventType:  "VALIDATION_EXHAUSTED",
			Timestamp:  p.Clock.Now(),
		})
	}

	return nil
}

// hasValidationRules checks whether the config map contains a non-empty
// validation rules slice, without marshalling to YAML/JSON.
func hasValidationRules(config map[string]any) bool {
	val, ok := config["validation"]
	if !ok {
		return false
	}
	valMap, ok := val.(map[string]any)
	if !ok {
		return false
	}
	rules, ok := valMap["rules"].([]any)
	return ok && len(rules) > 0
}
