package interlocksuite

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	interlocktypes "github.com/dwsmith1983/interlock/pkg/types"
	interlockvalidation "github.com/dwsmith1983/interlock/pkg/validation"
	"gopkg.in/yaml.v3"
)

// ValidationModule evaluates Interlock validation rules against sensor state
// and emits JOB_TRIGGERED or VALIDATION_EXHAUSTED.
type ValidationModule struct{}

// NewValidationModule returns a new ValidationModule.
func NewValidationModule() *ValidationModule { return &ValidationModule{} }

// Name returns the module identifier.
func (m *ValidationModule) Name() string { return "validation" }

// Evaluate reads validation rules from the pipeline config, builds a sensor
// data map from the state store, and delegates to the Interlock validation
// engine. Emits JOB_TRIGGERED if rules pass, VALIDATION_EXHAUSTED otherwise.
// Silently returns nil when the config has no validation section.
func (m *ValidationModule) Evaluate(ctx context.Context, p EvalParams) error {
	// Terminal trigger check: if trigger is already completed/failed,
	// exit silently to let PostRun/Recovery modules handle it.
	triggerKey := adapter.TriggerKey{Pipeline: p.Pipeline, Schedule: "default", Date: "default"}
	triggerStatus, _ := p.Store.ReadTriggerStatus(ctx, triggerKey)
	if isTerminalStatus(triggerStatus) {
		return nil
	}

	// Extract validation section from config map, then marshal only that
	// sub-map for typed deserialization into interlock rule structs.
	valRaw, ok := p.Config["validation"]
	if !ok {
		return nil // no validation section — skip
	}
	valSection, ok := valRaw.(map[string]any)
	if !ok {
		return nil // malformed validation section — skip
	}

	sectionBytes, err := yaml.Marshal(valSection)
	if err != nil {
		return fmt.Errorf("validation module: marshal validation section: %w", err)
	}

	var vc struct {
		Trigger string                          `yaml:"trigger" json:"trigger"`
		Rules   []interlocktypes.ValidationRule `yaml:"rules" json:"rules"`
	}
	if err := yaml.Unmarshal(sectionBytes, &vc); err != nil {
		if err2 := json.Unmarshal(sectionBytes, &vc); err2 != nil {
			return fmt.Errorf("validation module: parse config: yaml=%v, json=%v", err, err2)
		}
	}

	if len(vc.Rules) == 0 {
		return nil // no validation rules — skip
	}

	// Build sensor data map from state store.
	// Rule keys may include a SENSOR# prefix (interlock convention). Strip it
	// before calling ReadSensor since the harness writes bare keys.
	sensors := make(map[string]map[string]any)
	for _, rule := range vc.Rules {
		bareKey := strings.TrimPrefix(rule.Key, "SENSOR#")
		sensorData, err := p.Store.ReadSensor(ctx, p.Pipeline, bareKey)
		if err != nil {
			continue
		}
		if sensorData.Key != "" {
			data := make(map[string]any)
			data["status"] = string(sensorData.Status)
			for k, v := range sensorData.Metadata {
				data[k] = v
			}
			sensors[rule.Key] = data
		}
	}

	// Evaluate rules using Interlock's validation engine.
	result := interlockvalidation.EvaluateRules(
		vc.Trigger,
		vc.Rules,
		sensors,
		p.Clock.Now(),
	)

	if result.Passed {
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
