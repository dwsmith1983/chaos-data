package interlocksuite

import (
	"context"
	"fmt"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// SafetyModule runs FIRST in the module chain and handles calendar exclusion
// and dry-run scenarios. It short-circuits when events already exist for the
// pipeline, preventing duplicate emissions.
type SafetyModule struct{}

// NewSafetyModule returns a new SafetyModule.
func NewSafetyModule() *SafetyModule { return &SafetyModule{} }

// Name returns the module identifier.
func (m *SafetyModule) Name() string { return "safety" }

// Evaluate checks calendar exclusion and dry-run conditions, emitting
// PIPELINE_EXCLUDED, DRY_RUN_WOULD_TRIGGER, or DRY_RUN_DRIFT as appropriate.
// Returns nil silently when the config contains no schedule or dryrun section.
func (m *SafetyModule) Evaluate(ctx context.Context, p EvalParams) error {
	// 1. Short-circuit: if events already exist for this pipeline, skip.
	existing, err := p.EventWriter.ReadEvents(ctx, p.Pipeline, "")
	if err != nil {
		return fmt.Errorf("safety module: read existing events: %w", err)
	}
	if len(existing) > 0 {
		return nil
	}

	// 2. Calendar exclusion.
	if excluded, emitErr := m.evaluateCalendar(ctx, p); emitErr != nil {
		return emitErr
	} else if excluded {
		return nil
	}

	// 3. Dry-run checks.
	if err := m.evaluateDryRun(ctx, p); err != nil {
		return err
	}

	return nil
}

// evaluateCalendar checks schedule config for exclusion/inclusion calendars.
// Returns (true, nil) when PIPELINE_EXCLUDED was emitted, (false, nil) when
// no calendar match, or (false, err) on failure.
func (m *SafetyModule) evaluateCalendar(_ context.Context, p EvalParams) (bool, error) {
	schedRaw, ok := p.Config["schedule"]
	if !ok {
		return false, nil
	}
	schedMap, ok := schedRaw.(map[string]any)
	if !ok {
		return false, nil
	}

	today := p.Clock.Now().Format("2006-01-02")
	schedType := configString(schedMap, "type")

	switch schedType {
	case "exclusion_calendar":
		dates := configStringSlice(schedMap, "excluded_dates")
		if containsDate(dates, today) {
			p.EventWriter.Emit(InterlockEventRecord{
				PipelineID: p.Pipeline,
				EventType:  "PIPELINE_EXCLUDED",
				Timestamp:  p.Clock.Now(),
			})
			return true, nil
		}

	case "inclusion_calendar":
		dates := configStringSlice(schedMap, "dates")
		if !containsDate(dates, today) {
			p.EventWriter.Emit(InterlockEventRecord{
				PipelineID: p.Pipeline,
				EventType:  "PIPELINE_EXCLUDED",
				Timestamp:  p.Clock.Now(),
			})
			return true, nil
		}
	}

	return false, nil
}

// evaluateDryRun checks dry-run config and emits DRY_RUN_WOULD_TRIGGER or
// DRY_RUN_DRIFT when conditions are met.
func (m *SafetyModule) evaluateDryRun(ctx context.Context, p EvalParams) error {
	if !configBool(p.Config, "dryrun") {
		return nil
	}

	// Idempotency: if dryrun marker already exists, suppress.
	if configBool(p.Config, "dryrun_marker_exists") {
		return nil
	}

	// Check trigger status for drift detection.
	triggerStatus, err := p.Store.ReadTriggerStatus(ctx, adapter.TriggerKey{
		Pipeline: p.Pipeline,
		Schedule: "default",
		Date:     "default",
	})
	if err != nil {
		return fmt.Errorf("safety module: read trigger status: %w", err)
	}

	if triggerStatus == "COMPLETED" {
		if m.hasSensorDrift(ctx, p) {
			p.EventWriter.Emit(InterlockEventRecord{
				PipelineID: p.Pipeline,
				EventType:  "DRY_RUN_DRIFT",
				Timestamp:  p.Clock.Now(),
			})
			return nil
		}
	}

	// Check sensor readiness: all sensors ready → DRY_RUN_WOULD_TRIGGER.
	if len(p.SensorKeys) > 0 && m.allSensorsReady(ctx, p) {
		p.EventWriter.Emit(InterlockEventRecord{
			PipelineID: p.Pipeline,
			EventType:  "DRY_RUN_WOULD_TRIGGER",
			Timestamp:  p.Clock.Now(),
		})
	}

	return nil
}

// hasSensorDrift checks whether any sensor's current sensor_count differs from
// the __baseline_sensor_count stored in metadata.
func (m *SafetyModule) hasSensorDrift(ctx context.Context, p EvalParams) bool {
	for _, key := range p.SensorKeys {
		sd, err := p.Store.ReadSensor(ctx, p.Pipeline, key)
		if err != nil || sd.Key == "" {
			continue
		}
		baseline, hasBaseline := sd.Metadata["__baseline_sensor_count"]
		current, hasCurrent := sd.Metadata["sensor_count"]
		if hasBaseline && hasCurrent && baseline != current {
			return true
		}
	}
	return false
}

// allSensorsReady returns true when every sensor key has a ready or complete
// status in the state store.
func (m *SafetyModule) allSensorsReady(ctx context.Context, p EvalParams) bool {
	for _, key := range p.SensorKeys {
		sd, err := p.Store.ReadSensor(ctx, p.Pipeline, key)
		if err != nil || sd.Key == "" {
			return false
		}
		if sd.Status != types.SensorStatusReady && sd.Status != types.SensorStatusComplete {
			return false
		}
	}
	return true
}

// ---------------------------------------------------------------------------
// Config helpers — extract values safely from map[string]any
// ---------------------------------------------------------------------------

// configString returns the string value for a key, or "" if absent/wrong type.
func configString(m map[string]any, key string) string {
	v, ok := m[key]
	if !ok {
		return ""
	}
	s, ok := v.(string)
	if !ok {
		return ""
	}
	return s
}

// configBool returns true if the key holds bool true or string "true".
func configBool(m map[string]any, key string) bool {
	v, ok := m[key]
	if !ok {
		return false
	}
	switch tv := v.(type) {
	case bool:
		return tv
	case string:
		return tv == "true"
	default:
		return false
	}
}

// configStringSlice extracts a []string from a map key that may hold
// []any (JSON/YAML decoded) or []string.
func configStringSlice(m map[string]any, key string) []string {
	v, ok := m[key]
	if !ok {
		return nil
	}
	switch tv := v.(type) {
	case []string:
		return tv
	case []any:
		out := make([]string, 0, len(tv))
		for _, item := range tv {
			if s, ok := item.(string); ok {
				out = append(out, s)
			}
		}
		return out
	default:
		return nil
	}
}

// containsDate checks whether the date string is present in the slice.
func containsDate(dates []string, target string) bool {
	for _, d := range dates {
		if d == target {
			return true
		}
	}
	return false
}
