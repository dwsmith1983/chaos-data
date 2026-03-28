package interlocksuite

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
)

// PostRunModule evaluates post-run conditions after a pipeline trigger has
// completed or while it is still running. It detects sensor drift, checks
// post-run rule status, and verifies baseline capture.
//
// Chain position: after Trigger, before Recovery.
//
// Scenarios covered:
//   - baseline-captured: trigger COMPLETED, all sensors fresh
//   - baseline-capture-failed: trigger COMPLETED, a sensor stale (>2h)
//   - drift-count-changed: trigger COMPLETED, sensor_count differs from __baseline_sensor_count
//   - drift-inflight: trigger RUNNING, sensor_count differs from __baseline_sensor_count
//   - post-run-rules-passed: trigger COMPLETED, post-run-status sensor status COMPLETE
//   - post-run-rules-failed: trigger COMPLETED, post-run-status sensor status != COMPLETE
//   - sensor-missing-timeout: trigger COMPLETED, post-run-status sensor stale (>1h)
type PostRunModule struct{}

// NewPostRunModule returns a new PostRunModule.
func NewPostRunModule() *PostRunModule { return &PostRunModule{} }

// Name returns the module identifier.
func (m *PostRunModule) Name() string { return "postrun" }

// Evaluate checks post-run conditions and emits the appropriate event.
// Activates based on trigger status (COMPLETED or RUNNING), not on a config
// section. Returns nil silently when the trigger is in any other state.
func (m *PostRunModule) Evaluate(ctx context.Context, p EvalParams) error {
	// Short-circuit: return nil if events already exist for this pipeline.
	existing, err := p.EventWriter.ReadEvents(ctx, p.Pipeline, "")
	if err != nil {
		return fmt.Errorf("postrun module: read existing events: %w", err)
	}
	if len(existing) > 0 {
		return nil
	}

	// Read trigger status.
	triggerStatus, err := p.Store.ReadTriggerStatus(ctx, adapter.TriggerKey{
		Pipeline: p.Pipeline,
		Schedule: "default",
		Date:     "default",
	})
	if err != nil {
		return fmt.Errorf("postrun module: read trigger status: %w", err)
	}

	statusLower := strings.ToLower(triggerStatus)
	if statusLower != "running" && statusLower != "completed" {
		return nil
	}

	now := p.Clock.Now()
	isCompleted := statusLower == "completed"

	// 1. Drift detection (covers drift-count-changed, drift-inflight).
	if drifted, driftErr := m.checkDrift(ctx, p, isCompleted, now); driftErr != nil {
		return driftErr
	} else if drifted {
		return nil
	}

	// Remaining checks apply only when trigger is COMPLETED.
	if !isCompleted {
		return nil
	}

	// 2. Post-run rules check.
	if handled, rulesErr := m.checkPostRunRules(ctx, p, now); rulesErr != nil {
		return rulesErr
	} else if handled {
		return nil
	}

	// 3. Baseline check.
	return m.checkBaseline(ctx, p, now)
}

// checkDrift iterates sensor keys looking for sensor_count vs
// __baseline_sensor_count divergence. Returns (true, nil) when an event was
// emitted.
func (m *PostRunModule) checkDrift(ctx context.Context, p EvalParams, isCompleted bool, now time.Time) (bool, error) {
	for _, key := range p.SensorKeys {
		sd, err := p.Store.ReadSensor(ctx, p.Pipeline, key)
		if err != nil || sd.Key == "" {
			continue
		}
		baseline, hasBaseline := sd.Metadata["__baseline_sensor_count"]
		current, hasCurrent := sd.Metadata["sensor_count"]
		if hasBaseline && hasCurrent && baseline != current {
			eventType := "POST_RUN_DRIFT"
			if !isCompleted {
				eventType = "POST_RUN_DRIFT_INFLIGHT"
			}
			p.EventWriter.Emit(InterlockEventRecord{
				PipelineID: p.Pipeline,
				EventType:  eventType,
				Timestamp:  now,
			})
			return true, nil
		}
	}
	return false, nil
}

// checkPostRunRules looks for a "post-run-status" sensor and evaluates its
// staleness and status. Returns (true, nil) when an event was emitted.
func (m *PostRunModule) checkPostRunRules(ctx context.Context, p EvalParams, now time.Time) (bool, error) {
	if !containsSensorKey(p.SensorKeys, "post-run-status") {
		return false, nil
	}

	sd, err := p.Store.ReadSensor(ctx, p.Pipeline, "post-run-status")
	if err != nil || sd.Key == "" {
		return false, nil
	}

	// Stale check: >= 1 hour since last update.
	if now.Sub(sd.LastUpdated) >= 1*time.Hour {
		p.EventWriter.Emit(InterlockEventRecord{
			PipelineID: p.Pipeline,
			EventType:  "POST_RUN_SENSOR_MISSING",
			Timestamp:  now,
		})
		return true, nil
	}

	status := sd.Metadata["status"]
	if strings.EqualFold(status, "COMPLETE") {
		p.EventWriter.Emit(InterlockEventRecord{
			PipelineID: p.Pipeline,
			EventType:  "POST_RUN_PASSED",
			Timestamp:  now,
		})
		return true, nil
	}

	// PARTIAL or any other non-COMPLETE status.
	p.EventWriter.Emit(InterlockEventRecord{
		PipelineID: p.Pipeline,
		EventType:  "POST_RUN_FAILED",
		Timestamp:  now,
	})
	return true, nil
}

// checkBaseline verifies that all sensors are fresh (LastUpdated within 2
// hours). Emits BASELINE_CAPTURE_FAILED if any sensor is stale, or
// POST_RUN_BASELINE_CAPTURED if all are fresh.
func (m *PostRunModule) checkBaseline(ctx context.Context, p EvalParams, now time.Time) error {
	for _, key := range p.SensorKeys {
		sd, err := p.Store.ReadSensor(ctx, p.Pipeline, key)
		if err != nil || sd.Key == "" {
			continue
		}
		if now.Sub(sd.LastUpdated) >= 2*time.Hour {
			p.EventWriter.Emit(InterlockEventRecord{
				PipelineID: p.Pipeline,
				EventType:  "BASELINE_CAPTURE_FAILED",
				Timestamp:  now,
			})
			return nil
		}
	}

	p.EventWriter.Emit(InterlockEventRecord{
		PipelineID: p.Pipeline,
		EventType:  "POST_RUN_BASELINE_CAPTURED",
		Timestamp:  now,
	})
	return nil
}

// containsSensorKey checks whether a sensor key is present in the list.
func containsSensorKey(keys []string, target string) bool {
	for _, k := range keys {
		if k == target {
			return true
		}
	}
	return false
}
