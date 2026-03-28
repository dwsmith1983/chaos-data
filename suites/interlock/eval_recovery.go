package interlocksuite

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// RecoveryModule evaluates recovery conditions after a pipeline trigger has
// completed, failed, or is still running. It detects rerun eligibility, retry
// exhaustion, poll timeouts, false-success job failures, and trigger recovery
// when all sensors are ready.
//
// Chain position: after PostRun.
//
// Scenarios covered:
//   - rerun-accepted: trigger COMPLETED, current_drift_reruns < max_drift_reruns
//   - rerun-rejected: trigger COMPLETED, current_drift_reruns >= max_drift_reruns
//   - retry-exhausted: trigger FAILED, current_retries >= max_retries
//   - job-poll-exhausted: trigger RUNNING, elapsed > poll_timeout_minutes
//   - job-failed-false-success: trigger "succeeded", current_retries < max_code_retries → write rerun
//   - trigger-recovered: trigger RUNNING, all sensors ready/complete
type RecoveryModule struct{}

// NewRecoveryModule returns a new RecoveryModule.
func NewRecoveryModule() *RecoveryModule { return &RecoveryModule{} }

// Name returns the module identifier.
func (m *RecoveryModule) Name() string { return "recovery" }

// Evaluate checks recovery conditions and emits the appropriate event.
// Activates based on the presence of a "recovery" config section or terminal
// trigger statuses. Short-circuits when a terminal event already exists.
// Returns nil silently when the config does not contain a recovery section and
// no relevant trigger status is present.
func (m *RecoveryModule) Evaluate(ctx context.Context, p EvalParams) error {
	// Short-circuit: if a terminal event already exists, skip.
	existing, err := p.EventWriter.ReadEvents(ctx, p.Pipeline, "")
	if err != nil {
		return fmt.Errorf("recovery module: read existing events: %w", err)
	}
	for _, ev := range existing {
		if isTerminalRecoveryEvent(ev.EventType) {
			return nil
		}
	}

	// Read trigger status early so we can handle terminal states (killed,
	// timeout) regardless of whether a recovery config section exists.
	triggerStatus, err := p.Store.ReadTriggerStatus(ctx, adapter.TriggerKey{
		Pipeline: p.Pipeline,
		Schedule: "default",
		Date:     "default",
	})
	if err != nil {
		return fmt.Errorf("recovery module: read trigger status: %w", err)
	}

	statusLower := strings.ToLower(triggerStatus)
	now := p.Clock.Now()

	// Terminal trigger states that indicate job failure — fires regardless of
	// whether a recovery config section exists.
	if statusLower == "killed" || statusLower == "timeout" {
		p.EventWriter.Emit(InterlockEventRecord{
			PipelineID: p.Pipeline,
			EventType:  "JOB_FAILED",
			Timestamp:  now,
		})
		return nil
	}

	// No recovery config section → skip remaining recovery checks.
	recoveryRaw, ok := p.Config["recovery"]
	if !ok {
		return nil
	}
	recoveryMap, ok := recoveryRaw.(map[string]any)
	if !ok {
		return nil
	}

	// 1. Poll timeout: trigger RUNNING and elapsed > poll_timeout_minutes.
	if statusLower == "running" {
		if emitted, emitErr := m.checkPollTimeout(ctx, p, recoveryMap, now); emitErr != nil {
			return emitErr
		} else if emitted {
			return nil
		}
	}

	// 2. Trigger recovered: trigger RUNNING, all sensors ready/complete.
	if statusLower == "running" {
		if emitted, emitErr := m.checkTriggerRecovered(ctx, p, recoveryMap, now); emitErr != nil {
			return emitErr
		} else if emitted {
			return nil
		}
	}

	// 3. False-success job failure: trigger "succeeded" but has code retries configured.
	if statusLower == "succeeded" {
		if emitted, emitErr := m.checkFalseSuccess(ctx, p, recoveryMap, now); emitErr != nil {
			return emitErr
		} else if emitted {
			return nil
		}
	}

	// 4. Retry exhausted: trigger FAILED.
	if statusLower == "failed" {
		if emitted, emitErr := m.checkRetryExhausted(ctx, p, recoveryMap, now); emitErr != nil {
			return emitErr
		} else if emitted {
			return nil
		}
	}

	// 5. Drift rerun: trigger COMPLETED.
	if statusLower == "completed" {
		return m.checkDriftRerun(ctx, p, recoveryMap, now)
	}

	return nil
}

// checkPollTimeout emits JOB_POLL_EXHAUSTED when the trigger has been running
// longer than poll_timeout_minutes. Returns (true, nil) when emitted.
func (m *RecoveryModule) checkPollTimeout(ctx context.Context, p EvalParams, recoveryMap map[string]any, now time.Time) (bool, error) {
	timeoutMin := configInt(recoveryMap, "poll_timeout_minutes", 0)
	if timeoutMin <= 0 {
		return false, nil
	}

	// Read trigger time from sensor metadata.
	triggerTime, found := m.readSensorMetadataTime(ctx, p, "trigger_time", "schedule_time")
	if !found {
		return false, nil
	}

	elapsed := now.Sub(triggerTime).Minutes()
	if elapsed > float64(timeoutMin) {
		p.EventWriter.Emit(InterlockEventRecord{
			PipelineID: p.Pipeline,
			EventType:  "JOB_POLL_EXHAUSTED",
			Timestamp:  now,
		})
		return true, nil
	}

	return false, nil
}

// checkTriggerRecovered emits TRIGGER_RECOVERED when the trigger is RUNNING
// and all sensors are ready or complete. Returns (true, nil) when emitted.
func (m *RecoveryModule) checkTriggerRecovered(ctx context.Context, p EvalParams, recoveryMap map[string]any, now time.Time) (bool, error) {
	// stale_threshold_minutes gates whether this check applies.
	thresholdMin := configInt(recoveryMap, "stale_threshold_minutes", 0)
	if thresholdMin <= 0 {
		return false, nil
	}

	if len(p.SensorKeys) == 0 {
		return false, nil
	}

	threshold := time.Duration(thresholdMin) * time.Minute
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
		// Sensor must be fresh (updated within threshold).
		if now.Sub(sd.LastUpdated) > threshold {
			allReady = false
			break
		}
	}

	if allReady {
		p.EventWriter.Emit(InterlockEventRecord{
			PipelineID: p.Pipeline,
			EventType:  "TRIGGER_RECOVERED",
			Timestamp:  now,
		})
		return true, nil
	}

	return false, nil
}

// checkFalseSuccess detects a job reporting "succeeded" when max_code_retries
// is configured and current retries are below the max. Emits JOB_FAILED and
// writes a rerun record. Returns (true, nil) when emitted.
func (m *RecoveryModule) checkFalseSuccess(ctx context.Context, p EvalParams, recoveryMap map[string]any, now time.Time) (bool, error) {
	maxCodeRetries := configInt(recoveryMap, "max_code_retries", 0)
	if maxCodeRetries <= 0 {
		return false, nil
	}

	currentRetries := configInt(recoveryMap, "current_retries", 0)
	if currentRetries >= maxCodeRetries {
		return false, nil
	}

	// False success: job said "succeeded" but we have retries left — treat as failure.
	p.EventWriter.Emit(InterlockEventRecord{
		PipelineID: p.Pipeline,
		EventType:  "JOB_FAILED",
		Timestamp:  now,
	})

	if err := p.Store.WriteRerun(ctx, p.Pipeline, "default", "default", "false_success"); err != nil {
		return true, fmt.Errorf("recovery module: write rerun: %w", err)
	}

	return true, nil
}

// checkRetryExhausted emits RETRY_EXHAUSTED when current_retries >= max_retries.
// Returns (true, nil) when emitted.
func (m *RecoveryModule) checkRetryExhausted(_ context.Context, p EvalParams, recoveryMap map[string]any, now time.Time) (bool, error) {
	maxRetries := configInt(recoveryMap, "max_retries", 0)
	if maxRetries <= 0 {
		return false, nil
	}

	currentRetries := configInt(recoveryMap, "current_retries", 0)
	if currentRetries >= maxRetries {
		p.EventWriter.Emit(InterlockEventRecord{
			PipelineID: p.Pipeline,
			EventType:  "RETRY_EXHAUSTED",
			Timestamp:  now,
		})
		return true, nil
	}

	return false, nil
}

// checkDriftRerun emits RERUN_ACCEPTED when current_drift_reruns < max_drift_reruns,
// or RERUN_REJECTED when current >= max. Returns (true, nil) when emitted.
func (m *RecoveryModule) checkDriftRerun(_ context.Context, p EvalParams, recoveryMap map[string]any, now time.Time) error {
	maxReruns := configInt(recoveryMap, "max_drift_reruns", 0)
	if maxReruns <= 0 {
		return nil
	}

	currentReruns := configInt(recoveryMap, "current_drift_reruns", 0)
	if currentReruns < maxReruns {
		p.EventWriter.Emit(InterlockEventRecord{
			PipelineID: p.Pipeline,
			EventType:  "RERUN_ACCEPTED",
			Timestamp:  now,
		})
	} else {
		p.EventWriter.Emit(InterlockEventRecord{
			PipelineID: p.Pipeline,
			EventType:  "RERUN_REJECTED",
			Timestamp:  now,
		})
	}

	return nil
}

// readSensorMetadataTime reads the first matching metadata key from any sensor
// in p.SensorKeys and parses it as RFC3339Nano. Returns the parsed time and
// true if found.
func (m *RecoveryModule) readSensorMetadataTime(ctx context.Context, p EvalParams, keys ...string) (time.Time, bool) {
	for _, sensorKey := range p.SensorKeys {
		sd, err := p.Store.ReadSensor(ctx, p.Pipeline, sensorKey)
		if err != nil || sd.Key == "" {
			continue
		}
		for _, metaKey := range keys {
			if val, ok := sd.Metadata[metaKey]; ok && val != "" {
				t, parseErr := time.Parse(time.RFC3339Nano, val)
				if parseErr == nil {
					return t, true
				}
			}
		}
	}
	return time.Time{}, false
}

// isTerminalRecoveryEvent returns true for event types that represent a final
// recovery decision, causing subsequent recovery evaluation to short-circuit.
func isTerminalRecoveryEvent(eventType string) bool {
	switch eventType {
	case "RERUN_REJECTED", "RETRY_EXHAUSTED", "JOB_POLL_EXHAUSTED",
		"JOB_FAILED", "PIPELINE_EXCLUDED", "VALIDATION_EXHAUSTED":
		return true
	default:
		return false
	}
}

// configInt extracts an int from a map key. Handles float64, int, and string
// representations. Returns defaultVal when the key is absent or not
// convertible.
func configInt(m map[string]any, key string, defaultVal int) int {
	v, ok := m[key]
	if !ok {
		return defaultVal
	}
	switch tv := v.(type) {
	case float64:
		return int(tv)
	case int:
		return tv
	case string:
		n, err := strconv.Atoi(tv)
		if err != nil {
			return defaultVal
		}
		return n
	default:
		return defaultVal
	}
}
