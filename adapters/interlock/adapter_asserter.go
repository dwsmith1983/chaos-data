package interlock

import (
	"context"
	"fmt"
	"strings"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// AdapterAsserter implements adapter.Asserter by delegating to StateStore
// and EventReader with single-shot, non-blocking checks.
type AdapterAsserter struct {
	store  adapter.StateStore
	reader adapter.EventReader
}

// NewAdapterAsserter creates an AdapterAsserter.
func NewAdapterAsserter(store adapter.StateStore, reader adapter.EventReader) *AdapterAsserter {
	return &AdapterAsserter{store: store, reader: reader}
}

// Supports reports whether this asserter handles the given assertion type.
func (a *AdapterAsserter) Supports(at types.AssertionType) bool {
	switch at {
	case types.AssertSensorState, types.AssertTriggerState, types.AssertEventEmitted,
		types.AssertInterlockEvent, types.AssertJobState, types.AssertRerunState:
		return true
	default:
		return false
	}
}

// Evaluate checks whether the assertion's condition currently holds.
func (a *AdapterAsserter) Evaluate(ctx context.Context, assertion types.Assertion) (bool, error) {
	switch assertion.Type {
	case types.AssertSensorState:
		return a.evalSensor(ctx, assertion)
	case types.AssertTriggerState:
		return a.evalTrigger(ctx, assertion)
	case types.AssertEventEmitted:
		return a.evalEvent(ctx, assertion)
	case types.AssertInterlockEvent:
		return a.evalInterlockEvent(ctx, assertion)
	case types.AssertJobState:
		return a.evalJobState(ctx, assertion)
	case types.AssertRerunState:
		return a.evalRerunState(ctx, assertion)
	default:
		return false, fmt.Errorf("unsupported assertion type: %q", assertion.Type)
	}
}

// ValidateTarget validates the target format for supported assertion types.
// Called by the engine at load time for fail-fast on malformed targets.
func (a *AdapterAsserter) ValidateTarget(assertion types.Assertion) error {
	switch assertion.Type {
	case types.AssertSensorState:
		if strings.Count(assertion.Target, "/") != 1 {
			return fmt.Errorf("sensor_state target %q: expected pipeline/key", assertion.Target)
		}
	case types.AssertTriggerState:
		if strings.Count(assertion.Target, "/") != 2 {
			return fmt.Errorf("trigger_state target %q: expected pipeline/schedule/date", assertion.Target)
		}
	case types.AssertEventEmitted:
		if strings.Count(assertion.Target, "/") != 1 {
			return fmt.Errorf("event_emitted target %q: expected scenario/mutation", assertion.Target)
		}
	case types.AssertInterlockEvent:
		if assertion.Target == "" {
			return fmt.Errorf("interlock_event target must not be empty")
		}
	case types.AssertJobState:
		if strings.Count(assertion.Target, "/") != 2 {
			return fmt.Errorf("job_state target %q: expected pipeline/schedule/date", assertion.Target)
		}
	case types.AssertRerunState:
		if strings.Count(assertion.Target, "/") != 2 {
			return fmt.Errorf("rerun_state target %q: expected pipeline/schedule/date", assertion.Target)
		}
	}
	return nil
}

func (a *AdapterAsserter) evalSensor(ctx context.Context, assertion types.Assertion) (bool, error) {
	parts := strings.SplitN(assertion.Target, "/", 2)
	if len(parts) != 2 {
		return false, fmt.Errorf("sensor_state target %q: expected pipeline/key", assertion.Target)
	}
	sensor, err := a.store.ReadSensor(ctx, parts[0], parts[1])
	if err != nil {
		return false, fmt.Errorf("eval sensor %q: %w", assertion.Target, err)
	}
	switch assertion.Condition {
	case types.CondIsStale:
		return sensor.Status == types.SensorStatusStale, nil
	case types.CondIsReady:
		return sensor.Status == types.SensorStatusReady, nil
	case types.CondIsPending:
		return sensor.Status == types.SensorStatusPending, nil
	case types.CondStatusRunning:
		return sensor.Status == "running", nil
	case types.CondStatusStopped:
		return sensor.Status == "stopped", nil
	case types.CondExists:
		return sensor.Key != "", nil
	default:
		return false, fmt.Errorf("unsupported sensor condition: %q", assertion.Condition)
	}
}

func (a *AdapterAsserter) evalTrigger(ctx context.Context, assertion types.Assertion) (bool, error) {
	parts := strings.SplitN(assertion.Target, "/", 3)
	if len(parts) != 3 {
		return false, fmt.Errorf("trigger_state target %q: expected pipeline/schedule/date", assertion.Target)
	}
	key := adapter.TriggerKey{Pipeline: parts[0], Schedule: parts[1], Date: parts[2]}
	status, err := a.store.ReadTriggerStatus(ctx, key)
	if err != nil {
		return false, fmt.Errorf("eval trigger %q: %w", assertion.Target, err)
	}
	var expected string
	switch assertion.Condition {
	case types.CondStatusFailed:
		expected = "failed"
	case types.CondStatusSuccess:
		expected = "succeeded"
	case types.CondStatusKilled:
		expected = "killed"
	case types.CondStatusTimeout:
		expected = "timeout"
	case types.CondWasTriggered:
		expected = "triggered"
	case types.CondStatusRunning:
		expected = "running"
	case types.CondStatusStopped:
		expected = "stopped"
	default:
		return false, fmt.Errorf("unsupported trigger condition: %q", assertion.Condition)
	}
	return status == expected, nil
}

func (a *AdapterAsserter) evalEvent(ctx context.Context, assertion types.Assertion) (bool, error) {
	parts := strings.SplitN(assertion.Target, "/", 2)
	if len(parts) != 2 {
		return false, fmt.Errorf("event_emitted target %q: expected scenario/mutation", assertion.Target)
	}
	events, err := a.reader.Manifest(ctx)
	if err != nil {
		return false, fmt.Errorf("eval event %q: %w", assertion.Target, err)
	}
	for _, e := range events {
		if e.Scenario == parts[0] && e.Mutation == parts[1] {
			return true, nil
		}
	}
	return false, nil
}

func (a *AdapterAsserter) evalInterlockEvent(ctx context.Context, assertion types.Assertion) (bool, error) {
	events, err := a.reader.Manifest(ctx)
	if err != nil {
		return false, fmt.Errorf("eval interlock_event %q: %w", assertion.Target, err)
	}
	for _, e := range events {
		if e.Scenario == assertion.Target {
			return true, nil
		}
	}
	return false, nil
}

func (a *AdapterAsserter) evalJobState(ctx context.Context, assertion types.Assertion) (bool, error) {
	parts := strings.SplitN(assertion.Target, "/", 3)
	if len(parts) != 3 {
		return false, fmt.Errorf("job_state target %q: expected pipeline/schedule/date", assertion.Target)
	}
	events, err := a.store.ReadJobEvents(ctx, parts[0], parts[1], parts[2])
	if err != nil {
		return false, fmt.Errorf("eval job_state %q: %w", assertion.Target, err)
	}
	if len(events) == 0 {
		// No events: only is_pending is true when there are no events.
		return assertion.Condition == types.CondIsPending, nil
	}
	latest := events[0] // ReadJobEvents returns DESC order; first is most recent.
	switch assertion.Condition {
	case types.CondStatusFailed:
		return latest.Event == "failed", nil
	case types.CondStatusSuccess:
		return latest.Event == "completed" || latest.Event == "succeeded", nil
	case types.CondStatusRunning:
		return latest.Event == "started" || latest.Event == "running", nil
	case types.CondStatusKilled:
		return latest.Event == "killed", nil
	case types.CondIsPending:
		return false, nil // events exist, so not pending
	default:
		return false, fmt.Errorf("unsupported job_state condition: %q", assertion.Condition)
	}
}

func (a *AdapterAsserter) evalRerunState(ctx context.Context, assertion types.Assertion) (bool, error) {
	parts := strings.SplitN(assertion.Target, "/", 3)
	if len(parts) != 3 {
		return false, fmt.Errorf("rerun_state target %q: expected pipeline/schedule/date", assertion.Target)
	}
	count, err := a.store.CountReruns(ctx, parts[0], parts[1], parts[2])
	if err != nil {
		return false, fmt.Errorf("eval rerun_state %q: %w", assertion.Target, err)
	}
	switch assertion.Condition {
	case types.CondExists:
		return count > 0, nil
	case types.CondNotExists:
		return count == 0, nil
	default:
		return false, fmt.Errorf("unsupported rerun_state condition: %q", assertion.Condition)
	}
}
