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
	case types.AssertSensorState, types.AssertTriggerState, types.AssertEventEmitted:
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
	default:
		return false, fmt.Errorf("unsupported assertion type: %q", assertion.Type)
	}
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
