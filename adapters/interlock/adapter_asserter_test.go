package interlock_test

import (
	"context"
	"testing"

	"github.com/dwsmith1983/chaos-data/adapters/interlock"
	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// compile-time interface check
var _ adapter.Asserter = (*interlock.AdapterAsserter)(nil)

func TestAdapterAsserter_Supports(t *testing.T) {
	t.Parallel()
	store := newMockStateStore()
	reader := newMockEventReader()
	aa := interlock.NewAdapterAsserter(store, reader)

	tests := []struct {
		at   types.AssertionType
		want bool
	}{
		{types.AssertSensorState, true},
		{types.AssertTriggerState, true},
		{types.AssertEventEmitted, true},
		{types.AssertJobState, false},
		{types.AssertDataState, false},
	}
	for _, tt := range tests {
		t.Run(string(tt.at), func(t *testing.T) {
			if got := aa.Supports(tt.at); got != tt.want {
				t.Errorf("Supports(%q) = %v, want %v", tt.at, got, tt.want)
			}
		})
	}
}

func TestAdapterAsserter_SensorState_IsStale(t *testing.T) {
	t.Parallel()
	store := newMockStateStore()
	store.sensors["pipeline-a/upstream"] = adapter.SensorData{
		Pipeline: "pipeline-a",
		Key:      "upstream",
		Status:   types.SensorStatusStale,
	}
	reader := newMockEventReader()
	aa := interlock.NewAdapterAsserter(store, reader)

	ok, err := aa.Evaluate(context.Background(), types.Assertion{
		Type: types.AssertSensorState, Target: "pipeline-a/upstream", Condition: types.CondIsStale,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Error("expected true (sensor is stale)")
	}
}

func TestAdapterAsserter_SensorState_IsStale_NotStale(t *testing.T) {
	t.Parallel()
	store := newMockStateStore()
	store.sensors["pipeline-a/upstream"] = adapter.SensorData{
		Pipeline: "pipeline-a",
		Key:      "upstream",
		Status:   types.SensorStatusReady,
	}
	reader := newMockEventReader()
	aa := interlock.NewAdapterAsserter(store, reader)

	ok, err := aa.Evaluate(context.Background(), types.Assertion{
		Type: types.AssertSensorState, Target: "pipeline-a/upstream", Condition: types.CondIsStale,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok {
		t.Error("expected false (sensor is ready, not stale)")
	}
}

func TestAdapterAsserter_SensorState_Exists(t *testing.T) {
	t.Parallel()
	store := newMockStateStore()
	store.sensors["pipeline-a/upstream"] = adapter.SensorData{
		Pipeline: "pipeline-a",
		Key:      "upstream",
		Status:   types.SensorStatusReady,
	}
	reader := newMockEventReader()
	aa := interlock.NewAdapterAsserter(store, reader)

	ok, err := aa.Evaluate(context.Background(), types.Assertion{
		Type: types.AssertSensorState, Target: "pipeline-a/upstream", Condition: types.CondExists,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Error("expected true (sensor exists, non-empty key)")
	}
}

func TestAdapterAsserter_SensorState_ReadError(t *testing.T) {
	t.Parallel()
	store := newMockStateStore()
	store.readSensorErr = true
	reader := newMockEventReader()
	aa := interlock.NewAdapterAsserter(store, reader)

	_, err := aa.Evaluate(context.Background(), types.Assertion{
		Type: types.AssertSensorState, Target: "pipeline-a/upstream", Condition: types.CondIsStale,
	})
	if err == nil {
		t.Fatal("expected error when ReadSensor fails")
	}
}

func TestAdapterAsserter_TriggerState_Failed(t *testing.T) {
	t.Parallel()
	store := newMockStateStore()
	store.triggers["pipeline-a/hourly/2024-01-15"] = "failed"
	reader := newMockEventReader()
	aa := interlock.NewAdapterAsserter(store, reader)

	ok, err := aa.Evaluate(context.Background(), types.Assertion{
		Type: types.AssertTriggerState, Target: "pipeline-a/hourly/2024-01-15", Condition: types.CondStatusFailed,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Error("expected true (trigger is failed)")
	}
}

func TestAdapterAsserter_TriggerState_WasTriggered(t *testing.T) {
	t.Parallel()
	store := newMockStateStore()
	store.triggers["pipeline-a/hourly/2024-01-15"] = "triggered"
	reader := newMockEventReader()
	aa := interlock.NewAdapterAsserter(store, reader)

	ok, err := aa.Evaluate(context.Background(), types.Assertion{
		Type: types.AssertTriggerState, Target: "pipeline-a/hourly/2024-01-15", Condition: types.CondWasTriggered,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Error("expected true (trigger was triggered)")
	}
}

func TestAdapterAsserter_TriggerState_WrongSegments(t *testing.T) {
	t.Parallel()
	store := newMockStateStore()
	reader := newMockEventReader()
	aa := interlock.NewAdapterAsserter(store, reader)

	_, err := aa.Evaluate(context.Background(), types.Assertion{
		Type: types.AssertTriggerState, Target: "only/two", Condition: types.CondStatusFailed,
	})
	if err == nil {
		t.Fatal("expected error for malformed target")
	}
}

func TestAdapterAsserter_EventEmitted_Found(t *testing.T) {
	t.Parallel()
	store := newMockStateStore()
	reader := newMockEventReader()
	reader.events = []types.ChaosEvent{
		{Scenario: "stale-sensor", Mutation: "stale-sensor"},
		{Scenario: "other", Mutation: "other"},
	}
	aa := interlock.NewAdapterAsserter(store, reader)

	ok, err := aa.Evaluate(context.Background(), types.Assertion{
		Type: types.AssertEventEmitted, Target: "stale-sensor/stale-sensor", Condition: types.CondExists,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Error("expected true (event exists)")
	}
}

func TestAdapterAsserter_EventEmitted_NotFound(t *testing.T) {
	t.Parallel()
	store := newMockStateStore()
	reader := newMockEventReader()
	reader.events = []types.ChaosEvent{
		{Scenario: "other", Mutation: "other"},
	}
	aa := interlock.NewAdapterAsserter(store, reader)

	ok, err := aa.Evaluate(context.Background(), types.Assertion{
		Type: types.AssertEventEmitted, Target: "stale-sensor/stale-sensor", Condition: types.CondExists,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok {
		t.Error("expected false (event not found)")
	}
}

func TestAdapterAsserter_UnsupportedType(t *testing.T) {
	t.Parallel()
	store := newMockStateStore()
	reader := newMockEventReader()
	aa := interlock.NewAdapterAsserter(store, reader)

	_, err := aa.Evaluate(context.Background(), types.Assertion{
		Type: types.AssertJobState, Target: "job-1", Condition: types.CondStatusFailed,
	})
	if err == nil {
		t.Fatal("expected error for unsupported type")
	}
}
