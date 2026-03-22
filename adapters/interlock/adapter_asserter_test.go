package interlock_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/dwsmith1983/chaos-data/adapters/interlock"
	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// compile-time interface checks
var (
	_ adapter.Asserter        = (*interlock.AdapterAsserter)(nil)
	_ adapter.TargetValidator = (*interlock.AdapterAsserter)(nil)
)

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

func TestAdapterAsserter_TriggerState_Success(t *testing.T) {
	t.Parallel()
	store := newMockStateStore()
	store.triggers["pipeline-a/hourly/2024-01-15"] = "succeeded"
	reader := newMockEventReader()
	aa := interlock.NewAdapterAsserter(store, reader)

	ok, err := aa.Evaluate(context.Background(), types.Assertion{
		Type: types.AssertTriggerState, Target: "pipeline-a/hourly/2024-01-15", Condition: types.CondStatusSuccess,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Error("expected true (trigger succeeded)")
	}
}

func TestAdapterAsserter_TriggerState_Killed(t *testing.T) {
	t.Parallel()
	store := newMockStateStore()
	store.triggers["pipeline-a/hourly/2024-01-15"] = "killed"
	reader := newMockEventReader()
	aa := interlock.NewAdapterAsserter(store, reader)

	ok, err := aa.Evaluate(context.Background(), types.Assertion{
		Type: types.AssertTriggerState, Target: "pipeline-a/hourly/2024-01-15", Condition: types.CondStatusKilled,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Error("expected true (trigger killed)")
	}
}

func TestAdapterAsserter_TriggerState_Timeout(t *testing.T) {
	t.Parallel()
	store := newMockStateStore()
	store.triggers["pipeline-a/hourly/2024-01-15"] = "timeout"
	reader := newMockEventReader()
	aa := interlock.NewAdapterAsserter(store, reader)

	ok, err := aa.Evaluate(context.Background(), types.Assertion{
		Type: types.AssertTriggerState, Target: "pipeline-a/hourly/2024-01-15", Condition: types.CondStatusTimeout,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Error("expected true (trigger timeout)")
	}
}

func TestAdapterAsserter_TriggerState_ReadError(t *testing.T) {
	t.Parallel()
	store := newMockStateStore()
	// trigger not in map → ReadTriggerStatus returns error
	reader := newMockEventReader()
	aa := interlock.NewAdapterAsserter(store, reader)

	_, err := aa.Evaluate(context.Background(), types.Assertion{
		Type: types.AssertTriggerState, Target: "missing/sched/date", Condition: types.CondStatusFailed,
	})
	if err == nil {
		t.Fatal("expected error when trigger not found")
	}
}

func TestAdapterAsserter_SensorState_MalformedTarget(t *testing.T) {
	t.Parallel()
	store := newMockStateStore()
	reader := newMockEventReader()
	aa := interlock.NewAdapterAsserter(store, reader)

	_, err := aa.Evaluate(context.Background(), types.Assertion{
		Type: types.AssertSensorState, Target: "no-slash", Condition: types.CondIsStale,
	})
	if err == nil {
		t.Fatal("expected error for malformed sensor target")
	}
}

func TestAdapterAsserter_EventEmitted_MalformedTarget(t *testing.T) {
	t.Parallel()
	store := newMockStateStore()
	reader := newMockEventReader()
	aa := interlock.NewAdapterAsserter(store, reader)

	_, err := aa.Evaluate(context.Background(), types.Assertion{
		Type: types.AssertEventEmitted, Target: "no-slash", Condition: types.CondExists,
	})
	if err == nil {
		t.Fatal("expected error for malformed event target")
	}
}

func TestAdapterAsserter_EventEmitted_ManifestError(t *testing.T) {
	t.Parallel()
	store := newMockStateStore()
	reader := newMockEventReader()
	reader.err = fmt.Errorf("manifest unavailable")
	aa := interlock.NewAdapterAsserter(store, reader)

	_, err := aa.Evaluate(context.Background(), types.Assertion{
		Type: types.AssertEventEmitted, Target: "sc/mut", Condition: types.CondExists,
	})
	if err == nil {
		t.Fatal("expected error when manifest fails")
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

// --- ValidateTarget tests ---

func TestAdapterAsserter_ValidateTarget(t *testing.T) {
	t.Parallel()
	store := newMockStateStore()
	reader := newMockEventReader()
	aa := interlock.NewAdapterAsserter(store, reader)

	tests := []struct {
		name    string
		a       types.Assertion
		wantErr bool
	}{
		// sensor_state: expects 2 segments (pipeline/key)
		{
			name:    "sensor_state valid 2 segments",
			a:       types.Assertion{Type: types.AssertSensorState, Target: "pipeline/key"},
			wantErr: false,
		},
		{
			name:    "sensor_state invalid 1 segment",
			a:       types.Assertion{Type: types.AssertSensorState, Target: "no-slash"},
			wantErr: true,
		},
		// trigger_state: expects 3 segments (pipeline/schedule/date)
		{
			name:    "trigger_state valid 3 segments",
			a:       types.Assertion{Type: types.AssertTriggerState, Target: "pipeline/schedule/date"},
			wantErr: false,
		},
		{
			name:    "trigger_state invalid 2 segments",
			a:       types.Assertion{Type: types.AssertTriggerState, Target: "only/two"},
			wantErr: true,
		},
		// event_emitted: expects 2 segments (scenario/mutation)
		{
			name:    "event_emitted valid 2 segments",
			a:       types.Assertion{Type: types.AssertEventEmitted, Target: "scenario/mutation"},
			wantErr: false,
		},
		{
			name:    "event_emitted invalid 1 segment",
			a:       types.Assertion{Type: types.AssertEventEmitted, Target: "no-slash"},
			wantErr: true,
		},
		// over-segmented targets must be rejected
		{
			name:    "sensor_state invalid 3 segments",
			a:       types.Assertion{Type: types.AssertSensorState, Target: "a/b/c"},
			wantErr: true,
		},
		{
			name:    "trigger_state invalid 4 segments",
			a:       types.Assertion{Type: types.AssertTriggerState, Target: "a/b/c/d"},
			wantErr: true,
		},
		{
			name:    "event_emitted invalid 3 segments",
			a:       types.Assertion{Type: types.AssertEventEmitted, Target: "a/b/c"},
			wantErr: true,
		},
		// unsupported types: ValidateTarget should return nil (not our concern)
		{
			name:    "job_state not validated by interlock",
			a:       types.Assertion{Type: types.AssertJobState, Target: "job-1"},
			wantErr: false,
		},
		{
			name:    "data_state not validated by interlock",
			a:       types.Assertion{Type: types.AssertDataState, Target: "file.jsonl"},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := aa.ValidateTarget(tt.a)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateTarget(%q/%q) error = %v, wantErr %v",
					tt.a.Type, tt.a.Target, err, tt.wantErr)
			}
		})
	}
}
