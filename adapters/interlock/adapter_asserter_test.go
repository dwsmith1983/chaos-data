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
		{types.AssertInterlockEvent, true},
		{types.AssertJobState, true},
		{types.AssertRerunState, true},
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
	store.Sensors["pipeline-a/upstream"] = adapter.SensorData{
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
	store.Sensors["pipeline-a/upstream"] = adapter.SensorData{
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
	store.Sensors["pipeline-a/upstream"] = adapter.SensorData{
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
	store.ReadSensorErr = true
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
	store.Triggers["pipeline-a/hourly/2024-01-15"] = "failed"
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
	store.Triggers["pipeline-a/hourly/2024-01-15"] = "triggered"
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
	store.Triggers["pipeline-a/hourly/2024-01-15"] = "succeeded"
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
	store.Triggers["pipeline-a/hourly/2024-01-15"] = "killed"
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
	store.Triggers["pipeline-a/hourly/2024-01-15"] = "timeout"
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
		Type: types.AssertionType("unknown_type"), Target: "whatever", Condition: types.CondStatusFailed,
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
		// interlock_event: just non-empty
		{
			name:    "interlock_event valid non-empty",
			a:       types.Assertion{Type: types.AssertInterlockEvent, Target: "POST_RUN_DRIFT"},
			wantErr: false,
		},
		{
			name:    "interlock_event invalid empty",
			a:       types.Assertion{Type: types.AssertInterlockEvent, Target: ""},
			wantErr: true,
		},
		// job_state: expects 3 segments (pipeline/schedule/date)
		{
			name:    "job_state valid 3 segments",
			a:       types.Assertion{Type: types.AssertJobState, Target: "pipeline/schedule/2024-01-15"},
			wantErr: false,
		},
		{
			name:    "job_state invalid 1 segment",
			a:       types.Assertion{Type: types.AssertJobState, Target: "job-1"},
			wantErr: true,
		},
		{
			name:    "job_state invalid 2 segments",
			a:       types.Assertion{Type: types.AssertJobState, Target: "pipeline/schedule"},
			wantErr: true,
		},
		// rerun_state: expects 3 segments (pipeline/schedule/date)
		{
			name:    "rerun_state valid 3 segments",
			a:       types.Assertion{Type: types.AssertRerunState, Target: "pipeline/schedule/2024-01-15"},
			wantErr: false,
		},
		{
			name:    "rerun_state invalid 1 segment",
			a:       types.Assertion{Type: types.AssertRerunState, Target: "no-slash"},
			wantErr: true,
		},
		// unsupported types: ValidateTarget should return nil (not our concern)
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

// --- Interlock Event assertion tests ---

func TestAdapterAsserter_InterlockEvent_Exists_Found(t *testing.T) {
	t.Parallel()
	store := newMockStateStore()
	reader := newMockEventReader()
	reader.events = append(reader.events, types.ChaosEvent{
		Scenario: "POST_RUN_DRIFT",
		Mutation: "some-mutation",
	})
	aa := interlock.NewAdapterAsserter(store, reader)

	ok, err := aa.Evaluate(context.Background(), types.Assertion{
		Type: types.AssertInterlockEvent, Target: "POST_RUN_DRIFT", Condition: types.CondExists,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Error("expected interlock_event exists to be true")
	}
}

func TestAdapterAsserter_InterlockEvent_Exists_NotFound(t *testing.T) {
	t.Parallel()
	aa := interlock.NewAdapterAsserter(newMockStateStore(), newMockEventReader())

	ok, err := aa.Evaluate(context.Background(), types.Assertion{
		Type: types.AssertInterlockEvent, Target: "POST_RUN_DRIFT", Condition: types.CondExists,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok {
		t.Error("expected interlock_event exists to be false (no matching event)")
	}
}

func TestAdapterAsserter_InterlockEvent_NotExists_NotFound(t *testing.T) {
	t.Parallel()
	aa := interlock.NewAdapterAsserter(newMockStateStore(), newMockEventReader())

	ok, err := aa.Evaluate(context.Background(), types.Assertion{
		Type: types.AssertInterlockEvent, Target: "POST_RUN_DRIFT", Condition: types.CondNotExists,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// evalInterlockEvent reports whether event was found; engine handles condition inversion
	if ok {
		t.Error("expected false (no matching event)")
	}
}

func TestAdapterAsserter_InterlockEvent_NotExists_Found(t *testing.T) {
	t.Parallel()
	store := newMockStateStore()
	reader := newMockEventReader()
	reader.events = append(reader.events, types.ChaosEvent{
		Scenario: "POST_RUN_DRIFT",
		Mutation: "some-mutation",
	})
	aa := interlock.NewAdapterAsserter(store, reader)

	ok, err := aa.Evaluate(context.Background(), types.Assertion{
		Type: types.AssertInterlockEvent, Target: "POST_RUN_DRIFT", Condition: types.CondNotExists,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// evalInterlockEvent reports whether event was found; engine handles condition inversion
	if !ok {
		t.Error("expected true (event was found)")
	}
}

func TestAdapterAsserter_InterlockEvent_ManifestError(t *testing.T) {
	t.Parallel()
	store := newMockStateStore()
	reader := newMockEventReader()
	reader.err = fmt.Errorf("manifest unavailable")
	aa := interlock.NewAdapterAsserter(store, reader)

	_, err := aa.Evaluate(context.Background(), types.Assertion{
		Type: types.AssertInterlockEvent, Target: "POST_RUN_DRIFT", Condition: types.CondExists,
	})
	if err == nil {
		t.Fatal("expected error when manifest fails")
	}
}

// --- Job State tests ---

func TestAdapterAsserter_JobState_StatusFailed(t *testing.T) {
	t.Parallel()
	store := newMockStateStore()
	store.jobEvents = []adapter.JobEvent{
		{Pipeline: "pipe-a", Schedule: "hourly", Date: "2026-03-27T10", Event: "failed", RunID: "run-1"},
	}
	aa := interlock.NewAdapterAsserter(store, newMockEventReader())

	ok, err := aa.Evaluate(context.Background(), types.Assertion{
		Type: types.AssertJobState, Target: "pipe-a/hourly/2026-03-27T10", Condition: types.CondStatusFailed,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Error("expected job_state failed to be true")
	}
}

func TestAdapterAsserter_JobState_StatusSuccess_Completed(t *testing.T) {
	t.Parallel()
	store := newMockStateStore()
	store.jobEvents = []adapter.JobEvent{
		{Pipeline: "pipe-a", Schedule: "hourly", Date: "2026-03-27T10", Event: "completed", RunID: "run-1"},
	}
	aa := interlock.NewAdapterAsserter(store, newMockEventReader())

	ok, err := aa.Evaluate(context.Background(), types.Assertion{
		Type: types.AssertJobState, Target: "pipe-a/hourly/2026-03-27T10", Condition: types.CondStatusSuccess,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Error("expected job_state success to be true for completed event")
	}
}

func TestAdapterAsserter_JobState_StatusSuccess_Succeeded(t *testing.T) {
	t.Parallel()
	store := newMockStateStore()
	store.jobEvents = []adapter.JobEvent{
		{Pipeline: "pipe-a", Schedule: "hourly", Date: "2026-03-27T10", Event: "succeeded", RunID: "run-1"},
	}
	aa := interlock.NewAdapterAsserter(store, newMockEventReader())

	ok, err := aa.Evaluate(context.Background(), types.Assertion{
		Type: types.AssertJobState, Target: "pipe-a/hourly/2026-03-27T10", Condition: types.CondStatusSuccess,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Error("expected job_state success to be true for succeeded event")
	}
}

func TestAdapterAsserter_JobState_StatusRunning_Started(t *testing.T) {
	t.Parallel()
	store := newMockStateStore()
	store.jobEvents = []adapter.JobEvent{
		{Pipeline: "pipe-a", Schedule: "hourly", Date: "2026-03-27T10", Event: "started", RunID: "run-1"},
	}
	aa := interlock.NewAdapterAsserter(store, newMockEventReader())

	ok, err := aa.Evaluate(context.Background(), types.Assertion{
		Type: types.AssertJobState, Target: "pipe-a/hourly/2026-03-27T10", Condition: types.CondStatusRunning,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Error("expected job_state running to be true for started event")
	}
}

func TestAdapterAsserter_JobState_StatusRunning_Running(t *testing.T) {
	t.Parallel()
	store := newMockStateStore()
	store.jobEvents = []adapter.JobEvent{
		{Pipeline: "pipe-a", Schedule: "hourly", Date: "2026-03-27T10", Event: "running", RunID: "run-1"},
	}
	aa := interlock.NewAdapterAsserter(store, newMockEventReader())

	ok, err := aa.Evaluate(context.Background(), types.Assertion{
		Type: types.AssertJobState, Target: "pipe-a/hourly/2026-03-27T10", Condition: types.CondStatusRunning,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Error("expected job_state running to be true for running event")
	}
}

func TestAdapterAsserter_JobState_StatusKilled(t *testing.T) {
	t.Parallel()
	store := newMockStateStore()
	store.jobEvents = []adapter.JobEvent{
		{Pipeline: "pipe-a", Schedule: "hourly", Date: "2026-03-27T10", Event: "killed", RunID: "run-1"},
	}
	aa := interlock.NewAdapterAsserter(store, newMockEventReader())

	ok, err := aa.Evaluate(context.Background(), types.Assertion{
		Type: types.AssertJobState, Target: "pipe-a/hourly/2026-03-27T10", Condition: types.CondStatusKilled,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Error("expected job_state killed to be true")
	}
}

func TestAdapterAsserter_JobState_IsPending_NoEvents(t *testing.T) {
	t.Parallel()
	store := newMockStateStore()
	// no job events configured
	aa := interlock.NewAdapterAsserter(store, newMockEventReader())

	ok, err := aa.Evaluate(context.Background(), types.Assertion{
		Type: types.AssertJobState, Target: "pipe-a/hourly/2026-03-27T10", Condition: types.CondIsPending,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Error("expected job_state is_pending to be true when no events exist")
	}
}

func TestAdapterAsserter_JobState_IsPending_HasEvents(t *testing.T) {
	t.Parallel()
	store := newMockStateStore()
	store.jobEvents = []adapter.JobEvent{
		{Pipeline: "pipe-a", Schedule: "hourly", Date: "2026-03-27T10", Event: "started", RunID: "run-1"},
	}
	aa := interlock.NewAdapterAsserter(store, newMockEventReader())

	ok, err := aa.Evaluate(context.Background(), types.Assertion{
		Type: types.AssertJobState, Target: "pipe-a/hourly/2026-03-27T10", Condition: types.CondIsPending,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok {
		t.Error("expected job_state is_pending to be false when events exist")
	}
}

func TestAdapterAsserter_JobState_NoEvents(t *testing.T) {
	t.Parallel()
	store := newMockStateStore()
	aa := interlock.NewAdapterAsserter(store, newMockEventReader())

	ok, err := aa.Evaluate(context.Background(), types.Assertion{
		Type: types.AssertJobState, Target: "pipe-a/hourly/2026-03-27T10", Condition: types.CondStatusFailed,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok {
		t.Error("expected false when no job events")
	}
}

func TestAdapterAsserter_JobState_MalformedTarget(t *testing.T) {
	t.Parallel()
	store := newMockStateStore()
	aa := interlock.NewAdapterAsserter(store, newMockEventReader())

	_, err := aa.Evaluate(context.Background(), types.Assertion{
		Type: types.AssertJobState, Target: "only/two", Condition: types.CondStatusFailed,
	})
	if err == nil {
		t.Fatal("expected error for malformed job_state target")
	}
}

func TestAdapterAsserter_JobState_ConditionMismatch(t *testing.T) {
	t.Parallel()
	store := newMockStateStore()
	store.jobEvents = []adapter.JobEvent{
		{Pipeline: "pipe-a", Schedule: "hourly", Date: "2026-03-27T10", Event: "completed", RunID: "run-1"},
	}
	aa := interlock.NewAdapterAsserter(store, newMockEventReader())

	ok, err := aa.Evaluate(context.Background(), types.Assertion{
		Type: types.AssertJobState, Target: "pipe-a/hourly/2026-03-27T10", Condition: types.CondStatusFailed,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok {
		t.Error("expected false: event is completed, not failed")
	}
}

// --- Rerun State tests ---

func TestAdapterAsserter_RerunState_Exists(t *testing.T) {
	t.Parallel()
	store := newMockStateStore()
	store.reruns = 2
	aa := interlock.NewAdapterAsserter(store, newMockEventReader())

	ok, err := aa.Evaluate(context.Background(), types.Assertion{
		Type: types.AssertRerunState, Target: "pipe-a/hourly/2026-03-27T10", Condition: types.CondExists,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Error("expected rerun_state exists to be true")
	}
}

func TestAdapterAsserter_RerunState_Exists_WhenNone(t *testing.T) {
	t.Parallel()
	store := newMockStateStore()
	store.reruns = 0
	aa := interlock.NewAdapterAsserter(store, newMockEventReader())

	ok, err := aa.Evaluate(context.Background(), types.Assertion{
		Type: types.AssertRerunState, Target: "pipe-a/hourly/2026-03-27T10", Condition: types.CondExists,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok {
		t.Error("expected rerun_state exists to be false when count=0")
	}
}

func TestAdapterAsserter_RerunState_NotExists_WhenNone(t *testing.T) {
	t.Parallel()
	store := newMockStateStore()
	store.reruns = 0
	aa := interlock.NewAdapterAsserter(store, newMockEventReader())

	ok, err := aa.Evaluate(context.Background(), types.Assertion{
		Type: types.AssertRerunState, Target: "pipe-a/hourly/2026-03-27T10", Condition: types.CondNotExists,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Error("expected rerun_state not_exists to be true when count=0")
	}
}

func TestAdapterAsserter_RerunState_NotExists_WhenSome(t *testing.T) {
	t.Parallel()
	store := newMockStateStore()
	store.reruns = 3
	aa := interlock.NewAdapterAsserter(store, newMockEventReader())

	ok, err := aa.Evaluate(context.Background(), types.Assertion{
		Type: types.AssertRerunState, Target: "pipe-a/hourly/2026-03-27T10", Condition: types.CondNotExists,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok {
		t.Error("expected rerun_state not_exists to be false when count>0")
	}
}

func TestAdapterAsserter_RerunState_MalformedTarget(t *testing.T) {
	t.Parallel()
	store := newMockStateStore()
	aa := interlock.NewAdapterAsserter(store, newMockEventReader())

	_, err := aa.Evaluate(context.Background(), types.Assertion{
		Type: types.AssertRerunState, Target: "only/two", Condition: types.CondExists,
	})
	if err == nil {
		t.Fatal("expected error for malformed rerun_state target")
	}
}

func TestAdapterAsserter_RerunState_UnsupportedCondition(t *testing.T) {
	t.Parallel()
	store := newMockStateStore()
	aa := interlock.NewAdapterAsserter(store, newMockEventReader())

	_, err := aa.Evaluate(context.Background(), types.Assertion{
		Type: types.AssertRerunState, Target: "pipe-a/hourly/2026-03-27T10", Condition: types.CondStatusFailed,
	})
	if err == nil {
		t.Fatal("expected error for unsupported rerun_state condition")
	}
}
