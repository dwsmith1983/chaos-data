package interlocksuite

import (
	"context"
	"testing"
	"time"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// ---------------------------------------------------------------------------
// SuiteAsserter tests
// ---------------------------------------------------------------------------

func TestSuiteAsserter_Supports(t *testing.T) {
	t.Parallel()
	a := NewSuiteAsserter(NewLocalEventReader())

	tests := []struct {
		name     string
		at       types.AssertionType
		expected bool
	}{
		{"interlock_event", types.AssertInterlockEvent, true},
		{"sensor_state", types.AssertSensorState, false},
		{"trigger_state", types.AssertTriggerState, false},
		{"data_state", types.AssertDataState, false},
		{"rerun_state", types.AssertRerunState, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := a.Supports(tt.at); got != tt.expected {
				t.Errorf("Supports(%q) = %v, want %v", tt.at, got, tt.expected)
			}
		})
	}
}

func TestSuiteAsserter_ValidateTarget(t *testing.T) {
	t.Parallel()
	a := NewSuiteAsserter(NewLocalEventReader())

	if err := a.ValidateTarget(types.Assertion{
		Type:   types.AssertInterlockEvent,
		Target: "JOB_TRIGGERED",
	}); err != nil {
		t.Fatalf("expected nil error for valid target, got: %v", err)
	}

	if err := a.ValidateTarget(types.Assertion{
		Type:   types.AssertInterlockEvent,
		Target: "",
	}); err == nil {
		t.Fatal("expected error for empty target")
	}
}

func TestSuiteAsserter_Evaluate_Exists(t *testing.T) {
	t.Parallel()
	reader := NewLocalEventReader()
	a := NewSuiteAsserter(reader)
	a.SetPipeline("suite-001-bronze-cdr")

	ctx := context.Background()

	// No events yet — exists should return false.
	ok, err := a.Evaluate(ctx, types.Assertion{
		Type:      types.AssertInterlockEvent,
		Target:    "JOB_TRIGGERED",
		Condition: types.CondExists,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok {
		t.Fatal("expected false when no events emitted")
	}

	// Emit a matching event.
	reader.Emit(InterlockEventRecord{
		PipelineID: "suite-001-bronze-cdr",
		EventType:  "JOB_TRIGGERED",
		Timestamp:  time.Now(),
	})

	ok, err = a.Evaluate(ctx, types.Assertion{
		Type:      types.AssertInterlockEvent,
		Target:    "JOB_TRIGGERED",
		Condition: types.CondExists,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Fatal("expected true after matching event emitted")
	}
}

func TestSuiteAsserter_Evaluate_NotExists(t *testing.T) {
	t.Parallel()
	reader := NewLocalEventReader()
	a := NewSuiteAsserter(reader)
	a.SetPipeline("suite-001-bronze-cdr")

	ctx := context.Background()

	// No events — not_exists should return true.
	ok, err := a.Evaluate(ctx, types.Assertion{
		Type:      types.AssertInterlockEvent,
		Target:    "JOB_TRIGGERED",
		Condition: types.CondNotExists,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Fatal("expected true when no events emitted (not_exists)")
	}

	// Emit event — not_exists should return false.
	reader.Emit(InterlockEventRecord{
		PipelineID: "suite-001-bronze-cdr",
		EventType:  "JOB_TRIGGERED",
		Timestamp:  time.Now(),
	})

	ok, err = a.Evaluate(ctx, types.Assertion{
		Type:      types.AssertInterlockEvent,
		Target:    "JOB_TRIGGERED",
		Condition: types.CondNotExists,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok {
		t.Fatal("expected false after matching event emitted (not_exists)")
	}
}

func TestSuiteAsserter_PipelineIsolation(t *testing.T) {
	t.Parallel()
	reader := NewLocalEventReader()
	a := NewSuiteAsserter(reader)

	// Emit event for pipeline A.
	reader.Emit(InterlockEventRecord{
		PipelineID: "suite-001-pipeline-a",
		EventType:  "JOB_TRIGGERED",
		Timestamp:  time.Now(),
	})

	// Set asserter to pipeline B — should NOT see pipeline A's events.
	a.SetPipeline("suite-002-pipeline-b")

	ok, err := a.Evaluate(context.Background(), types.Assertion{
		Type:      types.AssertInterlockEvent,
		Target:    "JOB_TRIGGERED",
		Condition: types.CondExists,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok {
		t.Fatal("expected false — pipeline isolation should prevent cross-pollination")
	}
}

// ---------------------------------------------------------------------------
// RerunStateAsserter tests
// ---------------------------------------------------------------------------

func TestRerunStateAsserter_Supports(t *testing.T) {
	t.Parallel()
	store := newTestSQLiteStore(t)
	a := NewRerunStateAsserter(store)

	tests := []struct {
		name     string
		at       types.AssertionType
		expected bool
	}{
		{"rerun_state", types.AssertRerunState, true},
		{"interlock_event", types.AssertInterlockEvent, false},
		{"trigger_state", types.AssertTriggerState, false},
		{"sensor_state", types.AssertSensorState, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := a.Supports(tt.at); got != tt.expected {
				t.Errorf("Supports(%q) = %v, want %v", tt.at, got, tt.expected)
			}
		})
	}
}

func TestRerunStateAsserter_ValidateTarget(t *testing.T) {
	t.Parallel()
	store := newTestSQLiteStore(t)
	a := NewRerunStateAsserter(store)

	if err := a.ValidateTarget(types.Assertion{
		Type:   types.AssertRerunState,
		Target: "bronze-cdr",
	}); err != nil {
		t.Fatalf("expected nil error for valid target, got: %v", err)
	}

	if err := a.ValidateTarget(types.Assertion{
		Type:   types.AssertRerunState,
		Target: "",
	}); err == nil {
		t.Fatal("expected error for empty target")
	}
}

func TestRerunStateAsserter_Evaluate_Exists(t *testing.T) {
	t.Parallel()
	store := newTestSQLiteStore(t)
	a := NewRerunStateAsserter(store)
	a.SetPipeline("suite-001-bronze-cdr")

	ctx := context.Background()

	// No reruns yet — exists should return false.
	ok, err := a.Evaluate(ctx, types.Assertion{
		Type:      types.AssertRerunState,
		Target:    "bronze-cdr",
		Condition: types.CondExists,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok {
		t.Fatal("expected false when no reruns recorded")
	}

	// Write a rerun.
	if err := store.WriteRerun(ctx, "suite-001-bronze-cdr", "default", "default", "flaky"); err != nil {
		t.Fatalf("WriteRerun: %v", err)
	}

	ok, err = a.Evaluate(ctx, types.Assertion{
		Type:      types.AssertRerunState,
		Target:    "bronze-cdr",
		Condition: types.CondExists,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Fatal("expected true after rerun recorded")
	}
}

func TestRerunStateAsserter_Evaluate_NotExists(t *testing.T) {
	t.Parallel()
	store := newTestSQLiteStore(t)
	a := NewRerunStateAsserter(store)
	a.SetPipeline("suite-001-bronze-cdr")

	ctx := context.Background()

	// No reruns — not_exists should return true.
	ok, err := a.Evaluate(ctx, types.Assertion{
		Type:      types.AssertRerunState,
		Target:    "bronze-cdr",
		Condition: types.CondNotExists,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Fatal("expected true when no reruns recorded (not_exists)")
	}

	// Write a rerun — not_exists should return false.
	if err := store.WriteRerun(ctx, "suite-001-bronze-cdr", "default", "default", "timeout"); err != nil {
		t.Fatalf("WriteRerun: %v", err)
	}

	ok, err = a.Evaluate(ctx, types.Assertion{
		Type:      types.AssertRerunState,
		Target:    "bronze-cdr",
		Condition: types.CondNotExists,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok {
		t.Fatal("expected false after rerun recorded (not_exists)")
	}
}

func TestRerunStateAsserter_PipelineIsolation(t *testing.T) {
	t.Parallel()
	store := newTestSQLiteStore(t)
	a := NewRerunStateAsserter(store)

	ctx := context.Background()

	// Write rerun for pipeline A.
	if err := store.WriteRerun(ctx, "suite-001-pipeline-a", "default", "default", "flaky"); err != nil {
		t.Fatalf("WriteRerun: %v", err)
	}

	// Set asserter to pipeline B — should NOT see pipeline A's reruns.
	a.SetPipeline("suite-002-pipeline-b")

	ok, err := a.Evaluate(ctx, types.Assertion{
		Type:      types.AssertRerunState,
		Target:    "pipeline-b",
		Condition: types.CondExists,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok {
		t.Fatal("expected false — pipeline isolation should prevent cross-pollination")
	}
}

// ---------------------------------------------------------------------------
// TriggerStateAsserter tests
// ---------------------------------------------------------------------------

func TestTriggerStateAsserter_Supports(t *testing.T) {
	t.Parallel()
	store := newTestSQLiteStore(t)
	a := NewTriggerStateAsserter(store)

	if !a.Supports(types.AssertTriggerState) {
		t.Error("expected Supports(trigger_state) = true")
	}
	if a.Supports(types.AssertInterlockEvent) {
		t.Error("expected Supports(interlock_event) = false")
	}
}

func TestTriggerStateAsserter_WasTriggered(t *testing.T) {
	t.Parallel()
	store := newTestSQLiteStore(t)
	a := NewTriggerStateAsserter(store)
	a.SetPipeline("suite-001-bronze-cdr")

	ctx := context.Background()

	// Write trigger status "triggered".
	key := adapter.TriggerKey{Pipeline: "suite-001-bronze-cdr", Schedule: "default", Date: "default"}
	if err := store.WriteTriggerStatus(ctx, key, "triggered"); err != nil {
		t.Fatalf("WriteTriggerStatus: %v", err)
	}

	ok, err := a.Evaluate(ctx, types.Assertion{
		Type:      types.AssertTriggerState,
		Target:    "bronze-cdr",
		Condition: types.CondWasTriggered,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Fatal("expected true for was_triggered when status is 'triggered'")
	}
}

func TestTriggerStateAsserter_StatusRunning(t *testing.T) {
	t.Parallel()
	store := newTestSQLiteStore(t)
	a := NewTriggerStateAsserter(store)
	a.SetPipeline("suite-001-bronze-cdr")

	ctx := context.Background()

	key := adapter.TriggerKey{Pipeline: "suite-001-bronze-cdr", Schedule: "default", Date: "default"}
	if err := store.WriteTriggerStatus(ctx, key, "running"); err != nil {
		t.Fatalf("WriteTriggerStatus: %v", err)
	}

	ok, err := a.Evaluate(ctx, types.Assertion{
		Type:      types.AssertTriggerState,
		Target:    "bronze-cdr",
		Condition: types.CondStatusRunning,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Fatal("expected true for status:running when trigger is 'running'")
	}
}

// ---------------------------------------------------------------------------
// CompositeAsserter tests
// ---------------------------------------------------------------------------

func TestCompositeAsserter_DelegatesToCorrectChild(t *testing.T) {
	t.Parallel()
	reader := NewLocalEventReader()
	store := newTestSQLiteStore(t)

	sa := NewSuiteAsserter(reader)
	sa.SetPipeline("suite-001-bronze-cdr")
	ta := NewTriggerStateAsserter(store)
	ta.SetPipeline("suite-001-bronze-cdr")

	composite := NewCompositeAsserter(sa, ta)

	// Verify routing.
	if !composite.Supports(types.AssertInterlockEvent) {
		t.Error("composite should support interlock_event via SuiteAsserter")
	}
	if !composite.Supports(types.AssertTriggerState) {
		t.Error("composite should support trigger_state via TriggerStateAsserter")
	}
	if composite.Supports(types.AssertDataState) {
		t.Error("composite should not support data_state")
	}

	// Verify delegation: emit interlock event, check via composite.
	reader.Emit(InterlockEventRecord{
		PipelineID: "suite-001-bronze-cdr",
		EventType:  "VALIDATION_EXHAUSTED",
		Timestamp:  time.Now(),
	})

	ok, err := composite.Evaluate(context.Background(), types.Assertion{
		Type:      types.AssertInterlockEvent,
		Target:    "VALIDATION_EXHAUSTED",
		Condition: types.CondExists,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Fatal("expected true from composite for interlock_event")
	}

	// Verify delegation: write trigger, check via composite.
	ctx := context.Background()
	key := adapter.TriggerKey{Pipeline: "suite-001-bronze-cdr", Schedule: "default", Date: "default"}
	if err := store.WriteTriggerStatus(ctx, key, "triggered"); err != nil {
		t.Fatalf("WriteTriggerStatus: %v", err)
	}

	ok, err = composite.Evaluate(ctx, types.Assertion{
		Type:      types.AssertTriggerState,
		Target:    "bronze-cdr",
		Condition: types.CondWasTriggered,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Fatal("expected true from composite for trigger_state")
	}
}

func TestCompositeAsserter_UnsupportedType(t *testing.T) {
	t.Parallel()
	composite := NewCompositeAsserter()

	_, err := composite.Evaluate(context.Background(), types.Assertion{
		Type:      types.AssertDataState,
		Target:    "foo",
		Condition: types.CondExists,
	})
	if err == nil {
		t.Fatal("expected error for unsupported type")
	}
}
