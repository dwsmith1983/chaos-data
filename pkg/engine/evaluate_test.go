package engine_test

import (
	"context"
	"testing"
	"time"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/engine"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

func TestEvaluateAssertionSet_PositiveSatisfied(t *testing.T) {
	t.Parallel()

	asserter := &mockAsserter{
		supported: map[types.AssertionType]bool{types.AssertSensorState: true},
		results:   map[string]bool{"pipeline/key": true},
	}
	clk := adapter.NewTestClock(time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC))

	assertions := []types.Assertion{
		{Type: types.AssertSensorState, Target: "pipeline/key", Condition: types.CondIsStale},
	}

	results := engine.EvaluateAssertionSet(
		context.Background(), assertions, 5*time.Second,
		asserter, clk, nil, 10*time.Millisecond,
	)

	if len(results) != 1 {
		t.Fatalf("results len = %d, want 1", len(results))
	}
	if !results[0].Satisfied {
		t.Error("expected Satisfied=true for positive assertion that matches")
	}
	if results[0].Error != "" {
		t.Errorf("unexpected error: %s", results[0].Error)
	}
}

func TestEvaluateAssertionSet_PositiveTimeout(t *testing.T) {
	t.Parallel()

	asserter := &mockAsserter{
		supported: map[types.AssertionType]bool{types.AssertSensorState: true},
		results:   map[string]bool{"pipeline/key": false}, // never satisfied
	}
	clk := adapter.NewTestClock(time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC))

	assertions := []types.Assertion{
		{Type: types.AssertSensorState, Target: "pipeline/key", Condition: types.CondIsStale},
	}

	results := engine.EvaluateAssertionSet(
		context.Background(), assertions, 100*time.Millisecond,
		asserter, clk, nil, 10*time.Millisecond,
	)

	if len(results) != 1 {
		t.Fatalf("results len = %d, want 1", len(results))
	}
	if results[0].Satisfied {
		t.Error("expected Satisfied=false for positive assertion that times out")
	}
}

func TestEvaluateAssertionSet_NegativePassesOnTimeout(t *testing.T) {
	t.Parallel()

	asserter := &mockAsserter{
		supported: map[types.AssertionType]bool{types.AssertEventEmitted: true},
		results:   map[string]bool{}, // event never found
	}
	clk := adapter.NewTestClock(time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC))

	assertions := []types.Assertion{
		{Type: types.AssertEventEmitted, Target: "sc/mut", Condition: types.CondNotExists},
	}

	results := engine.EvaluateAssertionSet(
		context.Background(), assertions, 100*time.Millisecond,
		asserter, clk, nil, 10*time.Millisecond,
	)

	if len(results) != 1 {
		t.Fatalf("results len = %d, want 1", len(results))
	}
	if !results[0].Satisfied {
		t.Error("expected Satisfied=true for negative assertion that times out (never found)")
	}
}

func TestEvaluateAssertionSet_NegativeFailsImmediately(t *testing.T) {
	t.Parallel()

	asserter := &mockAsserter{
		supported: map[types.AssertionType]bool{types.AssertEventEmitted: true},
		results:   map[string]bool{"sc/mut": true}, // event IS found
	}
	clk := adapter.NewTestClock(time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC))

	assertions := []types.Assertion{
		{Type: types.AssertEventEmitted, Target: "sc/mut", Condition: types.CondNotExists},
	}

	results := engine.EvaluateAssertionSet(
		context.Background(), assertions, 5*time.Second,
		asserter, clk, nil, 10*time.Millisecond,
	)

	if len(results) != 1 {
		t.Fatalf("results len = %d, want 1", len(results))
	}
	if results[0].Satisfied {
		t.Error("expected Satisfied=false for negative assertion when event found")
	}
}

func TestEvaluateAssertionSet_MultipleAssertions(t *testing.T) {
	t.Parallel()

	asserter := &mockAsserter{
		supported: map[types.AssertionType]bool{
			types.AssertSensorState:  true,
			types.AssertTriggerState: true,
		},
		results: map[string]bool{
			"pipeline/key":           true,
			"pipeline/schedule/date": true,
		},
	}
	clk := adapter.NewTestClock(time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC))

	assertions := []types.Assertion{
		{Type: types.AssertSensorState, Target: "pipeline/key", Condition: types.CondIsStale},
		{Type: types.AssertTriggerState, Target: "pipeline/schedule/date", Condition: types.CondStatusSuccess},
	}

	results := engine.EvaluateAssertionSet(
		context.Background(), assertions, 5*time.Second,
		asserter, clk, nil, 10*time.Millisecond,
	)

	if len(results) != 2 {
		t.Fatalf("results len = %d, want 2", len(results))
	}
	for i, r := range results {
		if !r.Satisfied {
			t.Errorf("results[%d] Satisfied=false, want true", i)
		}
	}
}

func TestEvaluateAssertionSet_UnsupportedType(t *testing.T) {
	t.Parallel()

	asserter := &mockAsserter{
		supported: map[types.AssertionType]bool{}, // supports nothing
		results:   map[string]bool{},
	}
	clk := adapter.NewTestClock(time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC))

	assertions := []types.Assertion{
		{Type: types.AssertSensorState, Target: "pipeline/key", Condition: types.CondIsStale},
	}

	results := engine.EvaluateAssertionSet(
		context.Background(), assertions, 100*time.Millisecond,
		asserter, clk, nil, 10*time.Millisecond,
	)

	if len(results) != 1 {
		t.Fatalf("results len = %d, want 1", len(results))
	}
	if results[0].Error == "" {
		t.Error("expected error for unsupported assertion type")
	}
}

func TestEvaluateAssertionSet_EmptyAssertions(t *testing.T) {
	t.Parallel()

	clk := adapter.NewTestClock(time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC))

	results := engine.EvaluateAssertionSet(
		context.Background(), nil, 5*time.Second,
		nil, clk, nil, 10*time.Millisecond,
	)

	if len(results) != 0 {
		t.Errorf("results len = %d, want 0", len(results))
	}
}

func TestEvaluateAssertionSet_TargetValidation(t *testing.T) {
	t.Parallel()

	asserter := &mockValidatingAsserter{
		mockAsserter: mockAsserter{
			supported: map[types.AssertionType]bool{types.AssertSensorState: true},
			results:   map[string]bool{},
		},
		invalidTargets: map[string]bool{"bad-target": true},
	}
	clk := adapter.NewTestClock(time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC))

	assertions := []types.Assertion{
		{Type: types.AssertSensorState, Target: "bad-target", Condition: types.CondIsStale},
	}

	results := engine.EvaluateAssertionSet(
		context.Background(), assertions, 100*time.Millisecond,
		asserter, clk, nil, 10*time.Millisecond,
	)

	if len(results) != 1 {
		t.Fatalf("results len = %d, want 1", len(results))
	}
	if results[0].Error == "" {
		t.Error("expected error for invalid target")
	}
}
