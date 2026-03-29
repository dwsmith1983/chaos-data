package interlocksuite

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/mutation"
	"github.com/dwsmith1983/chaos-data/pkg/scenario"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

func TestLoadSequence(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	yamlContent := `name: test-sequence
description: A test sequence for unit testing
capabilities:
  - post_run/post_run_drift
  - recovery/rerun_accepted
steps:
  - name: setup-pipeline
    setup:
      pipeline: bronze-cdr
      trigger_status: COMPLETED
      sensors:
        hourly-status:
          status: COMPLETE
          sensor_count: 1000
  - name: inject-drift
    scenario: scenarios/post-run/drift-count-changed.yaml
    assert:
      - type: interlock_event
        target: "POST_RUN_DRIFT"
        condition: exists
  - wait: "5s"
  - name: second-inject
    scenario: scenarios/post-run/drift-count-changed.yaml
    continue_on_failure: true
`
	path := filepath.Join(dir, "test-seq.yaml")
	if err := os.WriteFile(path, []byte(yamlContent), 0o644); err != nil {
		t.Fatal(err)
	}

	seq, err := LoadSequence(path)
	if err != nil {
		t.Fatalf("LoadSequence: %v", err)
	}

	if seq.Name != "test-sequence" {
		t.Errorf("Name = %q, want %q", seq.Name, "test-sequence")
	}
	if seq.Description != "A test sequence for unit testing" {
		t.Errorf("Description = %q, want %q", seq.Description, "A test sequence for unit testing")
	}
	if len(seq.Capabilities) != 2 {
		t.Fatalf("Capabilities len = %d, want 2", len(seq.Capabilities))
	}
	if seq.Capabilities[0] != "post_run/post_run_drift" {
		t.Errorf("Capabilities[0] = %q, want %q", seq.Capabilities[0], "post_run/post_run_drift")
	}
	if len(seq.Steps) != 4 {
		t.Fatalf("Steps len = %d, want 4", len(seq.Steps))
	}

	// Step 0: setup only
	if seq.Steps[0].Name != "setup-pipeline" {
		t.Errorf("Steps[0].Name = %q, want %q", seq.Steps[0].Name, "setup-pipeline")
	}
	if seq.Steps[0].Setup == nil {
		t.Fatal("Steps[0].Setup should not be nil")
	}
	if seq.Steps[0].Setup.Pipeline != "bronze-cdr" {
		t.Errorf("Steps[0].Setup.Pipeline = %q, want %q", seq.Steps[0].Setup.Pipeline, "bronze-cdr")
	}

	// Step 1: scenario + assert
	if seq.Steps[1].Scenario != "scenarios/post-run/drift-count-changed.yaml" {
		t.Errorf("Steps[1].Scenario = %q", seq.Steps[1].Scenario)
	}
	if len(seq.Steps[1].Assert) != 1 {
		t.Fatalf("Steps[1].Assert len = %d, want 1", len(seq.Steps[1].Assert))
	}
	if seq.Steps[1].Assert[0].Type != types.AssertInterlockEvent {
		t.Errorf("Steps[1].Assert[0].Type = %q, want %q", seq.Steps[1].Assert[0].Type, types.AssertInterlockEvent)
	}

	// Step 2: wait
	if seq.Steps[2].Wait != "5s" {
		t.Errorf("Steps[2].Wait = %q, want %q", seq.Steps[2].Wait, "5s")
	}

	// Step 3: continue_on_failure
	if !seq.Steps[3].ContinueOnFailure {
		t.Error("Steps[3].ContinueOnFailure should be true")
	}
}

func TestLoadSequence_FileNotFound(t *testing.T) {
	t.Parallel()
	_, err := LoadSequence("/nonexistent/path.yaml")
	if err == nil {
		t.Fatal("expected error for missing file")
	}
}

func TestLoadSequence_InvalidYAML(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.yaml")
	if err := os.WriteFile(path, []byte(":::invalid"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := LoadSequence(path)
	if err == nil {
		t.Fatal("expected error for invalid YAML")
	}
}

func TestSequenceRunner_FailFast(t *testing.T) {
	t.Parallel()

	store := newTestSQLiteStore(t)
	clk := adapter.NewTestClock(time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC))
	reg := mutation.NewRegistry()
	ct, err := NewCoverageTracker("coverage.yaml")
	if err != nil {
		t.Fatal(err)
	}

	runner := NewSuiteRunner(store, reg, ct,
		WithSuiteClock(clk),
		WithSuiteEvaluator(&AWSInterlockEvaluator{}),
	)

	seq := Sequence{
		Name:         "fail-fast-test",
		Description:  "Step 2 fails, step 3 should be skipped",
		Capabilities: []string{"test/cap1"},
		Steps: []SequenceStep{
			{
				// Step 1: setup only (passes)
				Name: "setup-step",
				Setup: &SetupSpec{
					Pipeline:      "bronze-cdr",
					TriggerStatus: "COMPLETED",
				},
			},
			{
				// Step 2: references a scenario that uses an unregistered
				// mutation, so it will fail.
				Name:     "bad-scenario",
				Scenario: "testdata/test-scenario.yaml",
			},
			{
				// Step 3: should be SKIPPED because step 2 failed.
				Name: "should-be-skipped",
				Setup: &SetupSpec{
					Pipeline:      "silver-events",
					TriggerStatus: "READY",
				},
			},
		},
	}

	result := runner.RunSequence(context.Background(), seq)

	if result.Sequence != "fail-fast-test" {
		t.Errorf("Sequence = %q, want %q", result.Sequence, "fail-fast-test")
	}
	if result.Passed {
		t.Error("expected Passed=false for fail-fast sequence")
	}
	if len(result.Steps) != 3 {
		t.Fatalf("Steps len = %d, want 3", len(result.Steps))
	}

	// Step 1: should pass (setup only).
	if !result.Steps[0].Passed {
		t.Errorf("step 0 Passed = false, want true; error: %s", result.Steps[0].Error)
	}
	if result.Steps[0].Skipped {
		t.Error("step 0 should not be skipped")
	}

	// Step 2: should fail (unregistered mutation).
	if result.Steps[1].Passed {
		t.Error("step 1 should have failed")
	}
	if result.Steps[1].Skipped {
		t.Error("step 1 should not be skipped (it ran and failed)")
	}
	if result.Steps[1].Error == "" {
		t.Error("step 1 should have an error message")
	}

	// Step 3: should be SKIPPED.
	if result.Steps[2].Passed {
		t.Error("step 2 should not be passed")
	}
	if !result.Steps[2].Skipped {
		t.Error("step 2 should be skipped")
	}
}

func TestSequenceRunner_ContinueOnFailure(t *testing.T) {
	t.Parallel()

	store := newTestSQLiteStore(t)
	clk := adapter.NewTestClock(time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC))
	reg := mutation.NewRegistry()
	ct, err := NewCoverageTracker("coverage.yaml")
	if err != nil {
		t.Fatal(err)
	}

	runner := NewSuiteRunner(store, reg, ct,
		WithSuiteClock(clk),
		WithSuiteEvaluator(&AWSInterlockEvaluator{}),
	)

	seq := Sequence{
		Name:         "continue-on-failure-test",
		Description:  "Step 2 fails but has continue_on_failure, step 3 should run",
		Capabilities: []string{"test/cap2"},
		Steps: []SequenceStep{
			{
				Name: "setup-step",
				Setup: &SetupSpec{
					Pipeline:      "bronze-cdr",
					TriggerStatus: "COMPLETED",
				},
			},
			{
				Name:              "bad-scenario-continue",
				Scenario:          "testdata/test-scenario.yaml",
				ContinueOnFailure: true,
			},
			{
				// Step 3: should NOT be skipped because step 2 has continue_on_failure.
				Name: "should-still-run",
				Setup: &SetupSpec{
					Pipeline:      "silver-events",
					TriggerStatus: "READY",
				},
			},
		},
	}

	result := runner.RunSequence(context.Background(), seq)

	if result.Sequence != "continue-on-failure-test" {
		t.Errorf("Sequence = %q, want %q", result.Sequence, "continue-on-failure-test")
	}
	if len(result.Steps) != 3 {
		t.Fatalf("Steps len = %d, want 3", len(result.Steps))
	}

	// Step 1: passes.
	if !result.Steps[0].Passed {
		t.Errorf("step 0 Passed = false; error: %s", result.Steps[0].Error)
	}

	// Step 2: fails but has continue_on_failure.
	if result.Steps[1].Passed {
		t.Error("step 1 should have failed")
	}
	if result.Steps[1].Skipped {
		t.Error("step 1 should not be skipped")
	}

	// Step 3: should have run (not skipped).
	if result.Steps[2].Skipped {
		t.Error("step 2 should NOT be skipped with continue_on_failure")
	}
	// It should pass since it's just a setup step.
	if !result.Steps[2].Passed {
		t.Errorf("step 2 should pass (setup only); error: %s", result.Steps[2].Error)
	}
}

func TestSequenceRunner_WaitStep_AdvancesTestClock(t *testing.T) {
	t.Parallel()

	store := newTestSQLiteStore(t)
	startTime := time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)
	clk := adapter.NewTestClock(startTime)
	reg := mutation.NewRegistry()
	ct, err := NewCoverageTracker("coverage.yaml")
	if err != nil {
		t.Fatal(err)
	}

	runner := NewSuiteRunner(store, reg, ct,
		WithSuiteClock(clk),
		WithSuiteEvaluator(&AWSInterlockEvaluator{}),
	)

	seq := Sequence{
		Name:        "wait-test",
		Description: "Test that wait advances the test clock",
		Steps: []SequenceStep{
			{
				Name: "wait-10s",
				Wait: "10s",
			},
		},
	}

	result := runner.RunSequence(context.Background(), seq)

	if !result.Passed {
		t.Errorf("expected Passed=true; steps: %+v", result.Steps)
	}
	if len(result.Steps) != 1 {
		t.Fatalf("Steps len = %d, want 1", len(result.Steps))
	}
	if !result.Steps[0].Passed {
		t.Errorf("wait step should pass; error: %s", result.Steps[0].Error)
	}

	// The TestClock should have been advanced by 10s.
	expected := startTime.Add(10 * time.Second)
	if !clk.Now().Equal(expected) {
		t.Errorf("clock = %v, want %v", clk.Now(), expected)
	}
}

func TestSequenceRunner_WaitStep_InvalidDuration(t *testing.T) {
	t.Parallel()

	store := newTestSQLiteStore(t)
	clk := adapter.NewTestClock(time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC))
	reg := mutation.NewRegistry()
	ct, err := NewCoverageTracker("coverage.yaml")
	if err != nil {
		t.Fatal(err)
	}

	runner := NewSuiteRunner(store, reg, ct,
		WithSuiteClock(clk),
		WithSuiteEvaluator(&AWSInterlockEvaluator{}),
	)

	seq := Sequence{
		Name: "bad-wait",
		Steps: []SequenceStep{
			{
				Name: "bad-duration",
				Wait: "not-a-duration",
			},
		},
	}

	result := runner.RunSequence(context.Background(), seq)
	if result.Passed {
		t.Error("expected Passed=false for invalid wait duration")
	}
	if result.Steps[0].Error == "" {
		t.Error("expected error message for invalid duration")
	}
}

func TestSequenceRunner_EmptySequence(t *testing.T) {
	t.Parallel()

	store := newTestSQLiteStore(t)
	clk := adapter.NewTestClock(time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC))
	reg := mutation.NewRegistry()
	ct, err := NewCoverageTracker("coverage.yaml")
	if err != nil {
		t.Fatal(err)
	}

	runner := NewSuiteRunner(store, reg, ct,
		WithSuiteClock(clk),
		WithSuiteEvaluator(&AWSInterlockEvaluator{}),
	)

	seq := Sequence{
		Name:  "empty-sequence",
		Steps: []SequenceStep{},
	}

	result := runner.RunSequence(context.Background(), seq)
	if !result.Passed {
		t.Error("empty sequence should pass")
	}
	if len(result.Steps) != 0 {
		t.Errorf("Steps len = %d, want 0", len(result.Steps))
	}
}

func TestSequenceRunner_ContextCancellation(t *testing.T) {
	t.Parallel()

	store := newTestSQLiteStore(t)
	clk := adapter.NewTestClock(time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC))
	reg := mutation.NewRegistry()
	ct, err := NewCoverageTracker("coverage.yaml")
	if err != nil {
		t.Fatal(err)
	}

	runner := NewSuiteRunner(store, reg, ct,
		WithSuiteClock(clk),
		WithSuiteEvaluator(&AWSInterlockEvaluator{}),
	)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately.

	seq := Sequence{
		Name: "cancelled-sequence",
		Steps: []SequenceStep{
			{
				Name: "setup-step",
				Setup: &SetupSpec{
					Pipeline:      "bronze-cdr",
					TriggerStatus: "COMPLETED",
				},
			},
			{
				Name: "should-not-run",
				Setup: &SetupSpec{
					Pipeline:      "silver-events",
					TriggerStatus: "READY",
				},
			},
		},
	}

	result := runner.RunSequence(ctx, seq)
	if result.Passed {
		t.Error("cancelled context should not yield Passed=true")
	}
}

func TestSequenceRunner_DurationRecorded(t *testing.T) {
	t.Parallel()

	store := newTestSQLiteStore(t)
	clk := adapter.NewTestClock(time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC))
	reg := mutation.NewRegistry()
	ct, err := NewCoverageTracker("coverage.yaml")
	if err != nil {
		t.Fatal(err)
	}

	runner := NewSuiteRunner(store, reg, ct,
		WithSuiteClock(clk),
		WithSuiteEvaluator(&AWSInterlockEvaluator{}),
	)

	seq := Sequence{
		Name: "duration-test",
		Steps: []SequenceStep{
			{
				Name: "setup-only",
				Setup: &SetupSpec{
					Pipeline: "bronze-cdr",
				},
			},
		},
	}

	result := runner.RunSequence(context.Background(), seq)
	if result.Duration < 0 {
		t.Errorf("Duration = %v, want >= 0", result.Duration)
	}
}

// ---------------------------------------------------------------------------
// Mock asserter for suite-level assertion routing tests
// ---------------------------------------------------------------------------

// mockSuiteAsserter is a minimal adapter.Asserter for testing sequence assertion
// routing through the CompositeAsserter path.
type mockSuiteAsserter struct {
	supported map[types.AssertionType]bool
	results   map[string]bool
}

func (m *mockSuiteAsserter) Supports(at types.AssertionType) bool {
	return m.supported[at]
}

func (m *mockSuiteAsserter) Evaluate(_ context.Context, a types.Assertion) (bool, error) {
	return m.results[a.Target], nil
}

// ---------------------------------------------------------------------------
// Assertion routing tests
// ---------------------------------------------------------------------------

func TestSequenceRunner_AssertionViaSuiteAsserter(t *testing.T) {
	t.Parallel()

	store := newTestSQLiteStore(t)
	clk := adapter.NewTestClock(time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC))
	reg := mutation.NewRegistry()
	ct, err := NewCoverageTracker("coverage.yaml")
	if err != nil {
		t.Fatal(err)
	}

	mock := &mockSuiteAsserter{
		supported: map[types.AssertionType]bool{
			types.AssertSensorState: true,
		},
		results: map[string]bool{
			"my-sensor": true, // always satisfied
		},
	}
	composite := NewCompositeAsserter(mock)

	runner := NewSuiteRunner(store, reg, ct,
		WithSuiteClock(clk),
		WithSuiteEvaluator(&AWSInterlockEvaluator{}),
		WithSuiteAsserter(composite),
	)

	seq := Sequence{
		Name: "asserter-routing-test",
		Steps: []SequenceStep{
			{
				Name: "setup-step",
				Setup: &SetupSpec{
					Pipeline:      "bronze-cdr",
					TriggerStatus: "COMPLETED",
				},
			},
			{
				Name: "assert-via-asserter",
				Assert: []types.Assertion{
					{
						Type:      types.AssertSensorState,
						Target:    "my-sensor",
						Condition: types.CondIsReady,
					},
				},
			},
		},
	}

	result := runner.RunSequence(context.Background(), seq)

	if !result.Passed {
		t.Errorf("expected Passed=true; steps: %+v", result.Steps)
	}
	if len(result.Steps) != 2 {
		t.Fatalf("Steps len = %d, want 2", len(result.Steps))
	}
	if !result.Steps[0].Passed {
		t.Errorf("step 0 should pass; error: %s", result.Steps[0].Error)
	}
	if !result.Steps[1].Passed {
		t.Errorf("step 1 (assert) should pass; error: %s", result.Steps[1].Error)
	}
}

func TestSequenceRunner_AssertionTimeout(t *testing.T) {
	t.Parallel()

	store := newTestSQLiteStore(t)
	clk := adapter.NewTestClock(time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC))
	reg := mutation.NewRegistry()
	ct, err := NewCoverageTracker("coverage.yaml")
	if err != nil {
		t.Fatal(err)
	}

	// Mock that never satisfies — target not in results map → returns false.
	mock := &mockSuiteAsserter{
		supported: map[types.AssertionType]bool{
			types.AssertSensorState: true,
		},
		results: map[string]bool{},
	}
	composite := NewCompositeAsserter(mock)

	runner := NewSuiteRunner(store, reg, ct,
		WithSuiteClock(clk),
		WithSuiteEvaluator(&AWSInterlockEvaluator{}),
		WithSuiteAsserter(composite),
	)

	seq := Sequence{
		Name: "assertion-timeout-test",
		Steps: []SequenceStep{
			{
				Name:         "assert-never-satisfied",
				AssertWithin: "200ms",
				Assert: []types.Assertion{
					{
						Type:      types.AssertSensorState,
						Target:    "missing-sensor",
						Condition: types.CondIsReady,
					},
				},
			},
		},
	}

	result := runner.RunSequence(context.Background(), seq)

	if result.Passed {
		t.Error("expected Passed=false when assertion times out")
	}
	if len(result.Steps) != 1 {
		t.Fatalf("Steps len = %d, want 1", len(result.Steps))
	}
	if result.Steps[0].Passed {
		t.Error("step 0 should fail due to assertion timeout")
	}
	if result.Steps[0].Error == "" {
		t.Error("step 0 should have an error message describing the failed assertion")
	}
}

func TestSequenceRunner_AssertionDefaultTimeout(t *testing.T) {
	t.Parallel()

	store := newTestSQLiteStore(t)
	clk := adapter.NewTestClock(time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC))
	reg := mutation.NewRegistry()
	ct, err := NewCoverageTracker("coverage.yaml")
	if err != nil {
		t.Fatal(err)
	}

	// Mock that satisfies immediately.
	mock := &mockSuiteAsserter{
		supported: map[types.AssertionType]bool{
			types.AssertSensorState: true,
		},
		results: map[string]bool{
			"fast-sensor": true,
		},
	}
	composite := NewCompositeAsserter(mock)

	runner := NewSuiteRunner(store, reg, ct,
		WithSuiteClock(clk),
		WithSuiteEvaluator(&AWSInterlockEvaluator{}),
		WithSuiteAsserter(composite),
	)

	seq := Sequence{
		Name: "default-timeout-test",
		Steps: []SequenceStep{
			{
				Name: "assert-default-timeout",
				// No AssertWithin — should use the default 30s timeout.
				Assert: []types.Assertion{
					{
						Type:      types.AssertSensorState,
						Target:    "fast-sensor",
						Condition: types.CondIsReady,
					},
				},
			},
		},
	}

	result := runner.RunSequence(context.Background(), seq)

	if !result.Passed {
		t.Errorf("expected Passed=true; steps: %+v", result.Steps)
	}
	if len(result.Steps) != 1 {
		t.Fatalf("Steps len = %d, want 1", len(result.Steps))
	}
	if !result.Steps[0].Passed {
		t.Errorf("step 0 should pass with default timeout; error: %s", result.Steps[0].Error)
	}
}

// TestSequenceRunner_NamespaceIsolation verifies that a sequence's Setup step
// and a subsequent Scenario step share the same harness namespace so that state
// written during setup is visible to the scenario. Before the fix,
// RunScenario created its own harness with a different runID, making state
// from earlier sequence steps invisible.
func TestSequenceRunner_NamespaceIsolation(t *testing.T) {
	t.Parallel()

	store := newTestSQLiteStore(t)
	clk := adapter.NewTestClock(time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC))
	reg := mutation.NewRegistry()
	ct, err := NewCoverageTracker("coverage.yaml")
	if err != nil {
		t.Fatal(err)
	}

	runner := NewSuiteRunner(store, reg, ct,
		WithSuiteClock(clk),
		WithSuiteEvaluator(&AWSInterlockEvaluator{}),
	)

	// Step 1: Setup writes sensor state to pipeline "bronze-cdr".
	// Step 2: Scenario loads testdata/test-scenario.yaml which also
	//         references pipeline "bronze-cdr" in its own setup block.
	//         With namespace isolation both steps use the same harness,
	//         so the scenario's setup writes to "suite-001-bronze-cdr"
	//         (same namespace as the sequence).
	//         The scenario's mutation (interlock-phantom-sensor) is not
	//         registered, so injection will fail. That is fine — the test
	//         only cares that the scenario step used the sequence's runID
	//         (001) rather than allocating a new one (002).
	seq := Sequence{
		Name:         "namespace-isolation-test",
		Description:  "Verifies setup and scenario steps share a harness",
		Capabilities: []string{"validation/equals"},
		Steps: []SequenceStep{
			{
				Name: "write-sensor-state",
				Setup: &SetupSpec{
					Pipeline:      "bronze-cdr",
					TriggerStatus: "COMPLETED",
					Sensors: map[string]map[string]interface{}{
						"hourly-status": {
							"status":       "COMPLETE",
							"sensor_count": 1000,
						},
					},
				},
			},
			{
				// Scenario step — injection will fail (unregistered mutation)
				// but the important thing is it did NOT allocate a new runID.
				Name:              "inject-scenario",
				Scenario:          "testdata/test-scenario.yaml",
				ContinueOnFailure: true,
			},
		},
	}

	result := runner.RunSequence(context.Background(), seq)

	if len(result.Steps) != 2 {
		t.Fatalf("Steps len = %d, want 2", len(result.Steps))
	}
	if !result.Steps[0].Passed {
		t.Fatalf("step 0 (setup) should pass; error: %s", result.Steps[0].Error)
	}

	// The critical assertion: RunSequence increments runCounter once (to 1).
	// Before the fix, RunScenario inside the sequence would increment it
	// again (to 2), creating namespace "suite-002-". After the fix,
	// RunScenarioInHarness does NOT increment the counter, so it stays at 1.
	runner.mu.Lock()
	counter := runner.runCounter
	runner.mu.Unlock()

	if counter != 1 {
		t.Errorf("runCounter = %d, want 1 (RunScenarioInHarness must not allocate a new runID)", counter)
	}
}

// TestRunScenarioInHarness_SharedState verifies that RunScenarioInHarness uses
// the provided harness so that state written prior to the call is visible to
// the scenario's setup and mutation steps.
func TestRunScenarioInHarness_SharedState(t *testing.T) {
	t.Parallel()

	store := newTestSQLiteStore(t)
	clk := adapter.NewTestClock(time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC))
	reg := mutation.NewRegistry()
	ct, err := NewCoverageTracker("coverage.yaml")
	if err != nil {
		t.Fatal(err)
	}

	runner := NewSuiteRunner(store, reg, ct,
		WithSuiteClock(clk),
		WithSuiteEvaluator(&AWSInterlockEvaluator{}),
	)

	// Create a harness with a known runID.
	h := NewHarness(store, clk, "shared-ns")

	// Write sensor state through the harness.
	ctx := context.Background()
	err = h.Setup(ctx, SetupSpec{
		Pipeline:      "my-pipe",
		TriggerStatus: "COMPLETED",
		Sensors: map[string]map[string]interface{}{
			"sensor-a": {"status": "COMPLETE", "sensor_count": 42},
		},
	})
	if err != nil {
		t.Fatalf("harness setup: %v", err)
	}

	// Verify state is in the expected namespace.
	sd, err := store.ReadSensor(ctx, "suite-shared-ns-my-pipe", "sensor-a")
	if err != nil {
		t.Fatalf("ReadSensor: %v", err)
	}
	if string(sd.Status) != "COMPLETE" {
		t.Fatalf("pre-condition: sensor status = %q, want COMPLETE", sd.Status)
	}

	// Run a scenario through the same harness. The scenario has its own
	// setup with the same pipeline, so it writes to the same namespace.
	ss := SuiteScenario{
		Scenario: scenario.Scenario{
			Name:        "shared-harness-test",
			Category:    "state-consistency",
			Severity:    types.SeverityLow,
			Version:     1,
			Target:      scenario.TargetSpec{Layer: "state"},
			Mutation:    scenario.MutationSpec{Type: "delay"}, // unregistered — will fail
			Probability: 1.0,
			Safety:      scenario.ScenarioSafety{MaxAffectedPct: 100},
		},
		Setup: &SetupSpec{
			Pipeline:      "my-pipe",
			TriggerStatus: "READY",
		},
		Capability: "validation/equals",
	}

	result := runner.RunScenarioInHarness(ctx, ss, h)

	// Injection fails (unregistered mutation) but the scenario's setup
	// should have written to the SAME namespace as our prior harness setup.
	// Verify the trigger status was updated in the shared namespace.
	triggerKey := adapter.TriggerKey{
		Pipeline: "suite-shared-ns-my-pipe",
		Schedule: "default",
		Date:     "default",
	}
	status, err := store.ReadTriggerStatus(ctx, triggerKey)
	if err != nil {
		t.Fatalf("ReadTriggerStatus: %v", err)
	}
	// The scenario's setup overwrites the trigger to "READY" in the same
	// namespace — proving RunScenarioInHarness reused the harness.
	if status != "READY" {
		t.Errorf("trigger status = %q, want READY (proves shared namespace)", status)
	}

	// Original sensor should still be in the same namespace.
	sd2, err := store.ReadSensor(ctx, "suite-shared-ns-my-pipe", "sensor-a")
	if err != nil {
		t.Fatalf("ReadSensor after scenario: %v", err)
	}
	if string(sd2.Status) != "COMPLETE" {
		t.Errorf("sensor-a status = %q, want COMPLETE", sd2.Status)
	}

	// Scenario result should reference the correct scenario name.
	if result.Scenario != "shared-harness-test" {
		t.Errorf("Scenario = %q, want shared-harness-test", result.Scenario)
	}

	// Clean up.
	_ = h.Teardown(ctx)
}

func TestSequenceRunner_AssertionFallbackWithoutAsserter(t *testing.T) {
	t.Parallel()

	store := newTestSQLiteStore(t)
	clk := adapter.NewTestClock(time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC))
	reg := mutation.NewRegistry()
	ct, err := NewCoverageTracker("coverage.yaml")
	if err != nil {
		t.Fatal(err)
	}

	// No WithSuiteAsserter — r.asserter will be nil,
	// so assertions should use the legacy exists/not_exists path.
	runner := NewSuiteRunner(store, reg, ct,
		WithSuiteClock(clk),
		WithSuiteEvaluator(&AWSInterlockEvaluator{}),
	)

	seq := Sequence{
		Name: "fallback-no-asserter-test",
		Steps: []SequenceStep{
			{
				Name: "assert-not-exists-legacy",
				Assert: []types.Assertion{
					{
						Type:      types.AssertInterlockEvent,
						Target:    "SOME_EVENT",
						Condition: types.CondNotExists,
					},
				},
			},
		},
	}

	result := runner.RunSequence(context.Background(), seq)

	if !result.Passed {
		t.Errorf("expected Passed=true (no events exist so not_exists should pass); error: %+v", result.Steps)
	}
	if len(result.Steps) != 1 {
		t.Fatalf("Steps len = %d, want 1", len(result.Steps))
	}
	if !result.Steps[0].Passed {
		t.Errorf("step 0 should pass via legacy fallback; error: %s", result.Steps[0].Error)
	}
}
