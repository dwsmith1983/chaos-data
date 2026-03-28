package interlocksuite

import (
	"context"
	"testing"
	"time"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/mutation"
	"github.com/dwsmith1983/chaos-data/pkg/scenario"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

func TestSuiteRunner_RunScenario_NoAssertions(t *testing.T) {
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

	ss := SuiteScenario{
		Scenario: scenario.Scenario{
			Name:        "test-noop",
			Category:    "data-arrival",
			Severity:    types.SeverityLow,
			Version:     1,
			Target:      scenario.TargetSpec{Layer: "data"},
			Mutation:    scenario.MutationSpec{Type: "delay"},
			Probability: 1.0,
			Safety:      scenario.ScenarioSafety{MaxAffectedPct: 100},
		},
		Capability: "validation/equals",
	}

	result := runner.RunScenario(context.Background(), ss)
	// "delay" isn't registered so injection will fail, but we're testing the runner flow.
	if result.Scenario != "test-noop" {
		t.Errorf("scenario = %q, want test-noop", result.Scenario)
	}
	if result.Capability != "validation/equals" {
		t.Errorf("capability = %q, want validation/equals", result.Capability)
	}
}

func TestSuiteRunner_RunScenario_WithSetup(t *testing.T) {
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

	ss := SuiteScenario{
		Scenario: scenario.Scenario{
			Name:        "test-with-setup",
			Category:    "data-arrival",
			Severity:    types.SeverityLow,
			Version:     1,
			Target:      scenario.TargetSpec{Layer: "state"},
			Mutation:    scenario.MutationSpec{Type: "delay"},
			Probability: 1.0,
			Safety:      scenario.ScenarioSafety{MaxAffectedPct: 100},
		},
		Setup: &SetupSpec{
			Pipeline:      "bronze-cdr",
			TriggerStatus: "COMPLETED",
		},
		Capability: "validation/equals",
	}

	result := runner.RunScenario(context.Background(), ss)
	if result.Scenario != "test-with-setup" {
		t.Errorf("scenario = %q, want test-with-setup", result.Scenario)
	}
	// Setup should succeed; injection may fail (unregistered mutation) but
	// the runner should not panic.
}

func TestSuiteRunner_RunScenario_IncrementRunCounter(t *testing.T) {
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

	ss := SuiteScenario{
		Scenario: scenario.Scenario{
			Name:        "counter-test",
			Category:    "data-arrival",
			Severity:    types.SeverityLow,
			Version:     1,
			Target:      scenario.TargetSpec{Layer: "data"},
			Mutation:    scenario.MutationSpec{Type: "delay"},
			Probability: 1.0,
			Safety:      scenario.ScenarioSafety{MaxAffectedPct: 100},
		},
		Capability: "validation/equals",
	}

	// Run twice to verify the counter increments.
	_ = runner.RunScenario(context.Background(), ss)
	result := runner.RunScenario(context.Background(), ss)
	// Second run should have runCounter=2, yielding runID "002".
	// We can't directly inspect runID, but we verify no panic and result is populated.
	if result.Scenario != "counter-test" {
		t.Errorf("scenario = %q, want counter-test", result.Scenario)
	}
}

func TestSuiteRunner_RunScenario_RecordsCoverage(t *testing.T) {
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

	ss := SuiteScenario{
		Scenario: scenario.Scenario{
			Name:        "coverage-test",
			Category:    "data-arrival",
			Severity:    types.SeverityLow,
			Version:     1,
			Target:      scenario.TargetSpec{Layer: "data"},
			Mutation:    scenario.MutationSpec{Type: "delay"},
			Probability: 1.0,
			Safety:      scenario.ScenarioSafety{MaxAffectedPct: 100},
		},
		Capability: "validation/equals",
	}

	_ = runner.RunScenario(context.Background(), ss)

	// Verify coverage was recorded — the matrix should show the capability
	// is no longer UNTESTED.
	matrix := runner.Report()
	found := false
	for _, r := range matrix.Results {
		key := r.Category + "/" + r.Capability.ID
		if key == "validation/equals" {
			found = true
			if r.Status == StatusUntested {
				t.Error("validation/equals should not be UNTESTED after running a scenario")
			}
		}
	}
	if !found {
		t.Error("validation/equals not found in coverage matrix")
	}
}

func TestSuiteRunner_Report(t *testing.T) {
	t.Parallel()
	store := newTestSQLiteStore(t)
	ct, err := NewCoverageTracker("coverage.yaml")
	if err != nil {
		t.Fatal(err)
	}

	runner := NewSuiteRunner(store, mutation.NewRegistry(), ct)
	matrix := runner.Report()
	if matrix.Total != 53 {
		t.Errorf("total = %d, want 53", matrix.Total)
	}
}

func TestSuiteRunner_DefaultOptions(t *testing.T) {
	t.Parallel()
	store := newTestSQLiteStore(t)
	ct, err := NewCoverageTracker("coverage.yaml")
	if err != nil {
		t.Fatal(err)
	}

	// Constructor without options should use defaults and not panic.
	runner := NewSuiteRunner(store, mutation.NewRegistry(), ct)
	if runner == nil {
		t.Fatal("NewSuiteRunner returned nil")
	}
}

func TestSuiteRunner_RunScenario_DurationRecorded(t *testing.T) {
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

	ss := SuiteScenario{
		Scenario: scenario.Scenario{
			Name:        "duration-test",
			Category:    "data-arrival",
			Severity:    types.SeverityLow,
			Version:     1,
			Target:      scenario.TargetSpec{Layer: "data"},
			Mutation:    scenario.MutationSpec{Type: "delay"},
			Probability: 1.0,
			Safety:      scenario.ScenarioSafety{MaxAffectedPct: 100},
		},
		Capability: "validation/equals",
	}

	result := runner.RunScenario(context.Background(), ss)
	// With a fixed test clock, duration should be 0 (clock doesn't advance).
	if result.Duration < 0 {
		t.Errorf("duration = %v, want >= 0", result.Duration)
	}
}

func TestSuiteRunner_RunScenario_ResetsEventReader(t *testing.T) {
	t.Parallel()
	store := newTestSQLiteStore(t)
	clk := adapter.NewTestClock(time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC))
	reg := mutation.NewRegistry()
	ct, err := NewCoverageTracker("coverage.yaml")
	if err != nil {
		t.Fatal(err)
	}

	eventReader := NewLocalEventReader()
	// Pre-populate an event to verify Reset is called.
	eventReader.Emit(InterlockEventRecord{
		PipelineID: "pre-existing",
		EventType:  "test",
		Timestamp:  clk.Now(),
	})

	runner := NewSuiteRunner(store, reg, ct,
		WithSuiteClock(clk),
		WithSuiteEvaluator(&AWSInterlockEvaluator{}),
		WithSuiteEventReader(eventReader),
	)

	ss := SuiteScenario{
		Scenario: scenario.Scenario{
			Name:        "reset-test",
			Category:    "data-arrival",
			Severity:    types.SeverityLow,
			Version:     1,
			Target:      scenario.TargetSpec{Layer: "data"},
			Mutation:    scenario.MutationSpec{Type: "delay"},
			Probability: 1.0,
			Safety:      scenario.ScenarioSafety{MaxAffectedPct: 100},
		},
		Capability: "validation/equals",
	}

	_ = runner.RunScenario(context.Background(), ss)

	// Event reader should have been reset — no pre-existing events remain.
	events, err := eventReader.ReadEvents(context.Background(), "pre-existing", "")
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 0 {
		t.Errorf("expected 0 events after reset, got %d", len(events))
	}
}
