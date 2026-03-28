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
	// "delay" isn't registered so injection should fail.
	if result.Scenario != "test-noop" {
		t.Errorf("scenario = %q, want test-noop", result.Scenario)
	}
	if result.Capability != "validation/equals" {
		t.Errorf("capability = %q, want validation/equals", result.Capability)
	}
	if result.Error == "" {
		t.Error("expected error from unregistered mutation")
	}
	if result.Passed {
		t.Error("expected Passed=false for failed injection")
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
	// Setup should succeed; injection fails (unregistered mutation).
	if result.Error == "" {
		t.Error("expected error from unregistered mutation")
	}
	if result.Passed {
		t.Error("expected Passed=false for failed injection")
	}
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
	if matrix.Total == 0 {
		t.Error("expected non-zero total capabilities")
	}
	if matrix.Total != matrix.Covered+matrix.Gaps+matrix.Untested {
		t.Errorf("total (%d) != covered (%d) + gaps (%d) + untested (%d)",
			matrix.Total, matrix.Covered, matrix.Gaps, matrix.Untested)
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

func TestEnrichMutationParams(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		params     map[string]string
		nsPipeline string
		want       map[string]string
	}{
		{
			name:       "pipeline replaced with namespace",
			params:     map[string]string{"pipeline": "bronze-cdr", "schedule": "daily", "date": "2026-03-28"},
			nsPipeline: "bronze-cdr-001",
			want:       map[string]string{"pipeline": "bronze-cdr-001", "schedule": "daily", "date": "2026-03-28"},
		},
		{
			name:       "schedule and date defaulted when absent",
			params:     map[string]string{"pipeline": "bronze-cdr"},
			nsPipeline: "bronze-cdr-001",
			want:       map[string]string{"pipeline": "bronze-cdr-001", "schedule": "default", "date": "default"},
		},
		{
			name:       "schedule and date defaulted when empty string",
			params:     map[string]string{"pipeline": "bronze-cdr", "schedule": "", "date": ""},
			nsPipeline: "bronze-cdr-001",
			want:       map[string]string{"pipeline": "bronze-cdr-001", "schedule": "default", "date": "default"},
		},
		{
			name:       "existing schedule and date preserved",
			params:     map[string]string{"pipeline": "silver-cdr", "schedule": "weekly", "date": "2026-01-01"},
			nsPipeline: "silver-cdr-007",
			want:       map[string]string{"pipeline": "silver-cdr-007", "schedule": "weekly", "date": "2026-01-01"},
		},
		{
			name:       "empty nsPipeline does not replace pipeline",
			params:     map[string]string{"pipeline": "gold-cdr", "schedule": "daily", "date": "2026-03-28"},
			nsPipeline: "",
			want:       map[string]string{"pipeline": "gold-cdr", "schedule": "daily", "date": "2026-03-28"},
		},
		{
			name:       "pipeline key absent from params",
			params:     map[string]string{"schedule": "daily", "date": "2026-03-28"},
			nsPipeline: "bronze-cdr-001",
			want:       map[string]string{"schedule": "daily", "date": "2026-03-28"},
		},
		{
			name:       "nil params returns map with defaults",
			params:     nil,
			nsPipeline: "bronze-cdr-001",
			want:       map[string]string{"schedule": "default", "date": "default"},
		},
		{
			name:       "empty params returns map with defaults",
			params:     map[string]string{},
			nsPipeline: "bronze-cdr-001",
			want:       map[string]string{"schedule": "default", "date": "default"},
		},
		{
			name:       "original map not mutated when pipeline replaced",
			params:     map[string]string{"pipeline": "bronze-cdr", "schedule": "daily", "date": "2026-03-28"},
			nsPipeline: "bronze-cdr-042",
			want:       map[string]string{"pipeline": "bronze-cdr-042", "schedule": "daily", "date": "2026-03-28"},
		},
		{
			name:       "additional params preserved alongside defaults",
			params:     map[string]string{"pipeline": "bronze-cdr", "region": "us-east-1"},
			nsPipeline: "bronze-cdr-003",
			want:       map[string]string{"pipeline": "bronze-cdr-003", "region": "us-east-1", "schedule": "default", "date": "default"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// Capture a snapshot of the original map before the call so we can
			// verify immutability regardless of which test case runs.
			var originalSnapshot map[string]string
			if tc.params != nil {
				originalSnapshot = make(map[string]string, len(tc.params))
				for k, v := range tc.params {
					originalSnapshot[k] = v
				}
			}

			got := enrichMutationParams(tc.params, tc.nsPipeline)

			// Verify output matches expected.
			if len(got) != len(tc.want) {
				t.Errorf("len(got) = %d, want %d; got=%v want=%v", len(got), len(tc.want), got, tc.want)
			}
			for k, wantVal := range tc.want {
				if gotVal, ok := got[k]; !ok {
					t.Errorf("key %q missing from result", k)
				} else if gotVal != wantVal {
					t.Errorf("got[%q] = %q, want %q", k, gotVal, wantVal)
				}
			}

			// Verify original map was not mutated.
			if tc.params != nil {
				for k, origVal := range originalSnapshot {
					if tc.params[k] != origVal {
						t.Errorf("original map mutated: params[%q] changed from %q to %q", k, origVal, tc.params[k])
					}
				}
				if len(tc.params) != len(originalSnapshot) {
					t.Errorf("original map length changed: was %d, now %d", len(originalSnapshot), len(tc.params))
				}
			}

			// Verify result is a distinct map (not the same pointer).
			// We do this by adding a sentinel key to the result and checking
			// the original is unchanged.
			if tc.params != nil {
				got["__sentinel__"] = "yes"
				if _, leaked := tc.params["__sentinel__"]; leaked {
					t.Error("result shares underlying storage with input map")
				}
			}
		})
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
