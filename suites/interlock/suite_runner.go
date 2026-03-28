package interlocksuite

import (
	"context"
	"fmt"
	"time"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/engine"
	"github.com/dwsmith1983/chaos-data/pkg/mutation"
	"github.com/dwsmith1983/chaos-data/pkg/scenario"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// ScenarioResult holds the outcome of running one scenario.
type ScenarioResult struct {
	Scenario   string
	Capability string
	Passed     bool
	Duration   time.Duration
	Error      string
}

// SuiteRunner orchestrates chaos scenario execution.
type SuiteRunner struct {
	clock       adapter.Clock
	store       adapter.StateStore
	registry    *mutation.Registry
	evaluator   InterlockEvaluator
	eventReader *LocalEventReader
	coverage    *CoverageTracker
	runCounter  int
}

// SuiteOption configures a SuiteRunner.
type SuiteOption func(*SuiteRunner)

// WithSuiteClock sets the clock used by the suite runner.
func WithSuiteClock(c adapter.Clock) SuiteOption {
	return func(r *SuiteRunner) { r.clock = c }
}

// WithSuiteEvaluator sets the Interlock evaluator used by the suite runner.
func WithSuiteEvaluator(e InterlockEvaluator) SuiteOption {
	return func(r *SuiteRunner) { r.evaluator = e }
}

// WithSuiteEventReader sets the event reader used by the suite runner.
func WithSuiteEventReader(er *LocalEventReader) SuiteOption {
	return func(r *SuiteRunner) { r.eventReader = er }
}

// NewSuiteRunner creates a SuiteRunner with the given dependencies.
func NewSuiteRunner(store adapter.StateStore, reg *mutation.Registry, coverage *CoverageTracker, opts ...SuiteOption) *SuiteRunner {
	r := &SuiteRunner{
		clock:       adapter.NewWallClock(),
		store:       store,
		registry:    reg,
		evaluator:   &AWSInterlockEvaluator{},
		eventReader: NewLocalEventReader(),
		coverage:    coverage,
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// RunScenario executes a single chaos scenario: sets up state, injects chaos,
// triggers Interlock evaluation, evaluates assertions, records coverage, and
// tears down.
func (r *SuiteRunner) RunScenario(ctx context.Context, ss SuiteScenario) ScenarioResult {
	start := r.clock.Now()
	r.runCounter++
	runID := fmt.Sprintf("%03d", r.runCounter)

	result := ScenarioResult{
		Scenario:   ss.Name,
		Capability: ss.Capability,
	}

	// 1. Create harness with unique namespace.
	h := NewHarness(r.store, r.clock, runID)

	// 2. Setup prerequisite state.
	if ss.Setup != nil {
		if err := h.Setup(ctx, *ss.Setup); err != nil {
			result.Error = fmt.Sprintf("setup failed: %v", err)
			r.coverage.Record(ss.Capability, false, r.clock.Now().Sub(start))
			return result
		}
	}

	// 3. Reset event reader for isolation.
	r.eventReader.Reset()

	// 4. Create engine with scenario.
	eng := engine.New(
		types.EngineConfig{
			Mode:               "deterministic",
			AssertWait:         ss.Expected != nil,
			AssertPollInterval: types.Duration{Duration: 100 * time.Millisecond},
		},
		nil, // no data transport needed for state-layer mutations
		r.registry,
		[]scenario.Scenario{ss.Scenario},
		engine.WithClock(r.clock),
	)

	// 5. Build the object key — use namespaced pipeline if setup is present,
	//    otherwise use the scenario name as a fallback key.
	objKey := ss.Name
	if ss.Setup != nil {
		objKey = h.NamespacedPipeline(ss.Setup.Pipeline)
	}

	// 6. Process (inject chaos).
	_, err := eng.ProcessObject(ctx, types.DataObject{Key: objKey})
	if err != nil {
		result.Error = fmt.Sprintf("inject failed: %v", err)
		_ = h.Teardown(ctx)
		r.coverage.Record(ss.Capability, false, r.clock.Now().Sub(start))
		return result
	}

	// 7. Trigger Interlock evaluation.
	if ss.Setup != nil {
		_ = r.evaluator.EvaluateAfterInjection(ctx,
			h.NamespacedPipeline(ss.Setup.Pipeline), "default", "default")
	}

	// 8. Evaluate assertions.
	if ss.Expected != nil {
		assertResults := eng.EvaluateAssertions(ctx, []scenario.Scenario{ss.Scenario})
		allPassed := true
		for _, ar := range assertResults {
			if !ar.Satisfied {
				allPassed = false
				if ar.Error != "" {
					result.Error = ar.Error
				}
			}
		}
		result.Passed = allPassed
	} else {
		// No assertions — scenario passes if injection succeeded.
		result.Passed = true
	}

	// 9. Record coverage.
	duration := r.clock.Now().Sub(start)
	result.Duration = duration
	r.coverage.Record(ss.Capability, result.Passed, duration)

	// 10. Teardown.
	_ = h.Teardown(ctx)

	return result
}

// Report returns the current coverage matrix.
func (r *SuiteRunner) Report() CoverageMatrix {
	return r.coverage.Matrix()
}
