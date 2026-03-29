package interlocksuite

import (
	"context"
	"fmt"
	"sync"
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
	mu          sync.Mutex
	clock       adapter.Clock
	store       adapter.StateStore
	registry    *mutation.Registry
	evaluator   InterlockEvaluator
	eventReader InterlockEventReader
	asserter    *CompositeAsserter
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
func WithSuiteEventReader(er InterlockEventReader) SuiteOption {
	return func(r *SuiteRunner) { r.eventReader = er }
}

// WithSuiteAsserter sets the composite asserter used by the suite runner.
func WithSuiteAsserter(a *CompositeAsserter) SuiteOption {
	return func(r *SuiteRunner) { r.asserter = a }
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
// tears down. It creates its own isolated harness and namespace.
func (r *SuiteRunner) RunScenario(ctx context.Context, ss SuiteScenario) ScenarioResult {
	r.mu.Lock()
	r.runCounter++
	runID := fmt.Sprintf("%03d", r.runCounter)
	r.mu.Unlock()

	h := NewHarness(r.store, r.clock, runID)
	result := r.runScenarioWithHarness(ctx, ss, h, true)
	return result
}

// RunScenarioInHarness executes a chaos scenario using the provided harness
// instead of creating a new one. This allows sequences to share state across
// setup and scenario steps by reusing the same namespace. The caller is
// responsible for teardown of the harness.
func (r *SuiteRunner) RunScenarioInHarness(ctx context.Context, ss SuiteScenario, h *Harness) ScenarioResult {
	return r.runScenarioWithHarness(ctx, ss, h, false)
}

// runScenarioWithHarness is the shared implementation for RunScenario and
// RunScenarioInHarness. When ownsHarness is true the method performs teardown;
// when false the caller is responsible for cleanup.
func (r *SuiteRunner) runScenarioWithHarness(ctx context.Context, ss SuiteScenario, h *Harness, ownsHarness bool) ScenarioResult {
	start := r.clock.Now()

	result := ScenarioResult{
		Scenario:   ss.Name,
		Capability: ss.Capability,
	}

	// 1. Resolve date placeholders in pipeline config before setup.
	if ss.Setup != nil && ss.Setup.PipelineConfig != nil {
		resolveDatePlaceholders(ss.Setup.PipelineConfig, r.clock.Now())
	}

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

	// 4. Build the object key — use namespaced pipeline if setup is present,
	//    otherwise use the scenario name as a fallback key.
	objKey := ss.Name
	if ss.Setup != nil {
		objKey = h.NamespacedPipeline(ss.Setup.Pipeline)
	}

	// 5. Enrich mutation params: namespace pipeline, default schedule/date.
	enrichedScenario := ss.Scenario
	if ss.Setup != nil {
		enrichedScenario.Mutation.Params = enrichMutationParams(
			ss.Scenario.Mutation.Params, objKey)
	}

	// 6. Set asserter pipeline for namespace isolation, then create engine.
	var engineOpts []engine.EngineOption
	engineOpts = append(engineOpts, engine.WithClock(r.clock))
	if r.asserter != nil {
		// Update child asserters' pipeline to the current namespaced pipeline.
		for _, a := range r.asserter.asserters {
			if ps, ok := a.(PipelineSettable); ok {
				ps.SetPipeline(objKey)
			}
		}
		engineOpts = append(engineOpts, engine.WithAsserter(r.asserter))
	}

	eng := engine.New(
		types.EngineConfig{
			Mode:               "deterministic",
			AssertWait:         ss.Expected != nil,
			AssertPollInterval: types.Duration{Duration: 100 * time.Millisecond},
		},
		nil, // no data transport needed for state-layer mutations
		r.registry,
		[]scenario.Scenario{enrichedScenario},
		engineOpts...,
	)

	// 7. Process (inject chaos).
	_, err := eng.ProcessObject(ctx, types.DataObject{Key: objKey})
	if err != nil {
		result.Error = fmt.Sprintf("inject failed: %v", err)
		if ownsHarness {
			_ = h.Teardown(ctx)
		}
		r.coverage.Record(ss.Capability, false, r.clock.Now().Sub(start))
		return result
	}

	// 8. Trigger Interlock evaluation.
	if ss.Setup != nil {
		var sensorKeys []string
		for k := range ss.Setup.Sensors {
			sensorKeys = append(sensorKeys, k)
		}
		if err := r.evaluator.EvaluateAfterInjection(ctx,
			h.NamespacedPipeline(ss.Setup.Pipeline), "default", "default", sensorKeys); err != nil {
			result.Error = fmt.Sprintf("evaluation failed: %v", err)
			result.Passed = false
			if ownsHarness {
				_ = h.Teardown(ctx)
			}
			r.coverage.Record(ss.Capability, false, r.clock.Now().Sub(start))
			return result
		}
	}

	// 9. Evaluate assertions.
	if ss.Expected != nil {
		assertResults := eng.EvaluateAssertions(ctx, []scenario.Scenario{enrichedScenario})
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

	// 10. Record coverage.
	duration := r.clock.Now().Sub(start)
	result.Duration = duration
	r.coverage.Record(ss.Capability, result.Passed, duration)

	// 11. Teardown only when this method owns the harness.
	if ownsHarness {
		_ = h.Teardown(ctx)
	}

	return result
}

// Report returns the current coverage matrix.
func (r *SuiteRunner) Report() CoverageMatrix {
	return r.coverage.Matrix()
}

// resolveDatePlaceholders replaces $TODAY and $YESTERDAY placeholders in a
// pipeline config map with concrete date strings derived from now.
func resolveDatePlaceholders(config map[string]any, now time.Time) {
	if config == nil {
		return
	}
	today := now.Format("2006-01-02")
	yesterday := now.AddDate(0, 0, -1).Format("2006-01-02")
	resolveMapDates(config, today, yesterday)
}

// resolveMapDates recursively walks m, replacing "$TODAY" and "$YESTERDAY"
// string values with concrete date strings.
func resolveMapDates(m map[string]any, today, yesterday string) {
	for k, v := range m {
		switch tv := v.(type) {
		case string:
			if tv == "$TODAY" {
				m[k] = today
			} else if tv == "$YESTERDAY" {
				m[k] = yesterday
			}
		case map[string]any:
			resolveMapDates(tv, today, yesterday)
		case []any:
			for i, item := range tv {
				if s, ok := item.(string); ok {
					if s == "$TODAY" {
						tv[i] = today
					} else if s == "$YESTERDAY" {
						tv[i] = yesterday
					}
				}
			}
		}
	}
}

// enrichMutationParams returns a copy of params with the pipeline param
// replaced by nsPipeline (if present) and schedule/date defaulted to "default"
// when absent. This aligns mutation writes with the harness namespace so that
// the evaluator reads from the same pipeline the mutation wrote to.
func enrichMutationParams(params map[string]string, nsPipeline string) map[string]string {
	out := make(map[string]string, len(params)+2)
	for k, v := range params {
		out[k] = v
	}
	if _, ok := out["pipeline"]; ok && nsPipeline != "" {
		out["pipeline"] = nsPipeline
	}
	if out["schedule"] == "" {
		out["schedule"] = "default"
	}
	if out["date"] == "" {
		out["date"] = "default"
	}
	return out
}
