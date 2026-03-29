// Package engine provides the core chaos injection engine that orchestrates
// mutations against data objects using configured scenarios and safety controls.
package engine

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/mutation"
	"github.com/dwsmith1983/chaos-data/pkg/scenario"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// Engine is the core chaos injection engine.
type Engine struct {
	config    types.EngineConfig
	transport adapter.DataTransport
	emitter   adapter.EventEmitter
	safety    adapter.SafetyController
	resolver  adapter.DependencyResolver
	asserter  adapter.Asserter
	clock     adapter.Clock
	mutations *mutation.Registry
	scenarios []scenario.Scenario
}

// EngineOption configures optional Engine dependencies.
type EngineOption func(*Engine)

// WithEmitter attaches an EventEmitter to the engine.
func WithEmitter(e adapter.EventEmitter) EngineOption {
	return func(eng *Engine) {
		eng.emitter = e
	}
}

// WithSafety attaches a SafetyController to the engine.
func WithSafety(s adapter.SafetyController) EngineOption {
	return func(eng *Engine) {
		eng.safety = s
	}
}

// WithDependencyResolver attaches a DependencyResolver to the engine.
// When set, experiments created by StartExperiment will use the resolver
// to compute downstream blast radius via Experiment.BlastRadius().
func WithDependencyResolver(r adapter.DependencyResolver) EngineOption {
	return func(eng *Engine) {
		eng.resolver = r
	}
}

// WithAsserter attaches an Asserter to the engine for evaluating
// non-data_state assertion types in expected_response blocks.
func WithAsserter(a adapter.Asserter) EngineOption {
	return func(eng *Engine) { eng.asserter = a }
}

// WithClock attaches a Clock to the engine for time operations.
// When not set, a WallClock is used by default.
func WithClock(c adapter.Clock) EngineOption {
	return func(eng *Engine) { eng.clock = c }
}

// Clock returns the engine's clock. This is primarily useful for testing.
func (e *Engine) Clock() adapter.Clock { return e.clock }

// New creates an Engine with the given configuration, transport, mutation
// registry, scenarios, and optional dependencies.
func New(
	config types.EngineConfig,
	transport adapter.DataTransport,
	mutations *mutation.Registry,
	scenarios []scenario.Scenario,
	opts ...EngineOption,
) *Engine {
	eng := &Engine{
		config:    config,
		transport: transport,
		clock:     adapter.NewWallClock(),
		mutations: mutations,
		scenarios: scenarios,
	}
	for _, opt := range opts {
		opt(eng)
	}
	if eng.clock == nil {
		eng.clock = adapter.NewWallClock()
	}
	return eng
}

// ProcessObject runs the chaos pipeline for a single data object:
//  1. Check kill switch (if safety controller exists)
//  2. Find matching scenarios (by target filter)
//  3. For each matching scenario:
//     a. Check severity against safety controller
//     b. Look up the mutation in the registry
//     c. Apply the mutation
//     d. Emit a ChaosEvent
//  4. If no scenarios match, pass through unchanged
//
// In deterministic mode, all matching scenarios are applied. The Probability
// field on each scenario is only used in probabilistic mode (future).
//
// Returns a list of MutationRecords for what was applied.
func (e *Engine) ProcessObject(ctx context.Context, obj types.DataObject) ([]types.MutationRecord, error) {
	// Check kill switch first.
	if e.safety != nil {
		enabled, err := e.safety.IsEnabled(ctx)
		if err != nil {
			return nil, fmt.Errorf("process object %q: safety controller: %w", obj.Key, err)
		}
		if !enabled {
			return nil, nil
		}
	}

	// Determine max severity from safety controller.
	var maxSeverity types.Severity
	if e.safety != nil {
		sev, err := e.safety.MaxSeverity(ctx)
		if err != nil {
			return nil, fmt.Errorf("process object %q: max severity: %w", obj.Key, err)
		}
		maxSeverity = sev
	}

	var records []types.MutationRecord

	for _, sc := range e.scenarios {
		record, skipped, err := e.processSingleScenario(ctx, obj, sc, maxSeverity)
		if err != nil {
			return records, err
		}
		if !skipped {
			records = append(records, record)
		}
	}

	return records, nil
}

// isNegativeCondition returns true for conditions where the absence of the
// target constitutes success (e.g. not_exists).
func isNegativeCondition(c types.Condition) bool {
	return c == types.CondNotExists
}

// processSingleScenario runs the chaos pipeline for one scenario against a single object.
// It returns the MutationRecord (if applied), whether the scenario was skipped, and any error.
func (e *Engine) processSingleScenario(ctx context.Context, obj types.DataObject, sc scenario.Scenario, maxSeverity types.Severity) (types.MutationRecord, bool, error) {
	// Filter scenarios by target filter.
	filter := types.ObjectFilter{
		Prefix: sc.Target.Filter.Prefix,
		Match:  sc.Target.Filter.Match,
	}
	if !filter.Matches(obj) {
		return types.MutationRecord{}, true, nil
	}

	// Check severity against safety controller.
	if e.safety != nil && sc.Severity.ExceedsThreshold(maxSeverity) {
		return types.MutationRecord{}, true, nil
	}

	// Check cooldown for this scenario.
	if e.safety != nil {
		if cdErr := e.safety.CheckCooldown(ctx, sc.Name); cdErr != nil {
			if errors.Is(cdErr, adapter.ErrCooldownActive) {
				return types.MutationRecord{}, true, nil
			}
			return types.MutationRecord{}, false, fmt.Errorf("process object %q: scenario %q: cooldown: %w", obj.Key, sc.Name, cdErr)
		}
	}

	// Check SLA window for this scenario — skip if within window.
	if e.safety != nil {
		slaOK, slaErr := e.safety.CheckSLAWindow(ctx, sc.Name)
		if slaErr != nil {
			return types.MutationRecord{}, false, fmt.Errorf("process object %q: scenario %q: sla window: %w", obj.Key, sc.Name, slaErr)
		}
		if !slaOK {
			return types.MutationRecord{}, true, nil // skip scenario — pipeline is within its SLA window
		}
	}

	// Get mutation from registry.
	m, err := e.mutations.Get(sc.Mutation.Type)
	if err != nil {
		return types.MutationRecord{}, false, fmt.Errorf("process object %q: scenario %q: %w", obj.Key, sc.Name, err)
	}

	// Apply the mutation (or simulate in dry-run mode).
	var record types.MutationRecord
	if e.config.DryRun {
		record = types.MutationRecord{
			ObjectKey: obj.Key,
			Scenario:  sc.Name,
			Mutation:  sc.Mutation.Type,
			Params:    sc.Mutation.Params,
			Applied:   false,
			Error:     "dry-run",
			Timestamp: e.clock.Now(),
		}
	} else {
		var applyErr error
		record, applyErr = m.Apply(ctx, obj, e.transport, sc.Mutation.Params, e.clock)
		if applyErr != nil {
			return types.MutationRecord{}, false, fmt.Errorf("process object %q: scenario %q: apply %q: %w", obj.Key, sc.Name, sc.Mutation.Type, applyErr)
		}
		record.Scenario = sc.Name
	}

	// Emit a ChaosEvent.
	if e.emitter != nil {
		now := e.clock.Now()
		event := types.ChaosEvent{
			ID:        fmt.Sprintf("%s-%s-%d", sc.Name, obj.Key, now.UnixNano()),
			Scenario:  sc.Name,
			Category:  sc.Category,
			Severity:  sc.Severity,
			Target:    obj.Key,
			Mutation:  sc.Mutation.Type,
			Params:    sc.Mutation.Params,
			Timestamp: now,
			Mode:      e.config.Mode,
		}
		if err := e.emitter.Emit(ctx, event); err != nil {
			return types.MutationRecord{}, false, fmt.Errorf("process object %q: emit event: %w", obj.Key, err)
		}
	}

	// Record injection for cooldown tracking (skip in dry-run).
	if e.safety != nil && !e.config.DryRun {
		if riErr := e.safety.RecordInjection(ctx, sc.Name); riErr != nil {
			return types.MutationRecord{}, false, fmt.Errorf("process object %q: record injection: %w", obj.Key, riErr)
		}
	}

	return record, false, nil
}

// EvaluateAssertions polls all assertions from the given scenarios until all are
// satisfied or the maximum Within deadline (across all scenarios) elapses.
//
// It collects assertions and the maximum Within duration from the scenarios,
// then delegates to EvaluateAssertionSet for the actual polling logic.
//
// Returns nil when no scenarios have Expected set.
func (e *Engine) EvaluateAssertions(ctx context.Context, scenarios []scenario.Scenario) []types.AssertionResult {
	var allAssertions []types.Assertion
	var maxWithin time.Duration

	for _, sc := range scenarios {
		if sc.Expected == nil {
			continue
		}
		if sc.Expected.Within.Duration > maxWithin {
			maxWithin = sc.Expected.Within.Duration
		}
		allAssertions = append(allAssertions, sc.Expected.Asserts...)
	}

	if len(allAssertions) == 0 {
		return nil
	}

	return EvaluateAssertionSet(
		ctx,
		allAssertions,
		maxWithin,
		e.asserter,
		e.clock,
		e.transport,
		e.config.AssertPollInterval.Duration,
	)
}

// Run executes a deterministic chaos run against all objects in the staging area.
// It lists all objects via the transport, then processes each one.
//
// Run uses fail-fast semantics: if any mutation fails, it returns immediately
// with the partial results collected so far and the error. This is intentional
// for chaos testing -- partial mutations create unpredictable state that should
// not be compounded by continuing to mutate additional objects.
//
// Blast radius enforcement uses fail-open semantics: when CheckBlastRadius
// returns an error, Run stops injecting further objects and returns what was
// collected so far without surfacing the blast radius error to the caller.
func (e *Engine) Run(ctx context.Context) ([]types.MutationRecord, error) {
	objects, err := e.transport.List(ctx, "")
	if err != nil {
		return nil, fmt.Errorf("run: list objects: %w", err)
	}

	totalObjects := len(objects)

	var (
		allRecords       []types.MutationRecord
		affectedTargets  = make(map[string]struct{})
		affectedScens    = make(map[string]struct{})
		mutationsApplied int
	)

	for _, obj := range objects {
		if err := ctx.Err(); err != nil {
			return allRecords, fmt.Errorf("run: %w", err)
		}

		records, err := e.ProcessObject(ctx, obj)
		if err != nil {
			return allRecords, fmt.Errorf("run: %w", err)
		}

		// Track affected targets and scenario names from applied records.
		for _, r := range records {
			if r.Applied {
				affectedTargets[r.ObjectKey] = struct{}{}
				affectedScens[r.Mutation] = struct{}{}
				mutationsApplied++
			}
		}

		allRecords = append(allRecords, records...)

		// Check blast radius after each object. Fail-open: stop injecting
		// but do not surface the error to the caller.
		if e.safety != nil {
			stats := types.ExperimentStats{
				TotalObjects:      totalObjects,
				AffectedTargets:   len(affectedTargets),
				AffectedPipelines: len(affectedScens),
				MutationsApplied:  mutationsApplied,
			}
			if totalObjects > 0 {
				stats.AffectedPct = float64(stats.AffectedTargets) / float64(totalObjects) * 100
			}

			// Sum sizes of currently held objects to track held bytes.
			// ListHeld errors are silently ignored: fail-open on stats collection.
			if held, listErr := e.transport.ListHeld(ctx); listErr == nil {
				for _, h := range held {
					stats.HeldBytes += h.Size
				}
			}

			if brErr := e.safety.CheckBlastRadius(ctx, stats); brErr != nil {
				break // stop injecting; return partial results without error
			}
		}
	}

	// Evaluate assertions after the mutation loop.
	if e.config.AssertWait {
		// Collect scenarios where at least one Applied=true mutation was recorded.
		scenarioApplied := make(map[string]bool)
		for _, r := range allRecords {
			if r.Applied {
				scenarioApplied[r.Scenario] = true
			}
		}
		var appliedScenarios []scenario.Scenario
		for _, sc := range e.scenarios {
			if sc.Expected != nil && scenarioApplied[sc.Name] {
				appliedScenarios = append(appliedScenarios, sc)
			}
		}
		if len(appliedScenarios) > 0 {
			assertResults := e.EvaluateAssertions(ctx, appliedScenarios)
			if len(assertResults) > 0 && e.emitter != nil {
				now := e.clock.Now()
				var names []string
				for _, sc := range appliedScenarios {
					names = append(names, sc.Name)
				}
				event := types.ChaosEvent{
					ID:         fmt.Sprintf("assert-%d", now.UnixNano()),
					Scenario:   strings.Join(names, ","),
					Category:   "assertion",
					Severity:   types.SeverityLow,
					Mutation:   "assertion_evaluation",
					Assertions: assertResults,
					Timestamp:  now,
					Mode:       e.config.Mode,
				}
				_ = e.emitter.Emit(ctx, event) // best-effort: assertion event emission does not fail the run
			}
		}
	} else {
		// AssertWait=false: write unevaluated assertion placeholders.
		var assertResults []types.AssertionResult
		var names []string
		for _, sc := range e.scenarios {
			if sc.Expected == nil {
				continue
			}
			names = append(names, sc.Name)
			for _, a := range sc.Expected.Asserts {
				assertResults = append(assertResults, types.AssertionResult{Assertion: a})
			}
		}
		if len(assertResults) > 0 && e.emitter != nil {
			now := e.clock.Now()
			event := types.ChaosEvent{
				ID:         fmt.Sprintf("assert-%d", now.UnixNano()),
				Scenario:   strings.Join(names, ","),
				Category:   "assertion",
				Severity:   types.SeverityLow,
				Mutation:   "assertion_evaluation",
				Assertions: assertResults,
				Timestamp:  now,
				Mode:       e.config.Mode,
			}
			_ = e.emitter.Emit(ctx, event) // best-effort: assertion event emission does not fail the run
		}
	}

	return allRecords, nil
}
