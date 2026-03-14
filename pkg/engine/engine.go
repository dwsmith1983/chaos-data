// Package engine provides the core chaos injection engine that orchestrates
// mutations against data objects using configured scenarios and safety controls.
package engine

import (
	"context"
	"fmt"
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
		mutations: mutations,
		scenarios: scenarios,
	}
	for _, opt := range opts {
		opt(eng)
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
		// Filter scenarios by target filter.
		filter := types.ObjectFilter{
			Prefix: sc.Target.Filter.Prefix,
			Match:  sc.Target.Filter.Match,
		}
		if !filter.Matches(obj) {
			continue
		}

		// Check severity against safety controller.
		if e.safety != nil && sc.Severity.ExceedsThreshold(maxSeverity) {
			continue
		}

		// Get mutation from registry.
		m, err := e.mutations.Get(sc.Mutation.Type)
		if err != nil {
			return records, fmt.Errorf("process object %q: scenario %q: %w", obj.Key, sc.Name, err)
		}

		// Apply the mutation.
		record, err := m.Apply(ctx, obj, e.transport, sc.Mutation.Params)
		if err != nil {
			return records, fmt.Errorf("process object %q: scenario %q: apply %q: %w", obj.Key, sc.Name, sc.Mutation.Type, err)
		}

		records = append(records, record)

		// Emit a ChaosEvent.
		if e.emitter != nil {
			now := time.Now()
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
				return records, fmt.Errorf("process object %q: emit event: %w", obj.Key, err)
			}
		}
	}

	return records, nil
}

// Run executes a deterministic chaos run against all objects in the staging area.
// It lists all objects via the transport, then processes each one.
//
// Run uses fail-fast semantics: if any mutation fails, it returns immediately
// with the partial results collected so far and the error. This is intentional
// for chaos testing -- partial mutations create unpredictable state that should
// not be compounded by continuing to mutate additional objects.
func (e *Engine) Run(ctx context.Context) ([]types.MutationRecord, error) {
	objects, err := e.transport.List(ctx, "")
	if err != nil {
		return nil, fmt.Errorf("run: list objects: %w", err)
	}

	var allRecords []types.MutationRecord

	for _, obj := range objects {
		if err := ctx.Err(); err != nil {
			return allRecords, fmt.Errorf("run: %w", err)
		}

		records, err := e.ProcessObject(ctx, obj)
		if err != nil {
			return allRecords, fmt.Errorf("run: %w", err)
		}
		allRecords = append(allRecords, records...)
	}

	return allRecords, nil
}
