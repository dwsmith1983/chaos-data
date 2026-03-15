package engine

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/scenario"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// RunProbabilistic runs the engine in probabilistic mode, selecting scenarios
// randomly based on their probability field. It runs in a loop with the given
// interval between iterations until the context is cancelled or duration expires.
//
// On each tick the engine lists all objects from the transport, selects
// scenarios probabilistically using the provided rng, and applies the mutation
// pipeline (safety check, apply, observe) for each selected scenario against
// each object. All MutationRecords are accumulated across iterations.
//
// The provided rng must not be shared across goroutines without external
// synchronization.
func (e *Engine) RunProbabilistic(ctx context.Context, interval time.Duration, rng *rand.Rand) ([]types.MutationRecord, error) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var allRecords []types.MutationRecord

	for {
		select {
		case <-ctx.Done():
			return allRecords, nil
		case <-ticker.C:
			records, err := e.probabilisticIteration(ctx, rng)
			if err != nil {
				return allRecords, fmt.Errorf("probabilistic: %w", err)
			}
			allRecords = append(allRecords, records...)
		}
	}
}

// probabilisticIteration runs a single iteration of the probabilistic loop:
// list objects, select scenarios probabilistically, then apply mutations.
func (e *Engine) probabilisticIteration(ctx context.Context, rng *rand.Rand) ([]types.MutationRecord, error) {
	objects, err := e.transport.List(ctx, "")
	if err != nil {
		return nil, fmt.Errorf("list objects: %w", err)
	}

	selected := scenario.SelectProbabilistic(e.scenarios, rng)
	if len(selected) == 0 {
		return nil, nil
	}

	var records []types.MutationRecord

	for _, obj := range objects {
		if err := ctx.Err(); err != nil {
			return records, nil
		}

		// Check kill switch once per object (not per scenario).
		if e.safety != nil {
			enabled, safeErr := e.safety.IsEnabled(ctx)
			if safeErr != nil {
				return records, fmt.Errorf("safety controller: %w", safeErr)
			}
			if !enabled {
				return records, nil
			}
		}

		// Determine max severity once per object.
		var maxSeverity types.Severity
		if e.safety != nil {
			sev, sevErr := e.safety.MaxSeverity(ctx)
			if sevErr != nil {
				return records, fmt.Errorf("max severity: %w", sevErr)
			}
			maxSeverity = sev
		}

		for _, sc := range selected {
			// Check target filter.
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

			// Check cooldown for this scenario.
			if e.safety != nil {
				if cdErr := e.safety.CheckCooldown(ctx, sc.Name); cdErr != nil {
					if errors.Is(cdErr, adapter.ErrCooldownActive) {
						continue
					}
					// Non-sentinel cooldown errors are recorded and continue
					// (matching probabilistic error-handling pattern).
					records = append(records, types.MutationRecord{
						ObjectKey: obj.Key,
						Mutation:  sc.Mutation.Type,
						Params:    sc.Mutation.Params,
						Applied:   false,
						Error:     fmt.Sprintf("scenario %q: cooldown: %v", sc.Name, cdErr),
						Timestamp: time.Now(),
					})
					continue
				}
			}

			// Get mutation from registry.
			m, err := e.mutations.Get(sc.Mutation.Type)
			if err != nil {
				// Registry errors are configuration problems; record and continue.
				records = append(records, types.MutationRecord{
					ObjectKey: obj.Key,
					Mutation:  sc.Mutation.Type,
					Params:    sc.Mutation.Params,
					Applied:   false,
					Error:     fmt.Sprintf("scenario %q: %v", sc.Name, err),
					Timestamp: time.Now(),
				})
				continue
			}

			// Apply the mutation. In probabilistic mode multiple scenarios
			// may target the same object. A mutation can fail because a
			// prior scenario already moved/deleted the file. We record the
			// failure and continue rather than aborting the entire iteration.
			record, err := m.Apply(ctx, obj, e.transport, sc.Mutation.Params)
			if err != nil {
				records = append(records, types.MutationRecord{
					ObjectKey: obj.Key,
					Mutation:  sc.Mutation.Type,
					Params:    sc.Mutation.Params,
					Applied:   false,
					Error:     fmt.Sprintf("scenario %q: apply %q: %v", sc.Name, sc.Mutation.Type, err),
					Timestamp: time.Now(),
				})
				continue
			}

			records = append(records, record)

			// Emit a ChaosEvent only for successfully applied mutations.
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
					Mode:      "probabilistic",
				}
				if emitErr := e.emitter.Emit(ctx, event); emitErr != nil {
					return records, fmt.Errorf("emit event: %w", emitErr)
				}
			}

			// Record injection for cooldown tracking.
			if e.safety != nil {
				if riErr := e.safety.RecordInjection(ctx, sc.Name); riErr != nil {
					records = append(records, types.MutationRecord{
						ObjectKey: obj.Key,
						Mutation:  sc.Mutation.Type,
						Params:    sc.Mutation.Params,
						Applied:   false,
						Error:     fmt.Sprintf("scenario %q: record injection: %v", sc.Name, riErr),
						Timestamp: time.Now(),
					})
					continue
				}
			}
		}
	}

	return records, nil
}
