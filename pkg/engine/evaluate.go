package engine

import (
	"context"
	"fmt"
	"time"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// EvaluateAssertionSet polls assertions until all are satisfied or the deadline
// elapses. This is the shared evaluation logic used by both Engine.EvaluateAssertions
// (for scenarios) and SuiteRunner (for sequences).
//
// Assertion polarity:
//   - Positive (exists, is_ready, etc.): poll until found → Satisfied=true;
//     timeout → Satisfied=false (FAIL).
//   - Negative (not_exists): if found at any point → Satisfied=false immediately
//     (FAIL); timeout without finding → Satisfied=true (PASS).
//
// Each assertion is routed:
//  1. To the external asserter (if non-nil and Supports returns true)
//  2. To the DataStateAsserter (if transport non-nil) for data_state assertions
//  3. Otherwise an unsupported-type error is recorded
func EvaluateAssertionSet(
	ctx context.Context,
	assertions []types.Assertion,
	within time.Duration,
	asserter adapter.Asserter,
	clk adapter.Clock,
	transport adapter.DataTransport,
	pollInterval time.Duration,
) []types.AssertionResult {
	type pendingAssertion struct {
		assertion types.Assertion
		idx       int
		negative  bool
	}

	results := make([]types.AssertionResult, len(assertions))
	var pending []pendingAssertion

	for i, a := range assertions {
		results[i] = types.AssertionResult{Assertion: a}
		pending = append(pending, pendingAssertion{
			assertion: a,
			idx:       i,
			negative:  isNegativeCondition(a.Condition),
		})
	}

	if len(pending) == 0 {
		return results
	}

	// Pre-validate targets if the asserter supports it.
	if tv, ok := asserter.(adapter.TargetValidator); ok {
		var validated []pendingAssertion
		for _, p := range pending {
			if asserter.Supports(p.assertion.Type) {
				if err := tv.ValidateTarget(p.assertion); err != nil {
					results[p.idx].Error = err.Error()
					continue
				}
			}
			validated = append(validated, p)
		}
		pending = validated
	}

	if len(pending) == 0 {
		return results
	}

	if pollInterval <= 0 {
		pollInterval = time.Second
	}

	ctx, cancel := context.WithTimeout(ctx, within)
	defer cancel()

	ticker := clk.NewTicker(pollInterval)
	defer ticker.Stop()

	var dataAsserter *DataStateAsserter
	if transport != nil {
		dataAsserter = NewDataStateAsserter(transport)
	}

	for {
		select {
		case <-ctx.Done():
			// Timeout: negative assertions still pending are satisfied (never found).
			for _, p := range pending {
				if p.negative {
					results[p.idx].Satisfied = true
					results[p.idx].EvalAt = clk.Now()
				}
			}
			return results
		case <-ticker.C():
			remaining := make([]pendingAssertion, 0, len(pending))
			for _, p := range pending {
				ok, err := evaluateOneAssertion(ctx, p.assertion, asserter, dataAsserter)
				if err != nil {
					results[p.idx].Error = err.Error()
					remaining = append(remaining, p)
					continue
				}
				if p.negative {
					if ok {
						results[p.idx].Satisfied = false
						results[p.idx].EvalAt = clk.Now()
					} else {
						remaining = append(remaining, p)
					}
				} else {
					if ok {
						results[p.idx].Satisfied = true
						results[p.idx].EvalAt = clk.Now()
					} else {
						remaining = append(remaining, p)
					}
				}
			}
			pending = remaining
			if len(pending) == 0 {
				return results
			}
		}
	}
}

// evaluateOneAssertion routes a single assertion to the appropriate evaluator.
func evaluateOneAssertion(ctx context.Context, a types.Assertion, asserter adapter.Asserter, dataAsserter *DataStateAsserter) (bool, error) {
	if asserter != nil && asserter.Supports(a.Type) {
		return asserter.Evaluate(ctx, a)
	}
	if a.Type == types.AssertDataState && dataAsserter != nil {
		return dataAsserter.Evaluate(ctx, a)
	}
	return false, fmt.Errorf("no asserter supports assertion type %q", a.Type)
}
