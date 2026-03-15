package adapter

import (
	"context"

	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// Asserter evaluates declarative assertions against platform-specific state.
type Asserter interface {
	// Supports reports whether this asserter can evaluate the given assertion type.
	// Called at scenario load time for fail-fast validation. Pure — no I/O.
	Supports(assertionType types.AssertionType) bool

	// Evaluate checks whether the assertion's condition holds for its target.
	// Implementations must return promptly when ctx is cancelled or its deadline
	// has elapsed. Single-shot, non-blocking — the engine drives retry/timeout.
	// Returns:
	//   (true, nil)  — condition satisfied
	//   (false, nil) — not yet satisfied (engine retries)
	//   (false, err) — evaluation failed (engine logs, retries)
	Evaluate(ctx context.Context, assertion types.Assertion) (bool, error)
}
