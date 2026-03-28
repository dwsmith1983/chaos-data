package interlocksuite

import (
	"context"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
)

// EvalModule is a single evaluation subsystem within the composite evaluator.
// Each module inspects the pipeline config, reads relevant state, and emits
// Interlock events when its domain conditions are met. Modules execute
// sequentially and share mutable state (store + eventWriter), so earlier
// modules' emitted events are visible to later modules.
type EvalModule interface {
	// Name returns a short identifier for logging/debugging.
	Name() string

	// Evaluate inspects the pipeline config and state, emitting events via
	// eventWriter when the module's domain conditions are met. It should
	// silently return nil when the config does not contain its domain section.
	Evaluate(ctx context.Context, params EvalParams) error
}

// EvalParams bundles the shared dependencies passed to each EvalModule.
type EvalParams struct {
	Pipeline    string
	Config      map[string]any
	Store       adapter.StateStore
	EventWriter *LocalEventReader
	Clock       adapter.Clock
	Schedule    string
	Date        string
	SensorKeys  []string // sensor keys written during setup, for modules that need to check readiness
}
