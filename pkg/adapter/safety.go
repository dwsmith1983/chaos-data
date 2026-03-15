package adapter

import (
	"context"
	"errors"

	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// ErrCooldownActive is returned by CheckCooldown when the scenario is
// still within its cooldown period and injection should be skipped.
var ErrCooldownActive = errors.New("cooldown active")

// SafetyController enforces safety boundaries for chaos experiments.
type SafetyController interface {
	IsEnabled(ctx context.Context) (bool, error)
	MaxSeverity(ctx context.Context) (types.Severity, error)
	CheckBlastRadius(ctx context.Context, stats types.ExperimentStats) error

	// CheckSLAWindow reports whether chaos injection is safe for the given
	// pipeline. It returns true when the pipeline is NOT within an SLA
	// window and chaos injection may proceed. It returns false when the
	// pipeline IS within its SLA window and chaos should be skipped.
	CheckSLAWindow(ctx context.Context, pipeline string) (bool, error)

	// CheckCooldown returns ErrCooldownActive when the scenario was
	// injected too recently (within the configured cooldown duration).
	// A nil return means the scenario may proceed.
	CheckCooldown(ctx context.Context, scenario string) error

	// RecordInjection records the current time as the last injection
	// timestamp for the given scenario. Call after a successful mutation.
	RecordInjection(ctx context.Context, scenario string) error
}
