package adapter

import (
	"context"

	"github.com/dwsmith1983/chaos-data/pkg/types"
)

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
}
