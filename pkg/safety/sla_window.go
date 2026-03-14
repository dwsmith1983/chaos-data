package safety

import (
	"context"
	"fmt"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
)

// CheckSLAWindow returns an error if the pipeline is within its SLA window
// and chaos should be skipped. Returns nil if chaos is safe to proceed.
func CheckSLAWindow(ctx context.Context, pipeline string, controller adapter.SafetyController) error {
	safe, err := controller.CheckSLAWindow(ctx, pipeline)
	if err != nil {
		return fmt.Errorf("sla window: controller error (fail-safe): %w", err)
	}
	if !safe {
		return fmt.Errorf("sla window: pipeline %q is within its SLA window, chaos skipped", pipeline)
	}
	return nil
}
