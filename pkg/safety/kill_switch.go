package safety

import (
	"context"
	"fmt"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
)

// CheckKillSwitch returns an error if chaos is disabled via the safety controller.
// If the controller returns an error, chaos is treated as disabled (fail-safe).
func CheckKillSwitch(ctx context.Context, controller adapter.SafetyController) error {
	enabled, err := controller.IsEnabled(ctx)
	if err != nil {
		return fmt.Errorf("kill switch: controller error (fail-safe): %w", err)
	}
	if !enabled {
		return fmt.Errorf("kill switch: chaos is disabled")
	}
	return nil
}
