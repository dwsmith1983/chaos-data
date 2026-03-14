package safety

import (
	"fmt"

	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// CheckBlastRadius returns an error if the experiment stats exceed safety limits.
// config must have been validated with SafetyConfig.Validate() before calling.
func CheckBlastRadius(stats types.ExperimentStats, config types.SafetyConfig) error {
	if stats.AffectedPct > float64(config.MaxAffectedPct) {
		return fmt.Errorf(
			"blast radius: affected percentage %.1f%% exceeds maximum %d%%",
			stats.AffectedPct, config.MaxAffectedPct,
		)
	}
	if stats.AffectedPipelines > config.MaxPipelines {
		return fmt.Errorf(
			"blast radius: affected pipelines %d exceeds maximum %d",
			stats.AffectedPipelines, config.MaxPipelines,
		)
	}
	return nil
}
