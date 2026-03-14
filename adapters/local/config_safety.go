package local

import (
	"context"
	"fmt"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// Compile-time interface assertion.
var _ adapter.SafetyController = (*ConfigSafety)(nil)

// ConfigSafety implements adapter.SafetyController using a static
// SafetyConfig, suitable for local development and testing.
//
// Note: CheckSLAWindow always returns true (safe to proceed) in this
// implementation. It performs no real SLA enforcement and should not be
// relied upon for production safety decisions. Production adapters must
// implement actual SLA-window checking against a live schedule source.
type ConfigSafety struct {
	config types.SafetyConfig
}

// NewConfigSafety creates a ConfigSafety from the given SafetyConfig.
func NewConfigSafety(config types.SafetyConfig) *ConfigSafety {
	return &ConfigSafety{config: config}
}

// IsEnabled returns the KillSwitchEnabled value from the config.
func (c *ConfigSafety) IsEnabled(_ context.Context) (bool, error) {
	return c.config.KillSwitchEnabled, nil
}

// MaxSeverity returns the MaxSeverity value from the config.
func (c *ConfigSafety) MaxSeverity(_ context.Context) (types.Severity, error) {
	return c.config.MaxSeverity, nil
}

// CheckBlastRadius verifies that experiment stats are within the configured
// blast radius limits. Returns an error if AffectedPct exceeds MaxAffectedPct
// or AffectedPipelines exceeds MaxPipelines.
func (c *ConfigSafety) CheckBlastRadius(_ context.Context, stats types.ExperimentStats) error {
	if stats.AffectedPct > float64(c.config.MaxAffectedPct) {
		return fmt.Errorf(
			"blast radius exceeded: affected %.1f%% > max %d%%",
			stats.AffectedPct, c.config.MaxAffectedPct,
		)
	}
	if stats.AffectedPipelines > c.config.MaxPipelines {
		return fmt.Errorf(
			"blast radius exceeded: %d pipelines > max %d",
			stats.AffectedPipelines, c.config.MaxPipelines,
		)
	}
	return nil
}

// CheckSLAWindow always returns true (safe to proceed) for local development.
// There is no real SLA enforcement in the local adapter.
func (c *ConfigSafety) CheckSLAWindow(_ context.Context, _ string) (bool, error) {
	return true, nil
}
