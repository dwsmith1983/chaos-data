package local

import (
	"context"
	"fmt"
	"sync"
	"time"

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
	config       types.SafetyConfig
	mu           sync.Mutex
	lastInjected map[string]time.Time

	// Now returns the current time. It defaults to time.Now and can be
	// overridden in tests to control time-dependent behavior.
	Now func() time.Time
}

// NewConfigSafety creates a ConfigSafety from the given SafetyConfig.
func NewConfigSafety(config types.SafetyConfig) *ConfigSafety {
	return &ConfigSafety{
		config:       config,
		lastInjected: make(map[string]time.Time),
		Now:          time.Now,
	}
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
// blast radius limits. Returns an error if AffectedPct exceeds MaxAffectedPct,
// AffectedPipelines exceeds MaxPipelines, HeldBytes exceeds MaxHeldBytes
// (when MaxHeldBytes > 0), or MutationsApplied exceeds MaxMutations (when
// MaxMutations > 0). A zero value for MaxHeldBytes or MaxMutations means
// unlimited — no check is performed.
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
	if c.config.MaxHeldBytes > 0 && stats.HeldBytes > c.config.MaxHeldBytes {
		return fmt.Errorf(
			"blast radius exceeded: %d held bytes > max %d",
			stats.HeldBytes, c.config.MaxHeldBytes,
		)
	}
	if c.config.MaxMutations > 0 && stats.MutationsApplied > c.config.MaxMutations {
		return fmt.Errorf(
			"blast radius exceeded: %d mutations applied > max %d",
			stats.MutationsApplied, c.config.MaxMutations,
		)
	}
	return nil
}

// CheckSLAWindow always returns true (safe to proceed) for local development.
// There is no real SLA enforcement in the local adapter.
func (c *ConfigSafety) CheckSLAWindow(_ context.Context, _ string) (bool, error) {
	return true, nil
}

// CheckCooldown checks whether the scenario is within its cooldown period.
// Returns adapter.ErrCooldownActive when the scenario was injected within
// the configured CooldownDuration. A nil return means the scenario may proceed.
func (c *ConfigSafety) CheckCooldown(_ context.Context, scenario string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	last, ok := c.lastInjected[scenario]
	if !ok {
		return nil
	}

	elapsed := c.Now().Sub(last)
	if elapsed < c.config.CooldownDuration.Duration {
		return adapter.ErrCooldownActive
	}
	return nil
}

// RecordInjection records the current time as the last injection for a scenario.
func (c *ConfigSafety) RecordInjection(_ context.Context, scenario string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.lastInjected[scenario] = c.Now()
	return nil
}
