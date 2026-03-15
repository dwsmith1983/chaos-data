package types

import (
	"errors"
	"fmt"
	"time"
)

// ErrInvalidMode is returned when an engine mode is not recognized.
var ErrInvalidMode = errors.New("invalid engine mode")

// EngineConfig holds top-level configuration for the chaos engine.
type EngineConfig struct {
	Mode    string        `yaml:"mode"    json:"mode"`
	Adapter AdapterConfig `yaml:"adapter" json:"adapter"`
	Safety  SafetyConfig  `yaml:"safety"  json:"safety"`
	DryRun             bool     `yaml:"dry_run"             json:"dry_run"`
	AssertWait         bool     `yaml:"assert_wait"         json:"assert_wait"`
	AssertPollInterval Duration `yaml:"assert_poll_interval" json:"assert_poll_interval"`
}

// AdapterConfig identifies which adapter to use and its settings.
type AdapterConfig struct {
	Type     string            `yaml:"type"     json:"type"`
	Settings map[string]string `yaml:"settings" json:"settings"`
}

// SafetyConfig defines safety boundaries for chaos experiments.
type SafetyConfig struct {
	MaxSeverity       Severity `yaml:"max_severity"        json:"max_severity"`
	MaxAffectedPct    int      `yaml:"max_affected_pct"    json:"max_affected_pct"`
	MaxPipelines      int      `yaml:"max_pipelines"       json:"max_pipelines"`
	CooldownDuration  Duration `yaml:"cooldown_duration"   json:"cooldown_duration"`
	KillSwitchEnabled bool     `yaml:"kill_switch_enabled" json:"kill_switch_enabled"`
	SLAWindowMinutes  int      `yaml:"sla_window_minutes"  json:"sla_window_minutes"`
	MaxHeldBytes      int64    `yaml:"max_held_bytes"      json:"max_held_bytes"`
	MaxMutations      int      `yaml:"max_mutations"       json:"max_mutations"`
}

// ExperimentConfig holds the configuration for a single experiment run.
type ExperimentConfig struct {
	Scenarios []string `yaml:"scenarios" json:"scenarios"`
	Duration  Duration `yaml:"duration"  json:"duration"`
	Mode      string   `yaml:"mode"      json:"mode"`
	DryRun    bool     `yaml:"dry_run"   json:"dry_run"`
}

// Validate checks that EngineConfig has a valid mode and that nested
// configs pass their own validation.
func (c EngineConfig) Validate() error {
	if !ValidMode(c.Mode) {
		return fmt.Errorf("%w: %q", ErrInvalidMode, c.Mode)
	}
	if c.AssertPollInterval.Duration < 0 {
		return fmt.Errorf("assert_poll_interval must be >= 0, got %v", c.AssertPollInterval.Duration)
	}
	if c.AssertWait && c.AssertPollInterval.Duration <= 0 {
		return fmt.Errorf("assert_poll_interval must be > 0 when assert_wait is enabled")
	}
	return c.Safety.Validate()
}

// Validate checks that SafetyConfig values fall within acceptable ranges.
func (c SafetyConfig) Validate() error {
	// A zero-value severity means "unset" and is acceptable.
	if c.MaxSeverity != 0 && !c.MaxSeverity.IsValid() {
		return fmt.Errorf("%w: %d", ErrInvalidSeverity, int(c.MaxSeverity))
	}
	if c.MaxAffectedPct < 0 || c.MaxAffectedPct > 100 {
		return fmt.Errorf("max_affected_pct must be 0-100, got %d", c.MaxAffectedPct)
	}
	if c.MaxPipelines < 0 {
		return fmt.Errorf("max_pipelines must be >= 0, got %d", c.MaxPipelines)
	}
	if c.CooldownDuration.Duration < 0 {
		return fmt.Errorf("cooldown_duration must be >= 0, got %v", c.CooldownDuration.Duration)
	}
	if c.SLAWindowMinutes < 0 {
		return fmt.Errorf("sla_window_minutes must be >= 0, got %d", c.SLAWindowMinutes)
	}
	if c.MaxHeldBytes < 0 {
		return fmt.Errorf("max_held_bytes must be >= 0, got %d", c.MaxHeldBytes)
	}
	if c.MaxMutations < 0 {
		return fmt.Errorf("max_mutations must be >= 0, got %d", c.MaxMutations)
	}
	return nil
}

// Validate checks that ExperimentConfig has a valid mode and non-negative duration.
func (c ExperimentConfig) Validate() error {
	if !ValidMode(c.Mode) {
		return fmt.Errorf("%w: %q", ErrInvalidMode, c.Mode)
	}
	if c.Duration.Duration < 0 {
		return fmt.Errorf("duration must be >= 0, got %v", c.Duration.Duration)
	}
	return nil
}

// Defaults returns an EngineConfig populated with sensible default values.
func Defaults() EngineConfig {
	return EngineConfig{
		Mode: "deterministic",
		Safety: SafetyConfig{
			MaxSeverity:       SeverityModerate,
			MaxAffectedPct:    25,
			MaxPipelines:      5,
			CooldownDuration:  Duration{5 * time.Minute},
			KillSwitchEnabled: true,
			SLAWindowMinutes:  30,
		},
	}
}
