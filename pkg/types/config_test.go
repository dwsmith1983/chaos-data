package types_test

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/dwsmith1983/chaos-data/pkg/types"
	"gopkg.in/yaml.v3"
)

func TestEngineConfigYAMLUnmarshal(t *testing.T) {
	input := `
mode: deterministic
adapter:
  type: local
  settings:
    path: /tmp/data
safety:
  max_severity: moderate
  max_affected_pct: 25
  max_pipelines: 5
  cooldown_duration: 5m
  kill_switch_enabled: true
  sla_window_minutes: 30
`

	var cfg types.EngineConfig
	if err := yaml.Unmarshal([]byte(input), &cfg); err != nil {
		t.Fatalf("yaml.Unmarshal() error: %v", err)
	}

	if cfg.Mode != "deterministic" {
		t.Errorf("Mode = %q, want %q", cfg.Mode, "deterministic")
	}
	if cfg.Adapter.Type != "local" {
		t.Errorf("Adapter.Type = %q, want %q", cfg.Adapter.Type, "local")
	}
	if cfg.Adapter.Settings["path"] != "/tmp/data" {
		t.Errorf("Adapter.Settings[path] = %q, want %q", cfg.Adapter.Settings["path"], "/tmp/data")
	}
	if cfg.Safety.MaxSeverity != types.SeverityModerate {
		t.Errorf("Safety.MaxSeverity = %v, want %v", cfg.Safety.MaxSeverity, types.SeverityModerate)
	}
	if cfg.Safety.MaxAffectedPct != 25 {
		t.Errorf("Safety.MaxAffectedPct = %d, want %d", cfg.Safety.MaxAffectedPct, 25)
	}
	if cfg.Safety.MaxPipelines != 5 {
		t.Errorf("Safety.MaxPipelines = %d, want %d", cfg.Safety.MaxPipelines, 5)
	}
	if cfg.Safety.CooldownDuration.Duration != 5*time.Minute {
		t.Errorf("Safety.CooldownDuration = %v, want %v", cfg.Safety.CooldownDuration.Duration, 5*time.Minute)
	}
	if !cfg.Safety.KillSwitchEnabled {
		t.Error("Safety.KillSwitchEnabled = false, want true")
	}
	if cfg.Safety.SLAWindowMinutes != 30 {
		t.Errorf("Safety.SLAWindowMinutes = %d, want %d", cfg.Safety.SLAWindowMinutes, 30)
	}
}

func TestEngineConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     types.EngineConfig
		wantErr bool
	}{
		{
			name: "valid deterministic",
			cfg: types.EngineConfig{
				Mode: "deterministic",
				Safety: types.SafetyConfig{
					MaxSeverity:    types.SeverityModerate,
					MaxAffectedPct: 25,
					MaxPipelines:   5,
				},
			},
			wantErr: false,
		},
		{
			name: "valid probabilistic",
			cfg: types.EngineConfig{
				Mode: "probabilistic",
				Safety: types.SafetyConfig{
					MaxSeverity:    types.SeverityLow,
					MaxAffectedPct: 10,
					MaxPipelines:   1,
				},
			},
			wantErr: false,
		},
		{
			name: "valid replay",
			cfg: types.EngineConfig{
				Mode: "replay",
				Safety: types.SafetyConfig{
					MaxSeverity:    types.SeverityCritical,
					MaxAffectedPct: 100,
					MaxPipelines:   0,
				},
			},
			wantErr: false,
		},
		{
			name: "invalid mode",
			cfg: types.EngineConfig{
				Mode: "chaos",
				Safety: types.SafetyConfig{
					MaxSeverity:    types.SeverityModerate,
					MaxAffectedPct: 25,
					MaxPipelines:   5,
				},
			},
			wantErr: true,
		},
		{
			name: "empty mode",
			cfg: types.EngineConfig{
				Mode: "",
				Safety: types.SafetyConfig{
					MaxSeverity:    types.SeverityModerate,
					MaxAffectedPct: 25,
					MaxPipelines:   5,
				},
			},
			wantErr: true,
		},
		{
			name: "invalid severity propagates",
			cfg: types.EngineConfig{
				Mode: "deterministic",
				Safety: types.SafetyConfig{
					MaxSeverity:    types.Severity(99),
					MaxAffectedPct: 25,
					MaxPipelines:   5,
				},
			},
			wantErr: true,
		},
		{
			name: "zero severity is valid (unset)",
			cfg: types.EngineConfig{
				Mode: "deterministic",
				Safety: types.SafetyConfig{
					MaxAffectedPct: 25,
					MaxPipelines:   5,
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("EngineConfig.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestEngineConfigValidateInvalidModeError(t *testing.T) {
	cfg := types.EngineConfig{
		Mode: "chaos",
		Safety: types.SafetyConfig{
			MaxAffectedPct: 25,
			MaxPipelines:   5,
		},
	}
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for invalid mode")
	}
	if !errors.Is(err, types.ErrInvalidMode) {
		t.Errorf("expected ErrInvalidMode, got: %v", err)
	}
}

func TestEngineConfigValidateInvalidSeverityError(t *testing.T) {
	cfg := types.EngineConfig{
		Mode: "deterministic",
		Safety: types.SafetyConfig{
			MaxSeverity:    types.Severity(99),
			MaxAffectedPct: 25,
			MaxPipelines:   5,
		},
	}
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for invalid severity")
	}
	if !errors.Is(err, types.ErrInvalidSeverity) {
		t.Errorf("expected ErrInvalidSeverity, got: %v", err)
	}
}

func TestSafetyConfig_Validate_NegativeMaxHeldBytes(t *testing.T) {
	t.Parallel()

	cfg := types.SafetyConfig{
		MaxAffectedPct: 25,
		MaxPipelines:   5,
		MaxHeldBytes:   -1,
	}
	err := cfg.Validate()
	if err == nil {
		t.Fatal("Validate() error = nil, want error for negative MaxHeldBytes")
	}
}

func TestSafetyConfig_Validate_NegativeMaxMutations(t *testing.T) {
	t.Parallel()

	cfg := types.SafetyConfig{
		MaxAffectedPct: 25,
		MaxPipelines:   5,
		MaxMutations:   -1,
	}
	err := cfg.Validate()
	if err == nil {
		t.Fatal("Validate() error = nil, want error for negative MaxMutations")
	}
}

func TestSafetyConfig_Validate_ZeroValues(t *testing.T) {
	t.Parallel()

	cfg := types.SafetyConfig{
		MaxAffectedPct: 25,
		MaxPipelines:   5,
		MaxHeldBytes:   0,
		MaxMutations:   0,
	}
	err := cfg.Validate()
	if err != nil {
		t.Fatalf("Validate() error = %v, want nil for zero MaxHeldBytes/MaxMutations (0 = unlimited)", err)
	}
}

func TestSafetyConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     types.SafetyConfig
		wantErr bool
	}{
		{
			name: "valid config",
			cfg: types.SafetyConfig{
				MaxSeverity:       types.SeverityModerate,
				MaxAffectedPct:    25,
				MaxPipelines:      5,
				CooldownDuration:  types.Duration{Duration: 5 * time.Minute},
				KillSwitchEnabled: true,
				SLAWindowMinutes:  30,
			},
			wantErr: false,
		},
		{
			name: "zero values are valid",
			cfg: types.SafetyConfig{
				MaxAffectedPct: 0,
				MaxPipelines:   0,
			},
			wantErr: false,
		},
		{
			name: "max_affected_pct at 100 is valid",
			cfg: types.SafetyConfig{
				MaxAffectedPct: 100,
				MaxPipelines:   1,
			},
			wantErr: false,
		},
		{
			name: "max_affected_pct over 100",
			cfg: types.SafetyConfig{
				MaxAffectedPct: 101,
				MaxPipelines:   5,
			},
			wantErr: true,
		},
		{
			name: "negative max_affected_pct",
			cfg: types.SafetyConfig{
				MaxAffectedPct: -1,
				MaxPipelines:   5,
			},
			wantErr: true,
		},
		{
			name: "negative max_pipelines",
			cfg: types.SafetyConfig{
				MaxAffectedPct: 25,
				MaxPipelines:   -1,
			},
			wantErr: true,
		},
		{
			name: "negative cooldown_duration",
			cfg: types.SafetyConfig{
				MaxAffectedPct:   25,
				MaxPipelines:     5,
				CooldownDuration: types.Duration{Duration: -1 * time.Minute},
			},
			wantErr: true,
		},
		{
			name: "negative sla_window_minutes",
			cfg: types.SafetyConfig{
				MaxAffectedPct:   25,
				MaxPipelines:     5,
				SLAWindowMinutes: -1,
			},
			wantErr: true,
		},
		{
			name: "invalid max_severity",
			cfg: types.SafetyConfig{
				MaxSeverity:    types.Severity(99),
				MaxAffectedPct: 25,
				MaxPipelines:   5,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("SafetyConfig.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDefaults(t *testing.T) {
	cfg := types.Defaults()

	if cfg.Mode != "deterministic" {
		t.Errorf("Mode = %q, want %q", cfg.Mode, "deterministic")
	}
	if cfg.Safety.MaxSeverity != types.SeverityModerate {
		t.Errorf("Safety.MaxSeverity = %v, want %v", cfg.Safety.MaxSeverity, types.SeverityModerate)
	}
	if cfg.Safety.MaxAffectedPct != 25 {
		t.Errorf("Safety.MaxAffectedPct = %d, want %d", cfg.Safety.MaxAffectedPct, 25)
	}
	if cfg.Safety.MaxPipelines != 5 {
		t.Errorf("Safety.MaxPipelines = %d, want %d", cfg.Safety.MaxPipelines, 5)
	}
	if cfg.Safety.CooldownDuration.Duration != 5*time.Minute {
		t.Errorf("Safety.CooldownDuration = %v, want %v", cfg.Safety.CooldownDuration.Duration, 5*time.Minute)
	}
	if !cfg.Safety.KillSwitchEnabled {
		t.Error("Safety.KillSwitchEnabled = false, want true")
	}
	if cfg.Safety.SLAWindowMinutes != 30 {
		t.Errorf("Safety.SLAWindowMinutes = %d, want %d", cfg.Safety.SLAWindowMinutes, 30)
	}

	// Defaults should pass validation.
	if err := cfg.Validate(); err != nil {
		t.Errorf("Defaults().Validate() error: %v", err)
	}
}

func TestEngineConfigJSONRoundTrip(t *testing.T) {
	original := types.Defaults()
	original.Adapter = types.AdapterConfig{
		Type: "local",
		Settings: map[string]string{
			"path": "/tmp/data",
		},
	}

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("json.Marshal() error: %v", err)
	}

	var decoded types.EngineConfig
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("json.Unmarshal() error: %v", err)
	}

	if decoded.Mode != original.Mode {
		t.Errorf("Mode = %q, want %q", decoded.Mode, original.Mode)
	}
	if decoded.Adapter.Type != original.Adapter.Type {
		t.Errorf("Adapter.Type = %q, want %q", decoded.Adapter.Type, original.Adapter.Type)
	}
	if decoded.Adapter.Settings["path"] != original.Adapter.Settings["path"] {
		t.Errorf("Adapter.Settings[path] = %q, want %q",
			decoded.Adapter.Settings["path"], original.Adapter.Settings["path"])
	}
	if decoded.Safety.MaxSeverity != original.Safety.MaxSeverity {
		t.Errorf("Safety.MaxSeverity = %v, want %v",
			decoded.Safety.MaxSeverity, original.Safety.MaxSeverity)
	}
	if decoded.Safety.MaxAffectedPct != original.Safety.MaxAffectedPct {
		t.Errorf("Safety.MaxAffectedPct = %d, want %d",
			decoded.Safety.MaxAffectedPct, original.Safety.MaxAffectedPct)
	}
	if decoded.Safety.MaxPipelines != original.Safety.MaxPipelines {
		t.Errorf("Safety.MaxPipelines = %d, want %d",
			decoded.Safety.MaxPipelines, original.Safety.MaxPipelines)
	}
	if decoded.Safety.CooldownDuration != original.Safety.CooldownDuration {
		t.Errorf("Safety.CooldownDuration = %v, want %v",
			decoded.Safety.CooldownDuration, original.Safety.CooldownDuration)
	}
	if decoded.Safety.KillSwitchEnabled != original.Safety.KillSwitchEnabled {
		t.Errorf("Safety.KillSwitchEnabled = %v, want %v",
			decoded.Safety.KillSwitchEnabled, original.Safety.KillSwitchEnabled)
	}
	if decoded.Safety.SLAWindowMinutes != original.Safety.SLAWindowMinutes {
		t.Errorf("Safety.SLAWindowMinutes = %d, want %d",
			decoded.Safety.SLAWindowMinutes, original.Safety.SLAWindowMinutes)
	}
}

func TestExperimentConfigFields(t *testing.T) {
	cfg := types.ExperimentConfig{
		Scenarios: []string{"latency", "corruption"},
		Duration:  types.Duration{Duration: 10 * time.Minute},
		Mode:      "deterministic",
		DryRun:    true,
	}

	if len(cfg.Scenarios) != 2 {
		t.Errorf("Scenarios length = %d, want %d", len(cfg.Scenarios), 2)
	}
	if cfg.Duration.Duration != 10*time.Minute {
		t.Errorf("Duration = %v, want %v", cfg.Duration.Duration, 10*time.Minute)
	}
	if cfg.Mode != "deterministic" {
		t.Errorf("Mode = %q, want %q", cfg.Mode, "deterministic")
	}
	if !cfg.DryRun {
		t.Error("DryRun = false, want true")
	}
}

func TestExperimentConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     types.ExperimentConfig
		wantErr bool
	}{
		{
			name: "valid config",
			cfg: types.ExperimentConfig{
				Scenarios: []string{"latency"},
				Duration:  types.Duration{Duration: 10 * time.Minute},
				Mode:      "deterministic",
			},
			wantErr: false,
		},
		{
			name: "valid zero duration",
			cfg: types.ExperimentConfig{
				Mode: "probabilistic",
			},
			wantErr: false,
		},
		{
			name: "invalid mode",
			cfg: types.ExperimentConfig{
				Mode: "chaos",
			},
			wantErr: true,
		},
		{
			name: "empty mode",
			cfg: types.ExperimentConfig{
				Mode: "",
			},
			wantErr: true,
		},
		{
			name: "negative duration",
			cfg: types.ExperimentConfig{
				Mode:     "deterministic",
				Duration: types.Duration{Duration: -1 * time.Second},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("ExperimentConfig.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
