package local_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dwsmith1983/chaos-data/adapters/local"
	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

func TestConfigSafety_IsEnabled(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		enabled bool
		want    bool
	}{
		{
			name:    "kill switch enabled",
			enabled: true,
			want:    true,
		},
		{
			name:    "kill switch disabled",
			enabled: false,
			want:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cs := local.NewConfigSafety(types.SafetyConfig{
				KillSwitchEnabled: tt.enabled,
			})

			got, err := cs.IsEnabled(context.Background())
			if err != nil {
				t.Fatalf("IsEnabled() error = %v", err)
			}
			if got != tt.want {
				t.Errorf("IsEnabled() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestConfigSafety_MaxSeverity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		sev  types.Severity
		want types.Severity
	}{
		{
			name: "low severity",
			sev:  types.SeverityLow,
			want: types.SeverityLow,
		},
		{
			name: "critical severity",
			sev:  types.SeverityCritical,
			want: types.SeverityCritical,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cs := local.NewConfigSafety(types.SafetyConfig{
				MaxSeverity: tt.sev,
			})

			got, err := cs.MaxSeverity(context.Background())
			if err != nil {
				t.Fatalf("MaxSeverity() error = %v", err)
			}
			if got != tt.want {
				t.Errorf("MaxSeverity() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestConfigSafety_CheckBlastRadius(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		config  types.SafetyConfig
		stats   types.ExperimentStats
		wantErr bool
	}{
		{
			name: "within limits",
			config: types.SafetyConfig{
				MaxAffectedPct: 25,
				MaxPipelines:   5,
			},
			stats: types.ExperimentStats{
				AffectedPct:       10.0,
				AffectedPipelines: 2,
			},
			wantErr: false,
		},
		{
			name: "affected pct exceeds limit",
			config: types.SafetyConfig{
				MaxAffectedPct: 25,
				MaxPipelines:   5,
			},
			stats: types.ExperimentStats{
				AffectedPct:       30.0,
				AffectedPipelines: 2,
			},
			wantErr: true,
		},
		{
			name: "pipelines exceed limit",
			config: types.SafetyConfig{
				MaxAffectedPct: 25,
				MaxPipelines:   5,
			},
			stats: types.ExperimentStats{
				AffectedPct:       10.0,
				AffectedPipelines: 6,
			},
			wantErr: true,
		},
		{
			name: "at exact limits",
			config: types.SafetyConfig{
				MaxAffectedPct: 25,
				MaxPipelines:   5,
			},
			stats: types.ExperimentStats{
				AffectedPct:       25.0,
				AffectedPipelines: 5,
			},
			wantErr: false,
		},
		{
			name: "zero limits allow nothing",
			config: types.SafetyConfig{
				MaxAffectedPct: 0,
				MaxPipelines:   0,
			},
			stats: types.ExperimentStats{
				AffectedPct:       1.0,
				AffectedPipelines: 1,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cs := local.NewConfigSafety(tt.config)
			err := cs.CheckBlastRadius(context.Background(), tt.stats)
			if tt.wantErr && err == nil {
				t.Error("CheckBlastRadius() error = nil, want error")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("CheckBlastRadius() error = %v, want nil", err)
			}
		})
	}
}

func TestConfigSafety_CheckSLAWindow(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		config   types.SafetyConfig
		pipeline string
		want     bool
	}{
		{
			name: "no SLA configured returns safe",
			config: types.SafetyConfig{
				SLAWindowMinutes: 0,
			},
			pipeline: "pipeline-1",
			want:     true,
		},
		{
			name: "SLA configured still returns safe for local",
			config: types.SafetyConfig{
				SLAWindowMinutes: 30,
			},
			pipeline: "pipeline-1",
			want:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cs := local.NewConfigSafety(tt.config)
			got, err := cs.CheckSLAWindow(context.Background(), tt.pipeline)
			if err != nil {
				t.Fatalf("CheckSLAWindow() error = %v", err)
			}
			if got != tt.want {
				t.Errorf("CheckSLAWindow() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestConfigSafety_Cooldown(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		setup   func(cs *local.ConfigSafety)
		now     time.Time
		wantErr bool
	}{
		{
			name:    "CheckCooldown_NoPriorInjection",
			setup:   func(_ *local.ConfigSafety) {},
			now:     time.Now(),
			wantErr: false,
		},
		{
			name: "CheckCooldown_ActiveCooldown",
			setup: func(cs *local.ConfigSafety) {
				// Record an injection at "now - 1 minute" so the 5-min cooldown is active.
				cs.Now = func() time.Time { return time.Now().Add(-1 * time.Minute) }
				if err := cs.RecordInjection(context.Background(), "sc-1"); err != nil {
					panic(err)
				}
			},
			now:     time.Now(),
			wantErr: true,
		},
		{
			name: "CheckCooldown_ExpiredCooldown",
			setup: func(cs *local.ConfigSafety) {
				// Record an injection at "now - 10 minutes" so the 5-min cooldown has expired.
				cs.Now = func() time.Time { return time.Now().Add(-10 * time.Minute) }
				if err := cs.RecordInjection(context.Background(), "sc-1"); err != nil {
					panic(err)
				}
			},
			now:     time.Now(),
			wantErr: false,
		},
		{
			name: "RecordInjection_RecordsTimestamp",
			setup: func(cs *local.ConfigSafety) {
				if err := cs.RecordInjection(context.Background(), "sc-1"); err != nil {
					panic(err)
				}
			},
			now:     time.Now(),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cs := local.NewConfigSafety(types.SafetyConfig{
				CooldownDuration: types.Duration{Duration: 5 * time.Minute},
			})
			tt.setup(cs)
			// Reset Now to the test's "current time" for CheckCooldown.
			cs.Now = func() time.Time { return tt.now }

			err := cs.CheckCooldown(context.Background(), "sc-1")
			if tt.wantErr && !errors.Is(err, adapter.ErrCooldownActive) {
				t.Errorf("CheckCooldown() error = %v, want %v", err, adapter.ErrCooldownActive)
			}
			if !tt.wantErr && err != nil {
				t.Errorf("CheckCooldown() error = %v, want nil", err)
			}
		})
	}
}
