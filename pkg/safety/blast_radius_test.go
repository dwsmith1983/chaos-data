package safety_test

import (
	"strings"
	"testing"

	"github.com/dwsmith1983/chaos-data/pkg/safety"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

func TestCheckBlastRadius(t *testing.T) {
	tests := []struct {
		name    string
		stats   types.ExperimentStats
		config  types.SafetyConfig
		wantErr bool
	}{
		{
			name: "under limit passes",
			stats: types.ExperimentStats{
				AffectedPct:       10.0,
				AffectedPipelines: 2,
			},
			config: types.SafetyConfig{
				MaxAffectedPct: 25,
				MaxPipelines:   5,
			},
			wantErr: false,
		},
		{
			name: "at limit passes (equal is OK)",
			stats: types.ExperimentStats{
				AffectedPct:       25.0,
				AffectedPipelines: 5,
			},
			config: types.SafetyConfig{
				MaxAffectedPct: 25,
				MaxPipelines:   5,
			},
			wantErr: false,
		},
		{
			name: "over limit for pct fails",
			stats: types.ExperimentStats{
				AffectedPct:       30.0,
				AffectedPipelines: 2,
			},
			config: types.SafetyConfig{
				MaxAffectedPct: 25,
				MaxPipelines:   5,
			},
			wantErr: true,
		},
		{
			name: "over limit for pipelines fails",
			stats: types.ExperimentStats{
				AffectedPct:       10.0,
				AffectedPipelines: 8,
			},
			config: types.SafetyConfig{
				MaxAffectedPct: 25,
				MaxPipelines:   5,
			},
			wantErr: true,
		},
		{
			name: "both over fails (returns first error)",
			stats: types.ExperimentStats{
				AffectedPct:       50.0,
				AffectedPipelines: 10,
			},
			config: types.SafetyConfig{
				MaxAffectedPct: 25,
				MaxPipelines:   5,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := safety.CheckBlastRadius(tt.stats, tt.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("CheckBlastRadius() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestCheckBlastRadiusBothOverReturnsFirstError(t *testing.T) {
	stats := types.ExperimentStats{
		AffectedPct:       50.0,
		AffectedPipelines: 10,
	}
	config := types.SafetyConfig{
		MaxAffectedPct: 25,
		MaxPipelines:   5,
	}

	err := safety.CheckBlastRadius(stats, config)
	if err == nil {
		t.Fatal("expected error when both limits exceeded")
	}

	// The first check is AffectedPct, so the error message should mention percentage.
	got := err.Error()
	if !strings.Contains(got, "percentage") {
		t.Errorf("expected error to mention 'percentage', got: %s", got)
	}
}

func TestCheckBlastRadiusZeroConfig(t *testing.T) {
	stats := types.ExperimentStats{
		AffectedPct:       1.0,
		AffectedPipelines: 1,
	}
	var config types.SafetyConfig // zero-value config

	err := safety.CheckBlastRadius(stats, config)
	if err == nil {
		t.Fatal("expected error with zero-value config, any positive stats should exceed zero limits")
	}
}
