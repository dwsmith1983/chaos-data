package safety_test

import (
	"testing"

	"github.com/dwsmith1983/chaos-data/pkg/safety"
	"github.com/dwsmith1983/chaos-data/pkg/scenario"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

func TestCheckSeverity(t *testing.T) {
	baseScenario := func(sev types.Severity) scenario.Scenario {
		return scenario.Scenario{
			Name:     "test-scenario",
			Category: "data-quality",
			Severity: sev,
			Version:  1,
			Mutation: scenario.MutationSpec{Type: "null_inject"},
		}
	}

	tests := []struct {
		name       string
		scenario   scenario.Scenario
		maxAllowed types.Severity
		wantErr    bool
	}{
		{
			name:       "at threshold passes",
			scenario:   baseScenario(types.SeverityModerate),
			maxAllowed: types.SeverityModerate,
			wantErr:    false,
		},
		{
			name:       "below threshold passes",
			scenario:   baseScenario(types.SeverityLow),
			maxAllowed: types.SeveritySevere,
			wantErr:    false,
		},
		{
			name:       "above threshold fails",
			scenario:   baseScenario(types.SeverityCritical),
			maxAllowed: types.SeverityModerate,
			wantErr:    true,
		},
		{
			name:       "same severity as max passes",
			scenario:   baseScenario(types.SeveritySevere),
			maxAllowed: types.SeveritySevere,
			wantErr:    false,
		},
		{
			name:       "critical at critical max passes (boundary)",
			scenario:   baseScenario(types.SeverityCritical),
			maxAllowed: types.SeverityCritical,
			wantErr:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := safety.CheckSeverity(tt.scenario, tt.maxAllowed)
			if (err != nil) != tt.wantErr {
				t.Errorf("CheckSeverity() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr && err != nil {
				if err.Error() == "" {
					t.Error("CheckSeverity() error message should be descriptive")
				}
			}
		})
	}
}
