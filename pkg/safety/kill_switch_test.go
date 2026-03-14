package safety_test

import (
	"context"
	"errors"
	"testing"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/safety"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// Compile-time interface assertion.
var _ adapter.SafetyController = (*mockSafetyController)(nil)

// mockSafetyController implements adapter.SafetyController for testing.
type mockSafetyController struct {
	isEnabled    bool
	isEnabledErr error

	blastRadiusErr error

	slaWindowSafe bool
	slaWindowErr  error
}

func (m *mockSafetyController) IsEnabled(_ context.Context) (bool, error) {
	return m.isEnabled, m.isEnabledErr
}

func (m *mockSafetyController) MaxSeverity(_ context.Context) (types.Severity, error) {
	return 0, nil
}

func (m *mockSafetyController) CheckBlastRadius(_ context.Context, _ types.ExperimentStats) error {
	return m.blastRadiusErr
}

func (m *mockSafetyController) CheckSLAWindow(_ context.Context, _ string) (bool, error) {
	return m.slaWindowSafe, m.slaWindowErr
}

func TestCheckKillSwitch(t *testing.T) {
	tests := []struct {
		name       string
		controller *mockSafetyController
		wantErr    bool
	}{
		{
			name: "enabled returns nil",
			controller: &mockSafetyController{
				isEnabled: true,
			},
			wantErr: false,
		},
		{
			name: "disabled returns error",
			controller: &mockSafetyController{
				isEnabled: false,
			},
			wantErr: true,
		},
		{
			name: "controller error returns error (fail-safe)",
			controller: &mockSafetyController{
				isEnabled:    false,
				isEnabledErr: errors.New("connection refused"),
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := safety.CheckKillSwitch(context.Background(), tt.controller)
			if (err != nil) != tt.wantErr {
				t.Errorf("CheckKillSwitch() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
