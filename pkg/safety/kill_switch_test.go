package safety_test

import (
	"context"
	"errors"
	"testing"

	"github.com/dwsmith1983/chaos-data/internal/testutil"
	"github.com/dwsmith1983/chaos-data/pkg/safety"
)

func TestCheckKillSwitch(t *testing.T) {
	tests := []struct {
		name       string
		controller *testutil.MockSafety
		wantErr    bool
	}{
		{
			name: "enabled returns nil",
			controller: &testutil.MockSafety{
				Enabled: true,
			},
			wantErr: false,
		},
		{
			name: "disabled returns error",
			controller: &testutil.MockSafety{
				Enabled: false,
			},
			wantErr: true,
		},
		{
			name: "controller error returns error (fail-safe)",
			controller: &testutil.MockSafety{
				Enabled:    false,
				EnabledErr: errors.New("connection refused"),
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
