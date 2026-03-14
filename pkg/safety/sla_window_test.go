package safety_test

import (
	"context"
	"errors"
	"testing"

	"github.com/dwsmith1983/chaos-data/pkg/safety"
)

func TestCheckSLAWindow(t *testing.T) {
	tests := []struct {
		name       string
		pipeline   string
		controller *mockSafetyController
		wantErr    bool
	}{
		{
			name:     "outside window (controller returns true) passes",
			pipeline: "etl-daily",
			controller: &mockSafetyController{
				slaWindowSafe: true,
			},
			wantErr: false,
		},
		{
			name:     "inside window (controller returns false) fails",
			pipeline: "etl-daily",
			controller: &mockSafetyController{
				slaWindowSafe: false,
			},
			wantErr: true,
		},
		{
			name:     "controller error returns error (fail-safe)",
			pipeline: "etl-daily",
			controller: &mockSafetyController{
				slaWindowSafe: false,
				slaWindowErr:  errors.New("timeout"),
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := safety.CheckSLAWindow(context.Background(), tt.pipeline, tt.controller)
			if (err != nil) != tt.wantErr {
				t.Errorf("CheckSLAWindow() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
