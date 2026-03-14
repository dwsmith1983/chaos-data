package safety_test

import (
	"testing"
	"time"

	"github.com/dwsmith1983/chaos-data/pkg/safety"
)

func TestCheckTimeBox(t *testing.T) {
	now := time.Now()

	tests := []struct {
		name        string
		startTime   time.Time
		maxDuration time.Duration
		wantErr     bool
	}{
		{
			name:        "within duration passes",
			startTime:   now.Add(-5 * time.Minute),
			maxDuration: 10 * time.Minute,
			wantErr:     false,
		},
		{
			name:        "past duration fails",
			startTime:   now.Add(-15 * time.Minute),
			maxDuration: 10 * time.Minute,
			wantErr:     true,
		},
		{
			name:        "zero duration rejected",
			startTime:   now,
			maxDuration: 0,
			wantErr:     true,
		},
		{
			name:        "negative duration rejected",
			startTime:   now,
			maxDuration: -5 * time.Minute,
			wantErr:     true,
		},
		{
			name:        "elapsed equals max duration passes (boundary)",
			startTime:   now.Add(-10 * time.Minute),
			maxDuration: 10*time.Minute + time.Second,
			wantErr:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := safety.CheckTimeBox(tt.startTime, tt.maxDuration)
			if (err != nil) != tt.wantErr {
				t.Errorf("CheckTimeBox() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
