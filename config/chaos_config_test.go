package config

import (
	"testing"
	"time"
)

func TestChaosConfig_Validate(t *testing.T) {
	tests := []struct {
		name     string
		input    ChaosConfig
		expected ChaosConfig
	}{
		{
			name:     "zero values remain zero (safe defaults)",
			input:    ChaosConfig{},
			expected: ChaosConfig{},
		},
		{
			name: "valid values passthrough",
			input: ChaosConfig{
				ErrorRate: 0.5,
				Latency:   100 * time.Millisecond,
				Jitter:    50 * time.Millisecond,
			},
			expected: ChaosConfig{
				ErrorRate: 0.5,
				Latency:   100 * time.Millisecond,
				Jitter:    50 * time.Millisecond,
			},
		},
		{
			name: "error rate clamped low",
			input: ChaosConfig{
				ErrorRate: -0.1,
			},
			expected: ChaosConfig{
				ErrorRate: 0.0,
			},
		},
		{
			name: "error rate clamped high",
			input: ChaosConfig{
				ErrorRate: 1.1,
			},
			expected: ChaosConfig{
				ErrorRate: 1.0,
			},
		},
		{
			name: "latency clamped low",
			input: ChaosConfig{
				Latency: -1 * time.Second,
			},
			expected: ChaosConfig{
				Latency: 0,
			},
		},
		{
			name: "jitter clamped low",
			input: ChaosConfig{
				Jitter: -1 * time.Second,
			},
			expected: ChaosConfig{
				Jitter: 0,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := tt.input
			c.Validate()
			if c.ErrorRate != tt.expected.ErrorRate {
				t.Errorf("%s: ErrorRate = %v, want %v", tt.name, c.ErrorRate, tt.expected.ErrorRate)
			}
			if c.Latency != tt.expected.Latency {
				t.Errorf("%s: Latency = %v, want %v", tt.name, c.Latency, tt.expected.Latency)
			}
			if c.Jitter != tt.expected.Jitter {
				t.Errorf("%s: Jitter = %v, want %v", tt.name, c.Jitter, tt.expected.Jitter)
			}
		})
	}
}

func FuzzChaosConfig_Validate(f *testing.F) {
	f.Add(0.5, int64(time.Second), int64(time.Millisecond))
	f.Fuzz(func(t *testing.T, errorRate float64, latency int64, jitter int64) {
		c := ChaosConfig{
			ErrorRate: errorRate,
			Latency:   time.Duration(latency),
			Jitter:    time.Duration(jitter),
		}
		// Validate should never panic
		c.Validate()

		// Assert clamping logic holds
		if c.ErrorRate < 0 || c.ErrorRate > 1.0 {
			t.Errorf("ErrorRate out of bounds: %v", c.ErrorRate)
		}
		if c.Latency < 0 {
			t.Errorf("Latency out of bounds: %v", c.Latency)
		}
		if c.Jitter < 0 {
			t.Errorf("Jitter out of bounds: %v", c.Jitter)
		}
	})
}
