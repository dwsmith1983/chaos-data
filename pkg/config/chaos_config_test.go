package config

import (
	"testing"
)

func TestChaosConfig_Validate(t *testing.T) {
	t.Run("zero-value defaults", func(t *testing.T) {
		c := ChaosConfig{Enabled: true}
		c.Validate()

		if c.ErrorRate != DefaultErrorRate {
			t.Errorf("expected DefaultErrorRate, got %f", c.ErrorRate)
		}
		if c.LatencyRate != DefaultLatencyRate {
			t.Errorf("expected DefaultLatencyRate, got %f", c.LatencyRate)
		}
		if c.MinLatencyMs != DefaultMinLatencyMs {
			t.Errorf("expected DefaultMinLatencyMs, got %d", c.MinLatencyMs)
		}
		if c.MaxLatencyMs != DefaultMaxLatencyMs {
			t.Errorf("expected DefaultMaxLatencyMs, got %d", c.MaxLatencyMs)
		}
	})

	t.Run("clamping out-of-range values", func(t *testing.T) {
		c := ChaosConfig{
			Enabled:      true,
			ErrorRate:    1.5,
			LatencyRate:  -0.5,
			MinLatencyMs: -100,
			MaxLatencyMs: 50,
		}
		c.Validate()

		if c.ErrorRate != 1.0 {
			t.Errorf("expected ErrorRate 1.0, got %f", c.ErrorRate)
		}
		if c.LatencyRate != 0.0 {
			t.Errorf("expected LatencyRate 0.0, got %f", c.LatencyRate)
		}
		if c.MinLatencyMs != 0 {
			t.Errorf("expected MinLatencyMs 0, got %d", c.MinLatencyMs)
		}
		if c.MaxLatencyMs != 50 {
			t.Errorf("expected MaxLatencyMs 50, got %d", c.MaxLatencyMs)
		}
	})

	t.Run("min-max latency consistency", func(t *testing.T) {
		c := ChaosConfig{
			Enabled:      true,
			MinLatencyMs: 200,
			MaxLatencyMs: 100,
		}
		c.Validate()

		if c.MaxLatencyMs < c.MinLatencyMs {
			t.Errorf("MaxLatencyMs (%d) should be >= MinLatencyMs (%d)", c.MaxLatencyMs, c.MinLatencyMs)
		}
		if c.MaxLatencyMs != 200 {
			t.Errorf("expected MaxLatencyMs to be clamped to MinLatencyMs (200), got %d", c.MaxLatencyMs)
		}
	})

	t.Run("passthrough valid values", func(t *testing.T) {
		c := ChaosConfig{
			Enabled:      true,
			ErrorRate:    0.2,
			LatencyRate:  0.3,
			MinLatencyMs: 50,
			MaxLatencyMs: 150,
		}
		c.Validate()

		if c.ErrorRate != 0.2 {
			t.Errorf("expected 0.2, got %f", c.ErrorRate)
		}
		if c.LatencyRate != 0.3 {
			t.Errorf("expected 0.3, got %f", c.LatencyRate)
		}
		if c.MinLatencyMs != 50 {
			t.Errorf("expected 50, got %d", c.MinLatencyMs)
		}
		if c.MaxLatencyMs != 150 {
			t.Errorf("expected 150, got %d", c.MaxLatencyMs)
		}
	})

	t.Run("no-op when disabled", func(t *testing.T) {
		c := ChaosConfig{
			Enabled:      false,
			ErrorRate:    0,
			LatencyRate:  0,
			MinLatencyMs: 0,
			MaxLatencyMs: 0,
		}
		c.Validate()

		if c.ErrorRate != 0 {
			t.Errorf("expected 0 when disabled, got %f", c.ErrorRate)
		}
	})
}

func FuzzChaosConfig_Validate(f *testing.F) {
	f.Add(true, 0.5, 0.5, 10, 100)
	f.Add(false, -1.0, 2.0, -50, 10)
	f.Fuzz(func(t *testing.T, enabled bool, errorRate float64, latencyRate float64, minLatency int, maxLatency int) {
		c := ChaosConfig{
			Enabled:      enabled,
			ErrorRate:    errorRate,
			LatencyRate:  latencyRate,
			MinLatencyMs: minLatency,
			MaxLatencyMs: maxLatency,
		}

		// Ensure Validate never panics
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("Validate panicked with: %v", r)
			}
		}()

		c.Validate()

		if enabled {
			if c.ErrorRate < 0 || c.ErrorRate > 1 {
				t.Errorf("ErrorRate out of bounds: %f", c.ErrorRate)
			}
			if c.LatencyRate < 0 || c.LatencyRate > 1 {
				t.Errorf("LatencyRate out of bounds: %f", c.LatencyRate)
			}
			if c.MinLatencyMs < 0 {
				t.Errorf("MinLatencyMs negative: %d", c.MinLatencyMs)
			}
			if c.MaxLatencyMs < c.MinLatencyMs {
				t.Errorf("MaxLatencyMs < MinLatencyMs: %d < %d", c.MaxLatencyMs, c.MinLatencyMs)
			}
		}
	})
}
