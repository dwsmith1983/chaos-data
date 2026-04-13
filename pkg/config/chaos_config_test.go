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
		if c.MaxLatencyMs != DefaultMaxLatencyMs {
			t.Errorf("expected DefaultMaxLatencyMs, got %d", c.MaxLatencyMs)
		}
	})

	t.Run("out-of-range clamping", func(t *testing.T) {
		c := ChaosConfig{
			Enabled:        true,
			ErrorRate:      2.0,
			LatencyRate:    -0.5,
			CorruptionRate: 10.0,
			MinLatencyMs:   -100,
			MaxLatencyMs:   50,
		}
		c.Validate()

		if c.ErrorRate != 1.0 {
			t.Errorf("expected ErrorRate 1.0, got %f", c.ErrorRate)
		}
		if c.LatencyRate != 0.0 {
			t.Errorf("expected LatencyRate 0.0, got %f", c.LatencyRate)
		}
		if c.CorruptionRate != 1.0 {
			t.Errorf("expected CorruptionRate 1.0, got %f", c.CorruptionRate)
		}
		if c.MinLatencyMs != 0 {
			t.Errorf("expected MinLatencyMs 0, got %d", c.MinLatencyMs)
		}
		if c.MaxLatencyMs < c.MinLatencyMs {
			t.Errorf("expected MaxLatencyMs >= MinLatencyMs, got %d < %d", c.MaxLatencyMs, c.MinLatencyMs)
		}
	})

	t.Run("max latency less than min latency", func(t *testing.T) {
		c := ChaosConfig{
			Enabled:      true,
			MinLatencyMs: 200,
			MaxLatencyMs: 100,
		}
		c.Validate()
		if c.MaxLatencyMs != 200 {
			t.Errorf("expected MaxLatencyMs to be clamped to MinLatencyMs (200), got %d", c.MaxLatencyMs)
		}
	})

	t.Run("valid passthrough", func(t *testing.T) {
		c := ChaosConfig{
			Enabled:        true,
			ErrorRate:      0.25,
			LatencyRate:    0.33,
			CorruptionRate: 0.1,
			MinLatencyMs:   100,
			MaxLatencyMs:   200,
		}
		c.Validate()

		if c.ErrorRate != 0.25 {
			t.Errorf("expected ErrorRate 0.25, got %f", c.ErrorRate)
		}
		if c.LatencyRate != 0.33 {
			t.Errorf("expected LatencyRate 0.33, got %f", c.LatencyRate)
		}
		if c.MinLatencyMs != 100 {
			t.Errorf("expected MinLatencyMs 100, got %d", c.MinLatencyMs)
		}
		if c.MaxLatencyMs != 200 {
			t.Errorf("expected MaxLatencyMs 200, got %d", c.MaxLatencyMs)
		}
	})

	t.Run("disabled skip validation", func(t *testing.T) {
		c := ChaosConfig{
			Enabled:   false,
			ErrorRate: 2.0,
		}
		c.Validate()
		if c.ErrorRate != 2.0 {
			t.Errorf("Validate should do nothing if Enabled is false, but ErrorRate changed to %f", c.ErrorRate)
		}
	})
}

func FuzzChaosConfig_Validate(f *testing.F) {
	f.Add(true, 0.5, 0.5, 0.5, 100, 200)
	f.Add(false, -1.0, 2.0, 1.5, -100, 50)
	f.Add(true, 0.0, 0.0, 0.0, 0, 0)

	f.Fuzz(func(t *testing.T, enabled bool, errRate, latRate, corrRate float64, minLat, maxLat int) {
		c := ChaosConfig{
			Enabled:        enabled,
			ErrorRate:      errRate,
			LatencyRate:    latRate,
			CorruptionRate: corrRate,
			MinLatencyMs:   minLat,
			MaxLatencyMs:   maxLat,
		}

		// This should never panic
		c.Validate()

		if enabled {
			// Basic assertions for clamping
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
