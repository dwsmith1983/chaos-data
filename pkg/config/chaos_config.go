package config

import "math"

// ChaosConfig holds configuration for chaos injection experiments.
type ChaosConfig struct {
	Enabled        bool    `json:"enabled" yaml:"enabled"`
	ErrorRate      float64 `json:"error_rate" yaml:"error_rate"`
	LatencyRate    float64 `json:"latency_rate" yaml:"latency_rate"`
	CorruptionRate float64 `json:"corruption_rate" yaml:"corruption_rate"`
	MinLatencyMs   int     `json:"min_latency_ms" yaml:"min_latency_ms"`
	MaxLatencyMs   int     `json:"max_latency_ms" yaml:"max_latency_ms"`
}

const (
	DefaultErrorRate      = 0.05
	DefaultLatencyRate    = 0.10
	DefaultCorruptionRate = 0.05
	DefaultMaxLatencyMs   = 500
)

// Validate ensures the configuration is within sane boundaries and applies defaults.
func (c *ChaosConfig) Validate() {
	if !c.Enabled {
		return
	}

	// Apply defaults for zero values if Enabled
	if c.ErrorRate == 0 {
		c.ErrorRate = DefaultErrorRate
	}
	if c.LatencyRate == 0 {
		c.LatencyRate = DefaultLatencyRate
	}
	if c.CorruptionRate == 0 {
		c.CorruptionRate = DefaultCorruptionRate
	}
	if c.MaxLatencyMs == 0 && c.MinLatencyMs == 0 {
		c.MaxLatencyMs = DefaultMaxLatencyMs
	}

	// Clamp rates to [0.0, 1.0]
	c.ErrorRate = clamp(c.ErrorRate, 0.0, 1.0)
	c.LatencyRate = clamp(c.LatencyRate, 0.0, 1.0)
	c.CorruptionRate = clamp(c.CorruptionRate, 0.0, 1.0)

	// Ensure MinLatencyMs is non-negative
	if c.MinLatencyMs < 0 {
		c.MinLatencyMs = 0
	}

	// Ensure MaxLatencyMs >= MinLatencyMs
	if c.MaxLatencyMs < c.MinLatencyMs {
		c.MaxLatencyMs = c.MinLatencyMs
	}
}

func clamp(v, min, max float64) float64 {
	if math.IsNaN(v) {
		return min
	}
	if v < min {
		return min
	}
	if v > max {
		return max
	}
	return v
}
