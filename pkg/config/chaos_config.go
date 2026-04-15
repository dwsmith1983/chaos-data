package config

import "math"

const (
	DefaultErrorRate    = 0.01
	DefaultLatencyRate  = 0.05
	DefaultMinLatencyMs = 10
	DefaultMaxLatencyMs = 500
)

// ChaosConfig holds configuration for chaos injection experiments.
type ChaosConfig struct {
	Enabled      bool    `json:"enabled" yaml:"enabled"`
	ErrorRate    float64 `json:"error_rate" yaml:"error_rate"`
	LatencyRate  float64 `json:"latency_rate" yaml:"latency_rate"`
	MinLatencyMs int     `json:"min_latency_ms" yaml:"min_latency_ms"`
	MaxLatencyMs int     `json:"max_latency_ms" yaml:"max_latency_ms"`
}

// Validate applies clamping logic and sets defaults for zero values if Enabled is true.
func (c *ChaosConfig) Validate() {
	if !c.Enabled {
		return
	}

	// Apply defaults for zero values
	if c.ErrorRate == 0 {
		c.ErrorRate = DefaultErrorRate
	}
	if c.LatencyRate == 0 {
		c.LatencyRate = DefaultLatencyRate
	}
	if c.MinLatencyMs == 0 {
		c.MinLatencyMs = DefaultMinLatencyMs
	}
	if c.MaxLatencyMs == 0 {
		c.MaxLatencyMs = DefaultMaxLatencyMs
	}

	// Clamping ErrorRate and LatencyRate [0, 1]
	c.ErrorRate = math.Max(0, math.Min(1, c.ErrorRate))
	c.LatencyRate = math.Max(0, math.Min(1, c.LatencyRate))

	// Ensure MinLatencyMs >= 0
	if c.MinLatencyMs < 0 {
		c.MinLatencyMs = 0
	}

	// Ensure MaxLatencyMs >= MinLatencyMs
	if c.MaxLatencyMs < c.MinLatencyMs {
		c.MaxLatencyMs = c.MinLatencyMs
	}
}
