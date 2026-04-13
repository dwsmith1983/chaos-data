package config

import "time"

// ChaosConfig defines the configuration for chaos injection.
type ChaosConfig struct {
	// ErrorRate is the probability (0.0 to 1.0) of an operation failing.
	ErrorRate float64 `json:"error_rate"`
	// Latency is the base delay added to operations.
	Latency time.Duration `json:"latency"`
	// Jitter is the maximum random delay added to operations.
	Jitter time.Duration `json:"jitter"`
}

// Validate ensures the configuration values are within valid ranges and applies clamping.
// It modifies the receiver in-place to ensure it contains valid values.
func (c *ChaosConfig) Validate() {
	if c.ErrorRate < 0 {
		c.ErrorRate = 0
	} else if c.ErrorRate > 1.0 {
		c.ErrorRate = 1.0
	}

	if c.Latency < 0 {
		c.Latency = 0
	}

	if c.Jitter < 0 {
		c.Jitter = 0
	}
}
