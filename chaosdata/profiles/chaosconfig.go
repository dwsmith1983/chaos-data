package profiles

import (
	"encoding/json"
	"time"
)

// MaxRecordCount is the maximum number of records that can be generated.
const MaxRecordCount = 100_000

// ChaosConfig represents the configuration for chaos generation.
type ChaosConfig struct {
	// Structural
	SchemaDeviation float64 `json:"schema_deviation"` // 0-1: probability of missing/extra fields
	DuplicateRate   float64 `json:"duplicate_rate"`   // 0-1

	// Temporal
	OutOfOrderRate  float64       `json:"out_of_order_rate"` // 0-1
	TimestampJitter time.Duration `json:"timestamp_jitter"`
	ReplayDelay     time.Duration `json:"replay_delay"`

	// Semantic
	ErrorRate      float64 `json:"error_rate"`      // 0-1: domain-specific error injection
	CorruptionRate float64 `json:"corruption_rate"` // 0-1

	// Volume
	BurstSize     int           `json:"burst_size"`
	BurstInterval time.Duration `json:"burst_interval"`

	// Control
	Seed         int64  `json:"seed"`          // 0 = crypto/rand
	OutputFormat string `json:"output_format"` // "json" or "csv"
}

// DefaultChaosConfig returns the default configuration.
func DefaultChaosConfig() ChaosConfig {
	return ChaosConfig{
		ErrorRate:    0.1,
		Seed:         0,
		OutputFormat: "json",
		BurstSize:    10,
	}
}

// clampFloat64 ensures a value is between 0 and 1.
func clampFloat64(val float64) float64 {
	if val < 0.0 {
		return 0.0
	}
	if val > 1.0 {
		return 1.0
	}
	return val
}

// clampDuration ensures a duration is not negative.
func clampDuration(val time.Duration) time.Duration {
	if val < 0 {
		return 0
	}
	return val
}

// Validate sanitizes the configuration.
func (c ChaosConfig) Validate() ChaosConfig {
	c.SchemaDeviation = clampFloat64(c.SchemaDeviation)
	c.DuplicateRate = clampFloat64(c.DuplicateRate)
	c.OutOfOrderRate = clampFloat64(c.OutOfOrderRate)
	c.ErrorRate = clampFloat64(c.ErrorRate)
	c.CorruptionRate = clampFloat64(c.CorruptionRate)

	c.TimestampJitter = clampDuration(c.TimestampJitter)
	c.ReplayDelay = clampDuration(c.ReplayDelay)
	c.BurstInterval = clampDuration(c.BurstInterval)

	if c.OutputFormat == "" {
		c.OutputFormat = "json"
	}
	if c.BurstSize < 0 {
		c.BurstSize = 0
	}

	return c
}

// ParseConfig parses the configuration from a map of tags.
func ParseConfig(tags map[string]string) (ChaosConfig, error) {
	val, ok := tags["chaos_config"]
	if !ok {
		return DefaultChaosConfig().Validate(), nil
	}

	var cfg ChaosConfig
	if err := json.Unmarshal([]byte(val), &cfg); err != nil {
		return ChaosConfig{}, err
	}

	return cfg.Validate(), nil
}
