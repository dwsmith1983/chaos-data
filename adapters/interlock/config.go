package interlock

import "errors"

// Config holds Interlock adapter configuration.
type Config struct {
	// SensorTableName is the table for sensor state. Required.
	SensorTableName string

	// TriggerTableName is the table for trigger state. Required.
	TriggerTableName string

	// EventBusName is the event bus name. Defaults to "interlock".
	EventBusName string

	// PipelinePrefix is prepended to pipeline names in mutations.
	PipelinePrefix string

	// DefaultSchedule is used when a trigger mutation has no "schedule" param.
	DefaultSchedule string

	// DefaultDate is used when a trigger mutation has no "date" param.
	DefaultDate string

	// SLAWindowMinutes is the SLA window in minutes. Defaults to 30.
	SLAWindowMinutes int
}

// Validate checks that all required fields are set. It returns an error
// describing the first missing field found.
func (c *Config) Validate() error {
	if c.SensorTableName == "" {
		return errors.New("interlock: SensorTableName is required")
	}
	if c.TriggerTableName == "" {
		return errors.New("interlock: TriggerTableName is required")
	}
	return nil
}

// Defaults fills in default values for optional fields that are empty.
// Call Defaults before Validate to ensure optional fields have sensible
// values.
func (c *Config) Defaults() {
	if c.EventBusName == "" {
		c.EventBusName = "interlock"
	}
	if c.SLAWindowMinutes == 0 {
		c.SLAWindowMinutes = 30
	}
}
