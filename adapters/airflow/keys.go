package airflow

import (
	"fmt"
	"strings"
	"time"
)

// Key prefix for all chaos-data Airflow Variables.
const keyPrefix = "chaos:"

// SensorKey returns the Airflow Variable key for a sensor record.
func SensorKey(pipeline, objectKey string) string {
	return fmt.Sprintf("%ssensor:%s:%s", keyPrefix, pipeline, objectKey)
}

// TriggerStatusKey returns the Airflow Variable key for a trigger status record.
// Named TriggerStatusKey (not TriggerKey) to avoid collision with adapter.TriggerKey type.
func TriggerStatusKey(pipeline, schedule, date string) string {
	return fmt.Sprintf("%strigger:%s:%s:%s", keyPrefix, pipeline, schedule, date)
}

// EventKey returns the Airflow Variable key for a chaos event record.
func EventKey(experimentID string, ts time.Time, eventID string) string {
	return fmt.Sprintf("%sevent:%s:%s:%s", keyPrefix, experimentID, ts.Format(time.RFC3339Nano), eventID)
}

// EventKeyPrefix returns the key prefix for listing chaos events by experiment.
func EventKeyPrefix(experimentID string) string {
	return fmt.Sprintf("%sevent:%s:", keyPrefix, experimentID)
}

// ConfigKey returns the Airflow Variable key for a pipeline config record.
func ConfigKey(pipeline string) string {
	return fmt.Sprintf("%sconfig:%s", keyPrefix, pipeline)
}

// RerunKey returns the Airflow Variable key for a rerun record.
func RerunKey(pipeline, schedule, date string, ts time.Time) string {
	return fmt.Sprintf("%srerun:%s:%s:%s:%s", keyPrefix, pipeline, schedule, date, ts.Format(time.RFC3339Nano))
}

// RerunKeyPrefix returns the key prefix for listing reruns by pipeline/schedule/date.
func RerunKeyPrefix(pipeline, schedule, date string) string {
	return fmt.Sprintf("%srerun:%s:%s:%s:", keyPrefix, pipeline, schedule, date)
}

// JobEventKey returns the Airflow Variable key for a job event record.
func JobEventKey(pipeline, schedule, date string, ts time.Time, runID string) string {
	return fmt.Sprintf("%sjob:%s:%s:%s:%s:%s", keyPrefix, pipeline, schedule, date, ts.Format(time.RFC3339Nano), runID)
}

// JobEventKeyPrefix returns the key prefix for listing job events by pipeline/schedule/date.
func JobEventKeyPrefix(pipeline, schedule, date string) string {
	return fmt.Sprintf("%sjob:%s:%s:%s:", keyPrefix, pipeline, schedule, date)
}

// HasPrefix reports whether key starts with the given prefix.
func HasPrefix(key, prefix string) bool {
	return strings.HasPrefix(key, prefix)
}
