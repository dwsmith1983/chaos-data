package aws

import (
	"fmt"
	"time"
)

// SensorPK returns the partition key for a sensor record.
func SensorPK(pipeline string) string {
	return "SENSOR#" + pipeline
}

// SensorSK returns the sort key for a sensor record.
func SensorSK(objectKey string) string {
	return "KEY#" + objectKey
}

// TriggerPK returns the partition key for a trigger record.
func TriggerPK(pipeline string) string {
	return "TRIGGER#" + pipeline
}

// TriggerSK returns the sort key for a trigger record.
func TriggerSK(schedule, date string) string {
	return fmt.Sprintf("SCHED#%s#DATE#%s", schedule, date)
}

// ChaosPK returns the partition key for a chaos event record.
func ChaosPK(experimentID string) string {
	return "CHAOS#" + experimentID
}

// ChaosSK returns the sort key for a chaos event record.
func ChaosSK(ts time.Time, eventID string) string {
	return "EVENT#" + ts.Format(time.RFC3339Nano) + "#" + eventID
}

// ControlPK returns the partition key for a control record.
func ControlPK(name string) string {
	return "CONTROL#" + name
}

// CooldownPK returns the partition key for a cooldown record.
func CooldownPK() string {
	return "CONTROL#cooldown"
}

// CooldownSK returns the sort key for a cooldown record.
func CooldownSK(scenario string) string {
	return scenario
}

// ConfigPK returns the partition key for a pipeline config record.
func ConfigPK(pipeline string) string {
	return "CONFIG#" + pipeline
}

// ConfigSK returns the sort key for a pipeline config record.
func ConfigSK() string {
	return "CONFIG"
}

// RerunPK returns the partition key for a rerun record.
func RerunPK(pipeline string) string {
	return "RERUN#" + pipeline
}

// RerunSK returns the sort key for a rerun record.
func RerunSK(schedule, date, timestamp string) string {
	return fmt.Sprintf("SCHED#%s#DATE#%s#TS#%s", schedule, date, timestamp)
}

// RerunSKPrefix returns the sort key prefix for querying reruns by schedule and date.
func RerunSKPrefix(schedule, date string) string {
	return fmt.Sprintf("SCHED#%s#DATE#%s#", schedule, date)
}

// JobEventPK returns the partition key for a job event record.
func JobEventPK(pipeline string) string {
	return "JOBEVENT#" + pipeline
}

// JobEventSK returns the sort key for a job event record.
func JobEventSK(schedule, date, timestamp, runID string) string {
	return fmt.Sprintf("SCHED#%s#DATE#%s#TS#%s#%s", schedule, date, timestamp, runID)
}

// JobEventSKPrefix returns the sort key prefix for querying job events by schedule and date.
func JobEventSKPrefix(schedule, date string) string {
	return fmt.Sprintf("SCHED#%s#DATE#%s#", schedule, date)
}
