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
