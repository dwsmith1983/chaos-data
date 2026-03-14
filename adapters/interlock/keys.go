package interlock

import chaosaws "github.com/dwsmith1983/chaos-data/adapters/aws"

// SensorPK returns the partition key for a sensor record.
// It delegates to the aws package key helper.
func SensorPK(pipeline string) string {
	return chaosaws.SensorPK(pipeline)
}

// SensorSK returns the sort key for a sensor record.
// It delegates to the aws package key helper.
func SensorSK(objectKey string) string {
	return chaosaws.SensorSK(objectKey)
}

// TriggerPK returns the partition key for a trigger record.
// It delegates to the aws package key helper.
func TriggerPK(pipeline string) string {
	return chaosaws.TriggerPK(pipeline)
}

// TriggerSK returns the sort key for a trigger record.
// It delegates to the aws package key helper.
func TriggerSK(schedule, date string) string {
	return chaosaws.TriggerSK(schedule, date)
}
