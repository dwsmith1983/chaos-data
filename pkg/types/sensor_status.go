package types

// SensorStatus represents the operational state of a data sensor.
type SensorStatus string

// Known sensor statuses.
const (
	SensorStatusReady   SensorStatus = "ready"
	SensorStatusPending SensorStatus = "pending"
	SensorStatusStale   SensorStatus = "stale"
	SensorStatusUnknown SensorStatus = "unknown"
)

// IsValid returns true if the status is one of the known sensor statuses.
func (s SensorStatus) IsValid() bool {
	switch s {
	case SensorStatusReady, SensorStatusPending, SensorStatusStale, SensorStatusUnknown:
		return true
	default:
		return false
	}
}
