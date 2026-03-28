package types

// SensorStatus represents the operational state of a data sensor.
type SensorStatus string

// Known sensor statuses.
const (
	SensorStatusReady    SensorStatus = "ready"
	SensorStatusPending  SensorStatus = "pending"
	SensorStatusStale    SensorStatus = "stale"
	SensorStatusUnknown  SensorStatus = "unknown"
	SensorStatusComplete SensorStatus = "COMPLETE"
)

// IsValid returns true if the status is one of the known sensor statuses.
func (s SensorStatus) IsValid() bool {
	switch s {
	case SensorStatusReady, SensorStatusPending, SensorStatusStale, SensorStatusUnknown, SensorStatusComplete:
		return true
	default:
		return false
	}
}
