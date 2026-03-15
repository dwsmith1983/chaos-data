package adapter

import (
	"context"
	"time"

	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// SensorData represents the state of a data sensor.
type SensorData struct {
	Pipeline    string            `json:"pipeline"`
	Key         string            `json:"key"`
	Status      types.SensorStatus `json:"status"`
	LastUpdated time.Time         `json:"last_updated"`
	Metadata    map[string]string `json:"metadata,omitempty"`
}

// TriggerKey identifies a specific trigger evaluation by pipeline, schedule,
// and date.
type TriggerKey struct {
	Pipeline string `json:"pipeline"`
	Schedule string `json:"schedule"`
	Date     string `json:"date"`
}

// StateStore reads/writes control plane state.
type StateStore interface {
	ReadSensor(ctx context.Context, pipeline, key string) (SensorData, error)
	WriteSensor(ctx context.Context, pipeline, key string, data SensorData) error
	DeleteSensor(ctx context.Context, pipeline, key string) error
	ReadTriggerStatus(ctx context.Context, key TriggerKey) (string, error)
	WriteTriggerStatus(ctx context.Context, key TriggerKey, status string) error
	WriteEvent(ctx context.Context, event types.ChaosEvent) error
	ReadChaosEvents(ctx context.Context, experimentID string) ([]types.ChaosEvent, error)
}
