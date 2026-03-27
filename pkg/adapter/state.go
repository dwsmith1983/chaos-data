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

// JobEvent represents a job execution event.
type JobEvent struct {
	Pipeline  string    `json:"pipeline"`
	Schedule  string    `json:"schedule"`
	Date      string    `json:"date"`
	Event     string    `json:"event"`
	RunID     string    `json:"run_id"`
	Timestamp time.Time `json:"timestamp"`
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
	WritePipelineConfig(ctx context.Context, pipeline string, config []byte) error
	ReadPipelineConfig(ctx context.Context, pipeline string) ([]byte, error)
	DeleteByPrefix(ctx context.Context, prefix string) error
	CountReruns(ctx context.Context, pipeline, schedule, date string) (int, error)
	WriteRerun(ctx context.Context, pipeline, schedule, date, reason string) error
	ReadJobEvents(ctx context.Context, pipeline, schedule, date string) ([]JobEvent, error)
}
