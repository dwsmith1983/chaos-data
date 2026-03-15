package mutation

import (
	"context"
	"fmt"
	"time"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// TimestampForgeryMutation injects sensors where DynamoDB LastUpdated disagrees
// with an embedded payload timestamp, testing clock-skew validation.
type TimestampForgeryMutation struct {
	store adapter.StateStore
}

// NewTimestampForgeryMutation creates a TimestampForgeryMutation with the given state store.
func NewTimestampForgeryMutation(store adapter.StateStore) *TimestampForgeryMutation {
	return &TimestampForgeryMutation{store: store}
}

// Type returns "timestamp-forgery".
func (m *TimestampForgeryMutation) Type() string { return "timestamp-forgery" }

// Apply reads the sensor (creating a new one if absent), shifts LastUpdated and/or
// sets Metadata["payload_timestamp"] according to the provided offset params, then writes.
// Params:
//   - "sensor_key" (required): sensor key identifier.
//   - "pipeline" (required): pipeline the sensor belongs to.
//   - "last_updated_offset" (conditional): Go duration string to shift LastUpdated from now.
//   - "payload_timestamp_offset" (conditional): Go duration string stored as Metadata["payload_timestamp"].
//
// At least one of last_updated_offset or payload_timestamp_offset must be present.
func (m *TimestampForgeryMutation) Apply(ctx context.Context, obj types.DataObject, _ adapter.DataTransport, params map[string]string) (types.MutationRecord, error) {
	sensorKey, ok := params["sensor_key"]
	if !ok || sensorKey == "" {
		err := fmt.Errorf("timestamp-forgery mutation: missing required param \"sensor_key\"")
		return types.MutationRecord{Applied: false, Mutation: "timestamp-forgery", Error: err.Error()}, err
	}
	pipeline, ok := params["pipeline"]
	if !ok || pipeline == "" {
		err := fmt.Errorf("timestamp-forgery mutation: missing required param \"pipeline\"")
		return types.MutationRecord{Applied: false, Mutation: "timestamp-forgery", Error: err.Error()}, err
	}

	lastUpdatedOffsetStr := params["last_updated_offset"]
	payloadTimestampOffsetStr := params["payload_timestamp_offset"]

	if lastUpdatedOffsetStr == "" && payloadTimestampOffsetStr == "" {
		err := fmt.Errorf("timestamp-forgery mutation: at least one of \"last_updated_offset\" or \"payload_timestamp_offset\" must be provided")
		return types.MutationRecord{Applied: false, Mutation: "timestamp-forgery", Error: err.Error()}, err
	}

	var lastUpdatedOffset time.Duration
	if lastUpdatedOffsetStr != "" {
		d, err := time.ParseDuration(lastUpdatedOffsetStr)
		if err != nil {
			err = fmt.Errorf("timestamp-forgery mutation: invalid last_updated_offset %q: %w", lastUpdatedOffsetStr, err)
			return types.MutationRecord{Applied: false, Mutation: "timestamp-forgery", Error: err.Error()}, err
		}
		lastUpdatedOffset = d
	}

	var payloadTimestampOffset time.Duration
	if payloadTimestampOffsetStr != "" {
		d, err := time.ParseDuration(payloadTimestampOffsetStr)
		if err != nil {
			err = fmt.Errorf("timestamp-forgery mutation: invalid payload_timestamp_offset %q: %w", payloadTimestampOffsetStr, err)
			return types.MutationRecord{Applied: false, Mutation: "timestamp-forgery", Error: err.Error()}, err
		}
		payloadTimestampOffset = d
	}

	sensor, err := m.store.ReadSensor(ctx, pipeline, sensorKey)
	if err != nil {
		err = fmt.Errorf("timestamp-forgery mutation: read sensor failed: %w", err)
		return types.MutationRecord{Applied: false, Mutation: "timestamp-forgery", Error: err.Error()}, err
	}

	// Zero-value Key means the sensor was not found (matches SQLiteState not-found behavior).
	if sensor.Key == "" {
		sensor = adapter.SensorData{
			Pipeline:    pipeline,
			Key:         sensorKey,
			Status:      "ready",
			LastUpdated: time.Now(),
		}
	}

	if lastUpdatedOffsetStr != "" {
		sensor.LastUpdated = time.Now().Add(lastUpdatedOffset)
	}

	if payloadTimestampOffsetStr != "" {
		// Copy the existing metadata map to avoid mutating the shared map reference.
		newMeta := make(map[string]string, len(sensor.Metadata)+1)
		for k, v := range sensor.Metadata {
			newMeta[k] = v
		}
		newMeta["payload_timestamp"] = time.Now().Add(payloadTimestampOffset).Format(time.RFC3339Nano)
		sensor.Metadata = newMeta
	}

	if err := m.store.WriteSensor(ctx, pipeline, sensorKey, sensor); err != nil {
		err = fmt.Errorf("timestamp-forgery mutation: write sensor failed: %w", err)
		return types.MutationRecord{Applied: false, Mutation: "timestamp-forgery", Error: err.Error()}, err
	}

	return types.MutationRecord{
		ObjectKey: obj.Key,
		Mutation:  "timestamp-forgery",
		Params:    params,
		Applied:   true,
		Timestamp: time.Now(),
	}, nil
}
