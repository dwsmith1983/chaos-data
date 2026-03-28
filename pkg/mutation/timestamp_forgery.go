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
//   - "sensor_key" (conditionally required): sensor key identifier. If absent but "pipeline"
//     and "field"+"offset" are present, defaults to "__schedule_meta".
//   - "pipeline" (required): pipeline the sensor belongs to.
//   - "last_updated_offset" (conditional): Go duration string to shift LastUpdated from now.
//   - "payload_timestamp_offset" (conditional): Go duration string stored as Metadata["payload_timestamp"].
//   - "field" (optional): metadata field name to store the computed timestamp in.
//   - "offset" (optional): Go duration string; computes time.Now().Add(offset) for the "field" value.
//
// At least one of last_updated_offset, payload_timestamp_offset, or field+offset must be present.
func (m *TimestampForgeryMutation) Apply(ctx context.Context, obj types.DataObject, _ adapter.DataTransport, params map[string]string) (types.MutationRecord, error) {
	pipeline, ok := params["pipeline"]
	if !ok || pipeline == "" {
		err := fmt.Errorf("timestamp-forgery mutation: missing required param \"pipeline\"")
		return types.MutationRecord{Applied: false, Mutation: "timestamp-forgery", Error: err.Error()}, err
	}

	fieldName := params["field"]
	offsetStr := params["offset"]

	// When field+offset are present without sensor_key, use a synthetic key.
	sensorKey := params["sensor_key"]
	if sensorKey == "" && fieldName != "" && offsetStr != "" {
		sensorKey = "__schedule_meta"
	}
	if sensorKey == "" {
		err := fmt.Errorf("timestamp-forgery mutation: missing required param \"sensor_key\"")
		return types.MutationRecord{Applied: false, Mutation: "timestamp-forgery", Error: err.Error()}, err
	}

	lastUpdatedOffsetStr := params["last_updated_offset"]
	payloadTimestampOffsetStr := params["payload_timestamp_offset"]

	if lastUpdatedOffsetStr == "" && payloadTimestampOffsetStr == "" && (fieldName == "" || offsetStr == "") {
		err := fmt.Errorf("timestamp-forgery mutation: at least one of \"last_updated_offset\", \"payload_timestamp_offset\", or \"field\"+\"offset\" must be provided")
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

	var fieldOffset time.Duration
	if fieldName != "" && offsetStr != "" {
		d, err := time.ParseDuration(offsetStr)
		if err != nil {
			err = fmt.Errorf("timestamp-forgery mutation: invalid offset %q: %w", offsetStr, err)
			return types.MutationRecord{Applied: false, Mutation: "timestamp-forgery", Error: err.Error()}, err
		}
		fieldOffset = d
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

	// Build a new metadata map when any metadata writes are needed,
	// copying existing entries to avoid mutating the shared map reference.
	needsMeta := payloadTimestampOffsetStr != "" || (fieldName != "" && offsetStr != "")
	if needsMeta {
		newMeta := make(map[string]string, len(sensor.Metadata)+2)
		for k, v := range sensor.Metadata {
			newMeta[k] = v
		}
		if payloadTimestampOffsetStr != "" {
			newMeta["payload_timestamp"] = time.Now().Add(payloadTimestampOffset).Format(time.RFC3339Nano)
		}
		if fieldName != "" && offsetStr != "" {
			newMeta[fieldName] = time.Now().Add(fieldOffset).Format(time.RFC3339Nano)
		}
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
