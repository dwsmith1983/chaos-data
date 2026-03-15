package mutation

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// SensorFlappingMutation rapidly alternates a sensor between two status values
// for a given number of iterations, simulating race conditions in stream processing.
type SensorFlappingMutation struct {
	store adapter.StateStore
}

// NewSensorFlappingMutation creates a SensorFlappingMutation with the given state store.
func NewSensorFlappingMutation(store adapter.StateStore) *SensorFlappingMutation {
	return &SensorFlappingMutation{store: store}
}

// Type returns "sensor-flapping".
func (s *SensorFlappingMutation) Type() string { return "sensor-flapping" }

// Apply writes the sensor with alternating status values for flap_count iterations.
// Params:
//   - "sensor_key" (required): sensor key identifier.
//   - "pipeline" (required): pipeline the sensor belongs to.
//   - "flap_count" (required): number of writes to perform, must be >= 2.
//   - "start_status" (optional): first status in the alternating sequence (default "ready").
//   - "alternate_status" (optional): second status in the alternating sequence (default "pending").
func (s *SensorFlappingMutation) Apply(ctx context.Context, obj types.DataObject, _ adapter.DataTransport, params map[string]string) (types.MutationRecord, error) {
	sensorKey, ok := params["sensor_key"]
	if !ok || sensorKey == "" {
		err := fmt.Errorf("sensor-flapping mutation: missing required param \"sensor_key\"")
		return types.MutationRecord{Applied: false, Mutation: "sensor-flapping", Error: err.Error()}, err
	}
	pipeline, ok := params["pipeline"]
	if !ok || pipeline == "" {
		err := fmt.Errorf("sensor-flapping mutation: missing required param \"pipeline\"")
		return types.MutationRecord{Applied: false, Mutation: "sensor-flapping", Error: err.Error()}, err
	}
	flapCountStr, ok := params["flap_count"]
	if !ok || flapCountStr == "" {
		err := fmt.Errorf("sensor-flapping mutation: missing required param \"flap_count\"")
		return types.MutationRecord{Applied: false, Mutation: "sensor-flapping", Error: err.Error()}, err
	}

	flapCount, parseErr := strconv.Atoi(flapCountStr)
	if parseErr != nil {
		err := fmt.Errorf("sensor-flapping mutation: invalid \"flap_count\" %q: must be an integer", flapCountStr)
		return types.MutationRecord{Applied: false, Mutation: "sensor-flapping", Error: err.Error()}, err
	}
	if flapCount < 2 {
		err := fmt.Errorf("sensor-flapping mutation: \"flap_count\" must be >= 2, got %d", flapCount)
		return types.MutationRecord{Applied: false, Mutation: "sensor-flapping", Error: err.Error()}, err
	}

	startStatus := types.SensorStatus("ready")
	if v, ok := params["start_status"]; ok && v != "" {
		startStatus = types.SensorStatus(v)
	}
	alternateStatus := types.SensorStatus("pending")
	if v, ok := params["alternate_status"]; ok && v != "" {
		alternateStatus = types.SensorStatus(v)
	}

	for i := 0; i < flapCount; i++ {
		status := startStatus
		if i%2 != 0 {
			status = alternateStatus
		}
		sensor := adapter.SensorData{
			Pipeline:    pipeline,
			Key:         sensorKey,
			Status:      status,
			LastUpdated: time.Now(),
		}
		if err := s.store.WriteSensor(ctx, pipeline, sensorKey, sensor); err != nil {
			err = fmt.Errorf("sensor-flapping mutation: write sensor failed on iteration %d with status %q: %w", i, status, err)
			return types.MutationRecord{Applied: false, Mutation: "sensor-flapping", Error: err.Error()}, err
		}
	}

	return types.MutationRecord{
		ObjectKey: obj.Key,
		Mutation:  "sensor-flapping",
		Params:    params,
		Applied:   true,
		Timestamp: time.Now(),
	}, nil
}
