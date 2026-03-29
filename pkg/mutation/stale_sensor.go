package mutation

import (
	"context"
	"fmt"
	"time"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// StaleSensorMutation modifies a sensor's LastUpdated timestamp to simulate
// stale sensor data that hasn't been updated recently.
type StaleSensorMutation struct {
	store adapter.SensorStore
}

// NewStaleSensorMutation creates a StaleSensorMutation with the given sensor store.
func NewStaleSensorMutation(store adapter.SensorStore) *StaleSensorMutation {
	return &StaleSensorMutation{store: store}
}

// Type returns "stale-sensor".
func (s *StaleSensorMutation) Type() string { return "stale-sensor" }

// Apply reads the sensor, sets LastUpdated back by last_update_age, and writes it.
// Params:
//   - "sensor_key" (required): sensor key identifier.
//   - "pipeline" (required): pipeline the sensor belongs to.
//   - "last_update_age" (required): Go duration string for how old the sensor should appear.
func (s *StaleSensorMutation) Apply(ctx context.Context, obj types.DataObject, _ adapter.DataTransport, params map[string]string, clock adapter.Clock) (types.MutationRecord, error) {
	sensorKey, ok := params["sensor_key"]
	if !ok || sensorKey == "" {
		err := fmt.Errorf("stale-sensor mutation: missing required param \"sensor_key\"")
		return types.MutationRecord{Applied: false, Mutation: "stale-sensor", Error: err.Error()}, err
	}
	pipeline, ok := params["pipeline"]
	if !ok || pipeline == "" {
		err := fmt.Errorf("stale-sensor mutation: missing required param \"pipeline\"")
		return types.MutationRecord{Applied: false, Mutation: "stale-sensor", Error: err.Error()}, err
	}
	ageStr := params["last_update_age"]
	if ageStr == "" {
		ageStr = params["age"]
	}
	if ageStr == "" {
		err := fmt.Errorf("stale-sensor mutation: missing required param \"last_update_age\"")
		return types.MutationRecord{Applied: false, Mutation: "stale-sensor", Error: err.Error()}, err
	}

	age, err := time.ParseDuration(ageStr)
	if err != nil {
		err = fmt.Errorf("stale-sensor mutation: invalid last_update_age %q: %w", ageStr, err)
		return types.MutationRecord{Applied: false, Mutation: "stale-sensor", Error: err.Error()}, err
	}

	sensor, err := s.store.ReadSensor(ctx, pipeline, sensorKey)
	if err != nil {
		err = fmt.Errorf("stale-sensor mutation: read sensor failed: %w", err)
		return types.MutationRecord{Applied: false, Mutation: "stale-sensor", Error: err.Error()}, err
	}

	// Set LastUpdated back by the requested age.
	sensor.LastUpdated = clock.Now().Add(-age)

	if err := s.store.WriteSensor(ctx, pipeline, sensorKey, sensor); err != nil {
		err = fmt.Errorf("stale-sensor mutation: write sensor failed: %w", err)
		return types.MutationRecord{Applied: false, Mutation: "stale-sensor", Error: err.Error()}, err
	}

	return types.MutationRecord{
		ObjectKey: obj.Key,
		Mutation:  "stale-sensor",
		Params:    params,
		Applied:   true,
		Timestamp: clock.Now(),
	}, nil
}
