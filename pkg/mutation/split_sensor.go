package mutation

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// SplitSensorMutation writes multiple conflicting sensor values for the same
// key in rapid succession, simulating a split-brain or race condition.
type SplitSensorMutation struct {
	store adapter.StateStore
}

// NewSplitSensorMutation creates a SplitSensorMutation with the given state store.
func NewSplitSensorMutation(store adapter.StateStore) *SplitSensorMutation {
	return &SplitSensorMutation{store: store}
}

// Type returns "split-sensor".
func (s *SplitSensorMutation) Type() string { return "split-sensor" }

// Apply writes the sensor with each conflicting status value in sequence.
// Params:
//   - "sensor_key" (required): sensor key identifier.
//   - "pipeline" (required): pipeline the sensor belongs to.
//   - "conflicting_values" (required): comma-separated status values (e.g., "ready,pending,ready").
func (s *SplitSensorMutation) Apply(ctx context.Context, obj types.DataObject, _ adapter.DataTransport, params map[string]string) (types.MutationRecord, error) {
	sensorKey, ok := params["sensor_key"]
	if !ok || sensorKey == "" {
		err := fmt.Errorf("split-sensor mutation: missing required param \"sensor_key\"")
		return types.MutationRecord{Applied: false, Mutation: "split-sensor", Error: err.Error()}, err
	}
	pipeline, ok := params["pipeline"]
	if !ok || pipeline == "" {
		err := fmt.Errorf("split-sensor mutation: missing required param \"pipeline\"")
		return types.MutationRecord{Applied: false, Mutation: "split-sensor", Error: err.Error()}, err
	}
	valuesStr, ok := params["conflicting_values"]
	if !ok || valuesStr == "" {
		err := fmt.Errorf("split-sensor mutation: missing required param \"conflicting_values\"")
		return types.MutationRecord{Applied: false, Mutation: "split-sensor", Error: err.Error()}, err
	}

	values := strings.Split(valuesStr, ",")

	for _, v := range values {
		v = strings.TrimSpace(v)
		sensor := adapter.SensorData{
			Pipeline:    pipeline,
			Key:         sensorKey,
			Status:      types.SensorStatus(v),
			LastUpdated: time.Now(),
		}
		if err := s.store.WriteSensor(ctx, pipeline, sensorKey, sensor); err != nil {
			err = fmt.Errorf("split-sensor mutation: write sensor failed for status %q: %w", v, err)
			return types.MutationRecord{Applied: false, Mutation: "split-sensor", Error: err.Error()}, err
		}
	}

	return types.MutationRecord{
		ObjectKey: obj.Key,
		Mutation:  "split-sensor",
		Params:    params,
		Applied:   true,
		Timestamp: time.Now(),
	}, nil
}
