package mutation

import (
	"context"
	"fmt"
	"time"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// CascadeDelayMutation holds data and writes a stale sensor status, simulating
// a cascading delay where upstream data is late and the sensor reflects staleness.
type CascadeDelayMutation struct {
	store adapter.SensorStore
}

// NewCascadeDelayMutation creates a CascadeDelayMutation with the given sensor store.
func NewCascadeDelayMutation(store adapter.SensorStore) *CascadeDelayMutation {
	return &CascadeDelayMutation{store: store}
}

// Type returns "cascade-delay".
func (c *CascadeDelayMutation) Type() string { return "cascade-delay" }

// Apply holds the object and writes a stale sensor for the upstream pipeline.
// Params:
//   - "upstream_pipeline" (required): pipeline name of the upstream data source.
//   - "delay_duration" (required): Go duration string for how long to hold the data.
//   - "sensor_key" (optional, default "arrival"): sensor key to mark as stale.
func (c *CascadeDelayMutation) Apply(ctx context.Context, obj types.DataObject, transport adapter.DataTransport, params map[string]string, clock adapter.Clock) (types.MutationRecord, error) {
	pipeline, ok := params["upstream_pipeline"]
	if !ok || pipeline == "" {
		err := fmt.Errorf("cascade-delay mutation: missing required param \"upstream_pipeline\"")
		return types.MutationRecord{Applied: false, Mutation: "cascade-delay", Error: err.Error()}, err
	}
	durationStr, ok := params["delay_duration"]
	if !ok || durationStr == "" {
		err := fmt.Errorf("cascade-delay mutation: missing required param \"delay_duration\"")
		return types.MutationRecord{Applied: false, Mutation: "cascade-delay", Error: err.Error()}, err
	}

	duration, err := time.ParseDuration(durationStr)
	if err != nil {
		err = fmt.Errorf("cascade-delay mutation: invalid delay_duration %q: %w", durationStr, err)
		return types.MutationRecord{Applied: false, Mutation: "cascade-delay", Error: err.Error()}, err
	}

	sensorKey := "arrival"
	if sk, ok := params["sensor_key"]; ok && sk != "" {
		sensorKey = sk
	}

	// Hold the object until now + duration.
	releaseAt := clock.Now().Add(duration)
	if err := transport.Hold(ctx, obj.Key, releaseAt); err != nil {
		err = fmt.Errorf("cascade-delay mutation: hold failed: %w", err)
		return types.MutationRecord{Applied: false, Mutation: "cascade-delay", Error: err.Error()}, err
	}

	// Write a stale sensor status.
	sensor := adapter.SensorData{
		Pipeline:    pipeline,
		Key:         sensorKey,
		Status:      types.SensorStatusStale,
		LastUpdated: clock.Now(),
	}
	if err := c.store.WriteSensor(ctx, pipeline, sensorKey, sensor); err != nil {
		err = fmt.Errorf("cascade-delay mutation: write sensor failed: %w", err)
		return types.MutationRecord{Applied: false, Mutation: "cascade-delay", Error: err.Error()}, err
	}

	return types.MutationRecord{
		ObjectKey: obj.Key,
		Mutation:  "cascade-delay",
		Params:    params,
		Applied:   true,
		Timestamp: clock.Now(),
	}, nil
}
