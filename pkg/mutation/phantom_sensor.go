package mutation

import (
	"context"
	"fmt"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// PhantomSensorMutation writes a sensor record for a pipeline that doesn't
// actually have data, simulating a phantom or ghost sensor entry.
type PhantomSensorMutation struct {
	store adapter.SensorStore
}

// NewPhantomSensorMutation creates a PhantomSensorMutation with the given sensor store.
func NewPhantomSensorMutation(store adapter.SensorStore) *PhantomSensorMutation {
	return &PhantomSensorMutation{store: store}
}

// Type returns "phantom-sensor".
func (p *PhantomSensorMutation) Type() string { return "phantom-sensor" }

// Apply writes a fake sensor record to the state store.
// Params:
//   - "pipeline" (required): pipeline the sensor belongs to.
//   - "sensor_key" (required): sensor key identifier.
//   - "status" (optional, default "ready"): sensor status to write.
func (p *PhantomSensorMutation) Apply(ctx context.Context, obj types.DataObject, _ adapter.DataTransport, params map[string]string, clock adapter.Clock) (types.MutationRecord, error) {
	pipeline, ok := params["pipeline"]
	if !ok || pipeline == "" {
		err := fmt.Errorf("phantom-sensor mutation: missing required param \"pipeline\"")
		return types.MutationRecord{Applied: false, Mutation: "phantom-sensor", Error: err.Error()}, err
	}
	sensorKey, ok := params["sensor_key"]
	if !ok || sensorKey == "" {
		err := fmt.Errorf("phantom-sensor mutation: missing required param \"sensor_key\"")
		return types.MutationRecord{Applied: false, Mutation: "phantom-sensor", Error: err.Error()}, err
	}

	status := types.SensorStatus(params["status"])
	if status == "" {
		status = types.SensorStatusReady
	}

	// Copy remaining params (beyond the known keys) into metadata.
	knownKeys := map[string]struct{}{
		"pipeline":   {},
		"sensor_key": {},
		"status":     {},
	}
	var metadata map[string]string
	for k, v := range params {
		if _, isKnown := knownKeys[k]; !isKnown {
			if metadata == nil {
				metadata = make(map[string]string)
			}
			metadata[k] = v
		}
	}

	sensor := adapter.SensorData{
		Pipeline:    pipeline,
		Key:         sensorKey,
		Status:      status,
		LastUpdated: clock.Now(),
		Metadata:    metadata,
	}

	if err := p.store.WriteSensor(ctx, pipeline, sensorKey, sensor); err != nil {
		err = fmt.Errorf("phantom-sensor mutation: write sensor failed: %w", err)
		return types.MutationRecord{Applied: false, Mutation: "phantom-sensor", Error: err.Error()}, err
	}

	return types.MutationRecord{
		ObjectKey: obj.Key,
		Mutation:  "phantom-sensor",
		Params:    params,
		Applied:   true,
		Timestamp: clock.Now(),
	}, nil
}
