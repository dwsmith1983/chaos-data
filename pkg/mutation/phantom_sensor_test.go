package mutation_test

import (
	"context"
	"testing"

	"github.com/dwsmith1983/chaos-data/pkg/mutation"
	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

func TestPhantomSensorMutation_Type(t *testing.T) {
	store := newMockStateStore()
	p := mutation.NewPhantomSensorMutation(store)
	if got := p.Type(); got != "phantom-sensor" {
		t.Errorf("Type() = %q, want %q", got, "phantom-sensor")
	}
}

func TestPhantomSensorMutation_Apply(t *testing.T) {
	tests := []struct {
		name        string
		params      map[string]string
		wantApplied bool
		wantErr     bool
		wantStatus  types.SensorStatus
	}{
		{
			name: "writes phantom sensor with default ready status",
			params: map[string]string{
				"pipeline":   "etl-daily",
				"sensor_key": "phantom-1",
			},
			wantApplied: true,
			wantStatus:  types.SensorStatusReady,
		},
		{
			name: "writes phantom sensor with custom status",
			params: map[string]string{
				"pipeline":   "etl-daily",
				"sensor_key": "phantom-2",
				"status":     "pending",
			},
			wantApplied: true,
			wantStatus:  types.SensorStatusPending,
		},
		{
			name: "explicit ready status",
			params: map[string]string{
				"pipeline":   "etl-daily",
				"sensor_key": "phantom-3",
				"status":     "ready",
			},
			wantApplied: true,
			wantStatus:  types.SensorStatusReady,
		},
		{
			name: "extra params stored in metadata",
			params: map[string]string{
				"pipeline":     "etl-daily",
				"sensor_key":   "phantom-4",
				"status":       "ready",
				"sensor_count": "5",
				"custom_tag":   "abc",
			},
			wantApplied: true,
			wantStatus:  types.SensorStatusReady,
		},
		{
			name: "missing pipeline returns error",
			params: map[string]string{
				"sensor_key": "phantom-1",
			},
			wantApplied: false,
			wantErr:     true,
		},
		{
			name: "missing sensor_key returns error",
			params: map[string]string{
				"pipeline": "etl-daily",
			},
			wantApplied: false,
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newMockStateStore()
			transport := newMockTransport()
			p := mutation.NewPhantomSensorMutation(store)
			obj := types.DataObject{Key: "test/data.csv"}

			record, err := p.Apply(context.Background(), obj, transport, tt.params, adapter.NewWallClock())

			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if record.Applied {
					t.Error("expected Applied=false on error")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if record.Applied != tt.wantApplied {
				t.Errorf("Applied = %v, want %v", record.Applied, tt.wantApplied)
			}
			if record.Mutation != "phantom-sensor" {
				t.Errorf("Mutation = %q, want %q", record.Mutation, "phantom-sensor")
			}

			// Verify WriteSensor was called.
			if got := store.stateCallCount("WriteSensor"); got != 1 {
				t.Fatalf("WriteSensor call count = %d, want 1", got)
			}

			// Verify the written sensor data.
			calls := store.getStateCalls()
			for _, c := range calls {
				if c.Method == "WriteSensor" {
					if c.Pipeline != tt.params["pipeline"] {
						t.Errorf("sensor pipeline = %q, want %q", c.Pipeline, tt.params["pipeline"])
					}
					if c.Key != tt.params["sensor_key"] {
						t.Errorf("sensor key = %q, want %q", c.Key, tt.params["sensor_key"])
					}
					if c.Sensor.Status != tt.wantStatus {
						t.Errorf("sensor status = %q, want %q", c.Sensor.Status, tt.wantStatus)
					}
					// Verify extra params are stored in metadata.
					knownKeys := map[string]struct{}{
						"pipeline":   {},
						"sensor_key": {},
						"status":     {},
					}
					for k, v := range tt.params {
						if _, isKnown := knownKeys[k]; isKnown {
							continue
						}
						got, ok := c.Sensor.Metadata[k]
						if !ok {
							t.Errorf("Metadata[%q] not set, want %q", k, v)
						} else if got != v {
							t.Errorf("Metadata[%q] = %q, want %q", k, got, v)
						}
					}
					// Known keys must NOT appear in metadata.
					for k := range knownKeys {
						if _, inMeta := c.Sensor.Metadata[k]; inMeta {
							t.Errorf("known param %q should not be in Metadata", k)
						}
					}
				}
			}
		})
	}
}

func TestPhantomSensorMutation_WriteSensorError(t *testing.T) {
	store := newMockStateStore()
	store.WriteSensorErr = true
	transport := newMockTransport()
	p := mutation.NewPhantomSensorMutation(store)
	obj := types.DataObject{Key: "test/data.csv"}

	record, err := p.Apply(context.Background(), obj, transport, map[string]string{
		"pipeline":   "etl-daily",
		"sensor_key": "phantom-1",
	}, adapter.NewWallClock())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if record.Applied {
		t.Error("expected Applied=false on error")
	}
}
