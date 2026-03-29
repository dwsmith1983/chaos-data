package mutation_test

import (
	"context"
	"testing"

	"github.com/dwsmith1983/chaos-data/pkg/mutation"
	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

func TestSplitSensorMutation_Type(t *testing.T) {
	store := newMockStateStore()
	s := mutation.NewSplitSensorMutation(store)
	if got := s.Type(); got != "split-sensor" {
		t.Errorf("Type() = %q, want %q", got, "split-sensor")
	}
}

func TestSplitSensorMutation_Apply(t *testing.T) {
	tests := []struct {
		name             string
		params           map[string]string
		wantApplied      bool
		wantErr          bool
		wantWriteCount   int
		wantStatuses     []types.SensorStatus
	}{
		{
			name: "writes three conflicting sensor values",
			params: map[string]string{
				"sensor_key":         "sensor-1",
				"pipeline":           "etl-daily",
				"conflicting_values": "ready,pending,ready",
			},
			wantApplied:    true,
			wantWriteCount: 3,
			wantStatuses: []types.SensorStatus{
				types.SensorStatusReady,
				types.SensorStatusPending,
				types.SensorStatusReady,
			},
		},
		{
			name: "writes two conflicting sensor values",
			params: map[string]string{
				"sensor_key":         "sensor-2",
				"pipeline":           "etl-daily",
				"conflicting_values": "stale,ready",
			},
			wantApplied:    true,
			wantWriteCount: 2,
			wantStatuses: []types.SensorStatus{
				types.SensorStatusStale,
				types.SensorStatusReady,
			},
		},
		{
			name: "missing sensor_key returns error",
			params: map[string]string{
				"pipeline":           "etl-daily",
				"conflicting_values": "ready,pending",
			},
			wantApplied: false,
			wantErr:     true,
		},
		{
			name: "missing pipeline returns error",
			params: map[string]string{
				"sensor_key":         "sensor-1",
				"conflicting_values": "ready,pending",
			},
			wantApplied: false,
			wantErr:     true,
		},
		{
			name: "missing conflicting_values returns error",
			params: map[string]string{
				"sensor_key": "sensor-1",
				"pipeline":   "etl-daily",
			},
			wantApplied: false,
			wantErr:     true,
		},
		{
			name: "empty conflicting_values returns error",
			params: map[string]string{
				"sensor_key":         "sensor-1",
				"pipeline":           "etl-daily",
				"conflicting_values": "",
			},
			wantApplied: false,
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newMockStateStore()
			transport := newMockTransport()
			s := mutation.NewSplitSensorMutation(store)
			obj := types.DataObject{Key: "test/data.csv"}

			record, err := s.Apply(context.Background(), obj, transport, tt.params, adapter.NewWallClock())

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
			if record.Mutation != "split-sensor" {
				t.Errorf("Mutation = %q, want %q", record.Mutation, "split-sensor")
			}

			// Verify WriteSensor call count.
			if got := store.stateCallCount("WriteSensor"); got != tt.wantWriteCount {
				t.Errorf("WriteSensor call count = %d, want %d", got, tt.wantWriteCount)
			}

			// Verify statuses in order.
			if len(tt.wantStatuses) > 0 {
				calls := store.getStateCalls()
				var sensorCalls []stateCall
				for _, c := range calls {
					if c.Method == "WriteSensor" {
						sensorCalls = append(sensorCalls, c)
					}
				}
				if len(sensorCalls) != len(tt.wantStatuses) {
					t.Fatalf("WriteSensor calls = %d, want %d", len(sensorCalls), len(tt.wantStatuses))
				}
				for i, wantStatus := range tt.wantStatuses {
					if sensorCalls[i].Sensor.Status != wantStatus {
						t.Errorf("WriteSensor[%d] status = %q, want %q", i, sensorCalls[i].Sensor.Status, wantStatus)
					}
				}
			}
		})
	}
}

func TestSplitSensorMutation_WriteSensorError(t *testing.T) {
	store := newMockStateStore()
	store.WriteSensorErr = true
	transport := newMockTransport()
	s := mutation.NewSplitSensorMutation(store)
	obj := types.DataObject{Key: "test/data.csv"}

	record, err := s.Apply(context.Background(), obj, transport, map[string]string{
		"sensor_key":         "sensor-1",
		"pipeline":           "etl-daily",
		"conflicting_values": "ready,pending",
	}, adapter.NewWallClock())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if record.Applied {
		t.Error("expected Applied=false on error")
	}
}
