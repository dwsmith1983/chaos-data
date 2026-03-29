package mutation_test

import (
	"context"
	"testing"

	"github.com/dwsmith1983/chaos-data/pkg/mutation"
	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

func TestSensorFlappingMutation_Type(t *testing.T) {
	store := newMockStateStore()
	s := mutation.NewSensorFlappingMutation(store)
	if got := s.Type(); got != "sensor-flapping" {
		t.Errorf("Type() = %q, want %q", got, "sensor-flapping")
	}
}

func TestSensorFlappingMutation_Apply(t *testing.T) {
	tests := []struct {
		name           string
		params         map[string]string
		wantApplied    bool
		wantErr        bool
		wantWriteCount int
		wantStatuses   []types.SensorStatus
	}{
		{
			name: "valid_flap",
			params: map[string]string{
				"sensor_key":       "sensor-1",
				"pipeline":         "etl-daily",
				"flap_count":       "4",
				"start_status":     "ready",
				"alternate_status": "pending",
			},
			wantApplied:    true,
			wantWriteCount: 4,
			wantStatuses: []types.SensorStatus{
				types.SensorStatusReady,
				types.SensorStatusPending,
				types.SensorStatusReady,
				types.SensorStatusPending,
			},
		},
		{
			name: "odd_flap_count",
			params: map[string]string{
				"sensor_key":       "sensor-2",
				"pipeline":         "etl-daily",
				"flap_count":       "3",
				"start_status":     "ready",
				"alternate_status": "pending",
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
			name: "custom_statuses",
			params: map[string]string{
				"sensor_key":       "sensor-3",
				"pipeline":         "etl-daily",
				"flap_count":       "4",
				"start_status":     "stale",
				"alternate_status": "ready",
			},
			wantApplied:    true,
			wantWriteCount: 4,
			wantStatuses: []types.SensorStatus{
				types.SensorStatusStale,
				types.SensorStatusReady,
				types.SensorStatusStale,
				types.SensorStatusReady,
			},
		},
		{
			name: "default_statuses_when_omitted",
			params: map[string]string{
				"sensor_key": "sensor-4",
				"pipeline":   "etl-daily",
				"flap_count": "2",
			},
			wantApplied:    true,
			wantWriteCount: 2,
			wantStatuses: []types.SensorStatus{
				types.SensorStatusReady,
				types.SensorStatusPending,
			},
		},
		{
			name: "missing_sensor_key",
			params: map[string]string{
				"pipeline":   "etl-daily",
				"flap_count": "2",
			},
			wantApplied: false,
			wantErr:     true,
		},
		{
			name: "missing_pipeline",
			params: map[string]string{
				"sensor_key": "sensor-1",
				"flap_count": "2",
			},
			wantApplied: false,
			wantErr:     true,
		},
		{
			name: "missing_flap_count",
			params: map[string]string{
				"sensor_key": "sensor-1",
				"pipeline":   "etl-daily",
			},
			wantApplied: false,
			wantErr:     true,
		},
		{
			name: "invalid_flap_count",
			params: map[string]string{
				"sensor_key": "sensor-1",
				"pipeline":   "etl-daily",
				"flap_count": "abc",
			},
			wantApplied: false,
			wantErr:     true,
		},
		{
			name: "flap_count_too_low",
			params: map[string]string{
				"sensor_key": "sensor-1",
				"pipeline":   "etl-daily",
				"flap_count": "1",
			},
			wantApplied: false,
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newMockStateStore()
			transport := newMockTransport()
			s := mutation.NewSensorFlappingMutation(store)
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
			if record.Mutation != "sensor-flapping" {
				t.Errorf("Mutation = %q, want %q", record.Mutation, "sensor-flapping")
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

func TestSensorFlappingMutation_WriteSensorError(t *testing.T) {
	store := newMockStateStore()
	store.WriteSensorErr = true
	transport := newMockTransport()
	s := mutation.NewSensorFlappingMutation(store)
	obj := types.DataObject{Key: "test/data.csv"}

	record, err := s.Apply(context.Background(), obj, transport, map[string]string{
		"sensor_key": "sensor-1",
		"pipeline":   "etl-daily",
		"flap_count": "2",
	}, adapter.NewWallClock())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if record.Applied {
		t.Error("expected Applied=false on error")
	}
}
