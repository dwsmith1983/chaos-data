package mutation_test

import (
	"context"
	"testing"
	"time"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/mutation"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

func TestStaleSensorMutation_Type(t *testing.T) {
	store := newMockStateStore()
	s := mutation.NewStaleSensorMutation(store)
	if got := s.Type(); got != "stale-sensor" {
		t.Errorf("Type() = %q, want %q", got, "stale-sensor")
	}
}

func TestStaleSensorMutation_Apply(t *testing.T) {
	now := time.Now()
	originalSensor := adapter.SensorData{
		Pipeline:    "etl-daily",
		Key:         "sensor-1",
		Status:      types.SensorStatusReady,
		LastUpdated: now,
		Metadata:    map[string]string{"source": "test"},
	}

	tests := []struct {
		name        string
		params      map[string]string
		sensor      adapter.SensorData
		wantApplied bool
		wantErr     bool
		checkAge    string // duration string, verify LastUpdated is at least this old
	}{
		{
			name: "basic stale sensor with 6h age",
			params: map[string]string{
				"sensor_key":      "sensor-1",
				"pipeline":        "etl-daily",
				"last_update_age": "6h",
			},
			sensor:      originalSensor,
			wantApplied: true,
			wantErr:     false,
			checkAge:    "6h",
		},
		{
			name: "stale sensor with 30m age",
			params: map[string]string{
				"sensor_key":      "sensor-1",
				"pipeline":        "etl-daily",
				"last_update_age": "30m",
			},
			sensor:      originalSensor,
			wantApplied: true,
			wantErr:     false,
			checkAge:    "30m",
		},
		{
			name: "missing sensor_key returns error",
			params: map[string]string{
				"pipeline":        "etl-daily",
				"last_update_age": "6h",
			},
			wantApplied: false,
			wantErr:     true,
		},
		{
			name: "missing pipeline returns error",
			params: map[string]string{
				"sensor_key":      "sensor-1",
				"last_update_age": "6h",
			},
			wantApplied: false,
			wantErr:     true,
		},
		{
			name: "age alias works when last_update_age missing",
			params: map[string]string{
				"sensor_key": "sensor-1",
				"pipeline":   "etl-daily",
				"age":        "4h",
			},
			sensor:      originalSensor,
			wantApplied: true,
			wantErr:     false,
			checkAge:    "4h",
		},
		{
			name: "last_update_age takes precedence over age",
			params: map[string]string{
				"sensor_key":      "sensor-1",
				"pipeline":        "etl-daily",
				"last_update_age": "6h",
				"age":             "1h",
			},
			sensor:      originalSensor,
			wantApplied: true,
			wantErr:     false,
			checkAge:    "6h",
		},
		{
			name: "missing both last_update_age and age returns error",
			params: map[string]string{
				"sensor_key": "sensor-1",
				"pipeline":   "etl-daily",
			},
			wantApplied: false,
			wantErr:     true,
		},
		{
			name: "invalid last_update_age returns error",
			params: map[string]string{
				"sensor_key":      "sensor-1",
				"pipeline":        "etl-daily",
				"last_update_age": "not-a-duration",
			},
			wantApplied: false,
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newMockStateStore()
			if tt.sensor.Key != "" {
				store.sensors[tt.sensor.Pipeline+"/"+tt.sensor.Key] = tt.sensor
			}

			transport := newMockTransport()
			s := mutation.NewStaleSensorMutation(store)
			obj := types.DataObject{Key: "test/data.csv"}

			before := time.Now()
			record, err := s.Apply(context.Background(), obj, transport, tt.params)

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
			if record.Mutation != "stale-sensor" {
				t.Errorf("Mutation = %q, want %q", record.Mutation, "stale-sensor")
			}

			// Verify sensor was written with old timestamp.
			if tt.checkAge != "" {
				age, _ := time.ParseDuration(tt.checkAge)
				written, ok := store.sensors[tt.params["pipeline"]+"/"+tt.params["sensor_key"]]
				if !ok {
					t.Fatal("sensor not written to store")
				}

				// LastUpdated should be approximately before - age (within 2s tolerance).
				expectedTime := before.Add(-age)
				diff := written.LastUpdated.Sub(expectedTime)
				if diff < 0 {
					diff = -diff
				}
				if diff > 2*time.Second {
					t.Errorf("LastUpdated diff from expected = %v, want < 2s", diff)
				}

				// Original metadata should be preserved.
				if written.Status != originalSensor.Status {
					t.Errorf("Status = %q, want %q", written.Status, originalSensor.Status)
				}
			}
		})
	}
}

func TestStaleSensorMutation_ReadSensorError(t *testing.T) {
	store := newMockStateStore()
	store.readSensorErr = true

	transport := newMockTransport()
	s := mutation.NewStaleSensorMutation(store)
	obj := types.DataObject{Key: "test/data.csv"}

	record, err := s.Apply(context.Background(), obj, transport, map[string]string{
		"sensor_key":      "sensor-1",
		"pipeline":        "etl-daily",
		"last_update_age": "6h",
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if record.Applied {
		t.Error("expected Applied=false on error")
	}
}
