package mutation_test

import (
	"context"
	"testing"
	"time"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/mutation"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

func TestTimestampForgeryMutation_Type(t *testing.T) {
	store := newMockStateStore()
	m := mutation.NewTimestampForgeryMutation(store)
	if got := m.Type(); got != "timestamp-forgery" {
		t.Errorf("Type() = %q, want %q", got, "timestamp-forgery")
	}
}

func TestTimestampForgeryMutation_Apply(t *testing.T) {
	now := time.Now()
	seededSensor := adapter.SensorData{
		Pipeline:    "etl-daily",
		Key:         "clock-sensor",
		Status:      types.SensorStatusReady,
		LastUpdated: now,
		Metadata:    map[string]string{"existing": "value"},
	}

	tests := []struct {
		name          string
		params        map[string]string
		seedSensor    *adapter.SensorData
		readSensorErr bool
		writeSensorErr bool
		wantApplied   bool
		wantErr       bool
		check         func(t *testing.T, store *mockStateStore)
	}{
		{
			name: "both_offsets",
			params: map[string]string{
				"sensor_key":               "clock-sensor",
				"pipeline":                 "etl-daily",
				"last_updated_offset":      "-6h",
				"payload_timestamp_offset": "+2h",
			},
			seedSensor:  &seededSensor,
			wantApplied: true,
			wantErr:     false,
			check: func(t *testing.T, store *mockStateStore) {
				t.Helper()
				written, ok := store.sensors["etl-daily/clock-sensor"]
				if !ok {
					t.Fatal("sensor not written to store")
				}
				// LastUpdated should be ~6h ago (within 5s tolerance).
				expectedLastUpdated := time.Now().Add(-6 * time.Hour)
				diff := written.LastUpdated.Sub(expectedLastUpdated)
				if diff < 0 {
					diff = -diff
				}
				if diff > 5*time.Second {
					t.Errorf("LastUpdated diff from expected = %v, want < 5s", diff)
				}
				// payload_timestamp should be ~2h ahead (within 5s tolerance).
				payloadStr, ok := written.Metadata["payload_timestamp"]
				if !ok {
					t.Fatal("Metadata[\"payload_timestamp\"] not set")
				}
				payload, err := time.Parse(time.RFC3339Nano, payloadStr)
				if err != nil {
					t.Fatalf("parse payload_timestamp: %v", err)
				}
				expectedPayload := time.Now().Add(2 * time.Hour)
				diff = payload.Sub(expectedPayload)
				if diff < 0 {
					diff = -diff
				}
				if diff > 5*time.Second {
					t.Errorf("payload_timestamp diff from expected = %v, want < 5s", diff)
				}
			},
		},
		{
			name: "last_updated_only",
			params: map[string]string{
				"sensor_key":          "clock-sensor",
				"pipeline":            "etl-daily",
				"last_updated_offset": "-3h",
			},
			seedSensor:  &seededSensor,
			wantApplied: true,
			wantErr:     false,
			check: func(t *testing.T, store *mockStateStore) {
				t.Helper()
				written, ok := store.sensors["etl-daily/clock-sensor"]
				if !ok {
					t.Fatal("sensor not written to store")
				}
				// LastUpdated should be ~3h ago.
				expectedLastUpdated := time.Now().Add(-3 * time.Hour)
				diff := written.LastUpdated.Sub(expectedLastUpdated)
				if diff < 0 {
					diff = -diff
				}
				if diff > 5*time.Second {
					t.Errorf("LastUpdated diff = %v, want < 5s", diff)
				}
				// payload_timestamp must NOT be set.
				if _, ok := written.Metadata["payload_timestamp"]; ok {
					t.Error("payload_timestamp should not be set when only last_updated_offset provided")
				}
			},
		},
		{
			name: "payload_timestamp_only",
			params: map[string]string{
				"sensor_key":               "clock-sensor",
				"pipeline":                 "etl-daily",
				"payload_timestamp_offset": "-1h",
			},
			seedSensor:  &seededSensor,
			wantApplied: true,
			wantErr:     false,
			check: func(t *testing.T, store *mockStateStore) {
				t.Helper()
				written, ok := store.sensors["etl-daily/clock-sensor"]
				if !ok {
					t.Fatal("sensor not written to store")
				}
				// payload_timestamp should be ~1h ago.
				payloadStr, ok := written.Metadata["payload_timestamp"]
				if !ok {
					t.Fatal("Metadata[\"payload_timestamp\"] not set")
				}
				payload, err := time.Parse(time.RFC3339Nano, payloadStr)
				if err != nil {
					t.Fatalf("parse payload_timestamp: %v", err)
				}
				expectedPayload := time.Now().Add(-1 * time.Hour)
				diff := payload.Sub(expectedPayload)
				if diff < 0 {
					diff = -diff
				}
				if diff > 5*time.Second {
					t.Errorf("payload_timestamp diff = %v, want < 5s", diff)
				}
				// LastUpdated should be unchanged (seeded value preserved).
				diff = written.LastUpdated.Sub(seededSensor.LastUpdated)
				if diff < 0 {
					diff = -diff
				}
				if diff > 5*time.Second {
					t.Errorf("LastUpdated changed unexpectedly: diff = %v", diff)
				}
			},
		},
		{
			name: "sensor_not_found",
			params: map[string]string{
				"sensor_key":          "new-sensor",
				"pipeline":            "etl-daily",
				"last_updated_offset": "-2h",
			},
			// No seedSensor — mock returns zero-value + nil (not found).
			wantApplied: true,
			wantErr:     false,
			check: func(t *testing.T, store *mockStateStore) {
				t.Helper()
				written, ok := store.sensors["etl-daily/new-sensor"]
				if !ok {
					t.Fatal("sensor not written to store")
				}
				if written.Status != "ready" {
					t.Errorf("new sensor Status = %q, want %q", written.Status, "ready")
				}
				// LastUpdated should be shifted ~2h ago.
				expectedLastUpdated := time.Now().Add(-2 * time.Hour)
				diff := written.LastUpdated.Sub(expectedLastUpdated)
				if diff < 0 {
					diff = -diff
				}
				if diff > 5*time.Second {
					t.Errorf("LastUpdated diff = %v, want < 5s", diff)
				}
			},
		},
		{
			name: "preserves_existing_metadata",
			params: map[string]string{
				"sensor_key":               "clock-sensor",
				"pipeline":                 "etl-daily",
				"payload_timestamp_offset": "+1h",
			},
			seedSensor:  &seededSensor,
			wantApplied: true,
			wantErr:     false,
			check: func(t *testing.T, store *mockStateStore) {
				t.Helper()
				written, ok := store.sensors["etl-daily/clock-sensor"]
				if !ok {
					t.Fatal("sensor not written to store")
				}
				if written.Metadata["existing"] != "value" {
					t.Errorf("Metadata[\"existing\"] = %q, want %q", written.Metadata["existing"], "value")
				}
				if _, ok := written.Metadata["payload_timestamp"]; !ok {
					t.Error("Metadata[\"payload_timestamp\"] not set")
				}
			},
		},
		{
			name: "missing_sensor_key",
			params: map[string]string{
				"pipeline":            "etl-daily",
				"last_updated_offset": "-6h",
			},
			wantApplied: false,
			wantErr:     true,
		},
		{
			name: "missing_pipeline",
			params: map[string]string{
				"sensor_key":          "clock-sensor",
				"last_updated_offset": "-6h",
			},
			wantApplied: false,
			wantErr:     true,
		},
		{
			name: "no_offsets",
			params: map[string]string{
				"sensor_key": "clock-sensor",
				"pipeline":   "etl-daily",
			},
			wantApplied: false,
			wantErr:     true,
		},
		{
			name: "invalid_last_updated_offset",
			params: map[string]string{
				"sensor_key":          "clock-sensor",
				"pipeline":            "etl-daily",
				"last_updated_offset": "bad",
			},
			wantApplied: false,
			wantErr:     true,
		},
		{
			name: "invalid_payload_timestamp_offset",
			params: map[string]string{
				"sensor_key":               "clock-sensor",
				"pipeline":                 "etl-daily",
				"payload_timestamp_offset": "bad",
			},
			wantApplied: false,
			wantErr:     true,
		},
		{
			name: "write_error",
			params: map[string]string{
				"sensor_key":          "clock-sensor",
				"pipeline":            "etl-daily",
				"last_updated_offset": "-6h",
			},
			seedSensor:     &seededSensor,
			writeSensorErr: true,
			wantApplied:    false,
			wantErr:        true,
		},
		{
			name: "read_error",
			params: map[string]string{
				"sensor_key":          "clock-sensor",
				"pipeline":            "etl-daily",
				"last_updated_offset": "-6h",
			},
			readSensorErr: true,
			wantApplied:   false,
			wantErr:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newMockStateStore()
			store.readSensorErr = tt.readSensorErr
			store.writeSensorErr = tt.writeSensorErr

			if tt.seedSensor != nil {
				store.sensors[tt.seedSensor.Pipeline+"/"+tt.seedSensor.Key] = *tt.seedSensor
			}

			transport := newMockTransport()
			m := mutation.NewTimestampForgeryMutation(store)
			obj := types.DataObject{Key: "test/data.csv"}

			record, err := m.Apply(context.Background(), obj, transport, tt.params)

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
			if record.Mutation != "timestamp-forgery" {
				t.Errorf("Mutation = %q, want %q", record.Mutation, "timestamp-forgery")
			}

			if tt.check != nil {
				tt.check(t, store)
			}
		})
	}
}
