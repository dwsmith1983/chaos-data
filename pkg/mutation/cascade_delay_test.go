package mutation_test

import (
	"context"
	"errors"
	"testing"

	"github.com/dwsmith1983/chaos-data/pkg/mutation"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

var errInjected = errors.New("injected error")

func TestCascadeDelayMutation_Type(t *testing.T) {
	store := newMockStateStore()
	m := mutation.NewCascadeDelayMutation(store)
	if got := m.Type(); got != "cascade-delay" {
		t.Errorf("Type() = %q, want %q", got, "cascade-delay")
	}
}

func TestCascadeDelayMutation_Apply(t *testing.T) {
	tests := []struct {
		name             string
		params           map[string]string
		wantApplied      bool
		wantErr          bool
		wantHoldCount    int
		wantSensorCount  int
		wantSensorStatus types.SensorStatus
	}{
		{
			name: "holds data and writes stale sensor",
			params: map[string]string{
				"upstream_pipeline": "raw-ingest",
				"delay_duration":    "2h",
				"sensor_key":        "arrival",
			},
			wantApplied:      true,
			wantHoldCount:    1,
			wantSensorCount:  1,
			wantSensorStatus: types.SensorStatusStale,
		},
		{
			name: "uses default sensor_key arrival",
			params: map[string]string{
				"upstream_pipeline": "raw-ingest",
				"delay_duration":    "30m",
			},
			wantApplied:      true,
			wantHoldCount:    1,
			wantSensorCount:  1,
			wantSensorStatus: types.SensorStatusStale,
		},
		{
			name: "missing upstream_pipeline returns error",
			params: map[string]string{
				"delay_duration": "2h",
			},
			wantApplied: false,
			wantErr:     true,
		},
		{
			name: "missing delay_duration returns error",
			params: map[string]string{
				"upstream_pipeline": "raw-ingest",
			},
			wantApplied: false,
			wantErr:     true,
		},
		{
			name: "invalid delay_duration returns error",
			params: map[string]string{
				"upstream_pipeline": "raw-ingest",
				"delay_duration":    "not-a-duration",
			},
			wantApplied: false,
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newMockStateStore()
			transport := newMockTransport()
			obj := types.DataObject{Key: "data/records.jsonl"}

			m := mutation.NewCascadeDelayMutation(store)
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
			if record.Mutation != "cascade-delay" {
				t.Errorf("Mutation = %q, want %q", record.Mutation, "cascade-delay")
			}

			// Verify Hold was called on the transport.
			if got := transport.callCount("Hold"); got != tt.wantHoldCount {
				t.Errorf("Hold call count = %d, want %d", got, tt.wantHoldCount)
			}

			// Verify WriteSensor was called on the state store.
			if got := store.stateCallCount("WriteSensor"); got != tt.wantSensorCount {
				t.Errorf("WriteSensor call count = %d, want %d", got, tt.wantSensorCount)
			}

			// Verify sensor status is stale.
			if tt.wantSensorCount > 0 {
				calls := store.getStateCalls()
				for _, c := range calls {
					if c.Method == "WriteSensor" {
						if c.Sensor.Status != tt.wantSensorStatus {
							t.Errorf("sensor status = %q, want %q", c.Sensor.Status, tt.wantSensorStatus)
						}
					}
				}
			}
		})
	}
}

func TestCascadeDelayMutation_SensorPipelineAndKey(t *testing.T) {
	store := newMockStateStore()
	transport := newMockTransport()
	obj := types.DataObject{Key: "data/records.jsonl"}

	m := mutation.NewCascadeDelayMutation(store)
	_, err := m.Apply(context.Background(), obj, transport, map[string]string{
		"upstream_pipeline": "raw-ingest",
		"delay_duration":    "1h",
		"sensor_key":        "file-check",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	calls := store.getStateCalls()
	for _, c := range calls {
		if c.Method == "WriteSensor" {
			if c.Pipeline != "raw-ingest" {
				t.Errorf("sensor pipeline = %q, want %q", c.Pipeline, "raw-ingest")
			}
			if c.Key != "file-check" {
				t.Errorf("sensor key = %q, want %q", c.Key, "file-check")
			}
		}
	}
}

func TestCascadeDelayMutation_HoldError(t *testing.T) {
	store := newMockStateStore()
	transport := newMockTransport()
	obj := types.DataObject{Key: "data/records.jsonl"}
	transport.holdErr[obj.Key] = errInjected

	m := mutation.NewCascadeDelayMutation(store)
	record, err := m.Apply(context.Background(), obj, transport, map[string]string{
		"upstream_pipeline": "raw-ingest",
		"delay_duration":    "1h",
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if record.Applied {
		t.Error("expected Applied=false on error")
	}
}

func TestCascadeDelayMutation_WriteSensorError(t *testing.T) {
	store := newMockStateStore()
	store.writeSensorErr = true
	transport := newMockTransport()
	obj := types.DataObject{Key: "data/records.jsonl"}

	m := mutation.NewCascadeDelayMutation(store)
	record, err := m.Apply(context.Background(), obj, transport, map[string]string{
		"upstream_pipeline": "raw-ingest",
		"delay_duration":    "1h",
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if record.Applied {
		t.Error("expected Applied=false on error")
	}
}
