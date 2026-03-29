package airflow_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/dwsmith1983/chaos-data/adapters/airflow"
	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// ---------------------------------------------------------------------------
// ReadSensor
// ---------------------------------------------------------------------------

func TestAirflowVariableState_ReadSensor(t *testing.T) {
	t.Parallel()

	ts := time.Date(2026, 3, 14, 10, 0, 0, 0, time.UTC)

	tests := []struct {
		name     string
		mock     func(ctx context.Context, key string) (airflow.Variable, error)
		wantData adapter.SensorData
		wantErr  bool
	}{
		{
			name: "found",
			mock: func(_ context.Context, _ string) (airflow.Variable, error) {
				data := adapter.SensorData{
					Pipeline:    "etl",
					Key:         "landing",
					Status:      types.SensorStatusReady,
					LastUpdated: ts,
					Metadata:    map[string]string{"src": "s3"},
				}
				raw, _ := json.Marshal(data)
				return airflow.Variable{Key: "chaos:sensor:etl:landing", Value: string(raw)}, nil
			},
			wantData: adapter.SensorData{
				Pipeline:    "etl",
				Key:         "landing",
				Status:      types.SensorStatusReady,
				LastUpdated: ts,
				Metadata:    map[string]string{"src": "s3"},
			},
		},
		{
			name: "not found returns zero value",
			mock: func(_ context.Context, _ string) (airflow.Variable, error) {
				return airflow.Variable{}, airflow.ErrVariableNotFound
			},
			wantData: adapter.SensorData{},
		},
		{
			name: "api error",
			mock: func(_ context.Context, _ string) (airflow.Variable, error) {
				return airflow.Variable{}, errors.New("boom")
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			store := airflow.NewAirflowVariableState(&mockAirflowVariableAPI{
				getVariableFn: tt.mock,
			})
			got, err := store.ReadSensor(context.Background(), "etl", "landing")

			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got.Pipeline != tt.wantData.Pipeline {
				t.Errorf("Pipeline = %q, want %q", got.Pipeline, tt.wantData.Pipeline)
			}
			if got.Status != tt.wantData.Status {
				t.Errorf("Status = %q, want %q", got.Status, tt.wantData.Status)
			}
			if !got.LastUpdated.Equal(tt.wantData.LastUpdated) {
				t.Errorf("LastUpdated = %v, want %v", got.LastUpdated, tt.wantData.LastUpdated)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// WriteSensor
// ---------------------------------------------------------------------------

func TestAirflowVariableState_WriteSensor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		data    adapter.SensorData
		wantErr bool
	}{
		{
			name: "success with metadata",
			data: adapter.SensorData{
				Pipeline:    "etl",
				Key:         "landing",
				Status:      types.SensorStatusReady,
				LastUpdated: time.Now(),
				Metadata:    map[string]string{"src": "s3"},
			},
		},
		{
			name: "nil metadata normalised",
			data: adapter.SensorData{
				Pipeline:    "etl",
				Key:         "landing",
				Status:      types.SensorStatusPending,
				LastUpdated: time.Now(),
				Metadata:    nil,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var captured airflow.Variable
			store := airflow.NewAirflowVariableState(&mockAirflowVariableAPI{
				setVariableFn: func(_ context.Context, v airflow.Variable) error {
					captured = v
					return nil
				},
			})

			err := store.WriteSensor(context.Background(), "etl", "landing", tt.data)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			// Verify the stored JSON round-trips.
			var stored adapter.SensorData
			if err := json.Unmarshal([]byte(captured.Value), &stored); err != nil {
				t.Fatalf("unmarshal stored value: %v", err)
			}
			if stored.Status != tt.data.Status {
				t.Errorf("stored Status = %q, want %q", stored.Status, tt.data.Status)
			}
			// When metadata is nil, the JSON omits the field (omitempty),
			// but ReadSensor round-trips this back to nil, which matches
			// SQLiteState zero-value behavior.
			// Verify description format.
			if !strings.Contains(captured.Description, "chaos-data sensor:") {
				t.Errorf("Description = %q, expected to contain 'chaos-data sensor:'", captured.Description)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// DeleteSensor
// ---------------------------------------------------------------------------

func TestAirflowVariableState_DeleteSensor(t *testing.T) {
	t.Parallel()

	var deletedKey string
	store := airflow.NewAirflowVariableState(&mockAirflowVariableAPI{
		deleteVariableFn: func(_ context.Context, key string) error {
			deletedKey = key
			return nil
		},
	})

	if err := store.DeleteSensor(context.Background(), "etl", "landing"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	wantKey := "chaos:sensor:etl:landing"
	if deletedKey != wantKey {
		t.Errorf("deleted key = %q, want %q", deletedKey, wantKey)
	}
}

// ---------------------------------------------------------------------------
// ReadTriggerStatus
// ---------------------------------------------------------------------------

func TestAirflowVariableState_ReadTriggerStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		mock       func(ctx context.Context, key string) (airflow.Variable, error)
		wantStatus string
		wantErr    bool
	}{
		{
			name: "found",
			mock: func(_ context.Context, _ string) (airflow.Variable, error) {
				return airflow.Variable{Value: "fired"}, nil
			},
			wantStatus: "fired",
		},
		{
			name: "not found returns empty",
			mock: func(_ context.Context, _ string) (airflow.Variable, error) {
				return airflow.Variable{}, airflow.ErrVariableNotFound
			},
			wantStatus: "",
		},
		{
			name: "api error",
			mock: func(_ context.Context, _ string) (airflow.Variable, error) {
				return airflow.Variable{}, errors.New("boom")
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			store := airflow.NewAirflowVariableState(&mockAirflowVariableAPI{
				getVariableFn: tt.mock,
			})
			got, err := store.ReadTriggerStatus(context.Background(), adapter.TriggerKey{
				Pipeline: "etl", Schedule: "daily", Date: "2026-03-14",
			})

			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.wantStatus {
				t.Errorf("status = %q, want %q", got, tt.wantStatus)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// WriteTriggerStatus
// ---------------------------------------------------------------------------

func TestAirflowVariableState_WriteTriggerStatus(t *testing.T) {
	t.Parallel()

	var captured airflow.Variable
	store := airflow.NewAirflowVariableState(&mockAirflowVariableAPI{
		setVariableFn: func(_ context.Context, v airflow.Variable) error {
			captured = v
			return nil
		},
	})

	key := adapter.TriggerKey{Pipeline: "etl", Schedule: "daily", Date: "2026-03-14"}
	if err := store.WriteTriggerStatus(context.Background(), key, "fired"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if captured.Value != "fired" {
		t.Errorf("Value = %q, want %q", captured.Value, "fired")
	}
	wantKey := "chaos:trigger:etl:daily:2026-03-14"
	if captured.Key != wantKey {
		t.Errorf("Key = %q, want %q", captured.Key, wantKey)
	}
}

// ---------------------------------------------------------------------------
// WriteEvent + ReadChaosEvents
// ---------------------------------------------------------------------------

func TestAirflowVariableState_WriteAndReadChaosEvents(t *testing.T) {
	t.Parallel()

	ts1 := time.Date(2026, 3, 14, 10, 0, 0, 0, time.UTC)
	ts2 := time.Date(2026, 3, 14, 11, 0, 0, 0, time.UTC)

	event1 := types.ChaosEvent{
		ID: "e1", ExperimentID: "exp-1", Scenario: "stale-sensor",
		Severity: 1, Timestamp: ts1, Mode: "deterministic",
	}
	event2 := types.ChaosEvent{
		ID: "e2", ExperimentID: "exp-1", Scenario: "phantom-sensor",
		Severity: 2, Timestamp: ts2, Mode: "deterministic",
	}

	// Build the variables that ListVariables would return.
	raw1, _ := json.Marshal(event1)
	raw2, _ := json.Marshal(event2)

	store := airflow.NewAirflowVariableState(&mockAirflowVariableAPI{
		setVariableFn: func(_ context.Context, _ airflow.Variable) error {
			return nil
		},
		listVariablesFn: func(_ context.Context) ([]airflow.Variable, error) {
			return []airflow.Variable{
				// Return in reverse order to test sorting.
				{Key: airflow.EventKey("exp-1", ts2, "e2"), Value: string(raw2)},
				{Key: airflow.EventKey("exp-1", ts1, "e1"), Value: string(raw1)},
				// Unrelated variable — should be filtered out.
				{Key: "chaos:sensor:etl:landing", Value: "{}"},
			}, nil
		},
	})

	// Write events (we don't actually store them in the mock, just verify no error).
	if err := store.WriteEvent(context.Background(), event1); err != nil {
		t.Fatalf("WriteEvent(e1): %v", err)
	}
	if err := store.WriteEvent(context.Background(), event2); err != nil {
		t.Fatalf("WriteEvent(e2): %v", err)
	}

	// Read back — should be sorted ascending.
	events, err := store.ReadChaosEvents(context.Background(), "exp-1")
	if err != nil {
		t.Fatalf("ReadChaosEvents: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("got %d events, want 2", len(events))
	}
	if events[0].ID != "e1" {
		t.Errorf("events[0].ID = %q, want %q", events[0].ID, "e1")
	}
	if events[1].ID != "e2" {
		t.Errorf("events[1].ID = %q, want %q", events[1].ID, "e2")
	}
}

func TestAirflowVariableState_ReadChaosEvents_Empty(t *testing.T) {
	t.Parallel()

	store := airflow.NewAirflowVariableState(&mockAirflowVariableAPI{
		listVariablesFn: func(_ context.Context) ([]airflow.Variable, error) {
			return nil, nil
		},
	})

	events, err := store.ReadChaosEvents(context.Background(), "exp-none")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if events == nil {
		t.Fatal("expected non-nil empty slice, got nil")
	}
	if len(events) != 0 {
		t.Errorf("got %d events, want 0", len(events))
	}
}

// ---------------------------------------------------------------------------
// WritePipelineConfig + ReadPipelineConfig
// ---------------------------------------------------------------------------

func TestAirflowVariableState_PipelineConfigRoundTrip(t *testing.T) {
	t.Parallel()

	original := []byte(`{"dag_id": "etl-daily", "schedule": "@daily"}`)
	encoded := base64.StdEncoding.EncodeToString(original)

	var captured airflow.Variable
	store := airflow.NewAirflowVariableState(&mockAirflowVariableAPI{
		setVariableFn: func(_ context.Context, v airflow.Variable) error {
			captured = v
			return nil
		},
		getVariableFn: func(_ context.Context, _ string) (airflow.Variable, error) {
			return airflow.Variable{Value: encoded}, nil
		},
	})

	// Write.
	if err := store.WritePipelineConfig(context.Background(), "etl", original); err != nil {
		t.Fatalf("WritePipelineConfig: %v", err)
	}
	// Verify base64 encoding.
	if captured.Value != encoded {
		t.Errorf("stored value = %q, want base64 %q", captured.Value, encoded)
	}

	// Read back.
	got, err := store.ReadPipelineConfig(context.Background(), "etl")
	if err != nil {
		t.Fatalf("ReadPipelineConfig: %v", err)
	}
	if string(got) != string(original) {
		t.Errorf("got %q, want %q", string(got), string(original))
	}
}

func TestAirflowVariableState_ReadPipelineConfig_NotFound(t *testing.T) {
	t.Parallel()

	store := airflow.NewAirflowVariableState(&mockAirflowVariableAPI{
		getVariableFn: func(_ context.Context, _ string) (airflow.Variable, error) {
			return airflow.Variable{}, airflow.ErrVariableNotFound
		},
	})

	got, err := store.ReadPipelineConfig(context.Background(), "etl")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != nil {
		t.Errorf("expected nil, got %v", got)
	}
}

func TestAirflowVariableState_WritePipelineConfig_SizeGuard(t *testing.T) {
	t.Parallel()

	store := airflow.NewAirflowVariableState(&mockAirflowVariableAPI{})

	bigConfig := make([]byte, 65*1024) // 65 KB — over limit
	err := store.WritePipelineConfig(context.Background(), "etl", bigConfig)
	if err == nil {
		t.Fatal("expected size guard error, got nil")
	}
	if !strings.Contains(err.Error(), "exceeds maximum") {
		t.Errorf("error = %q, want substring 'exceeds maximum'", err.Error())
	}
}

// ---------------------------------------------------------------------------
// CountReruns + WriteRerun
// ---------------------------------------------------------------------------

func TestAirflowVariableState_CountReruns(t *testing.T) {
	t.Parallel()

	store := airflow.NewAirflowVariableState(&mockAirflowVariableAPI{
		setVariableFn: func(_ context.Context, _ airflow.Variable) error {
			return nil
		},
		listVariablesFn: func(_ context.Context) ([]airflow.Variable, error) {
			return []airflow.Variable{
				{Key: "chaos:rerun:etl:daily:2026-03-14:2026-03-14T10:00:00Z", Value: "reason1"},
				{Key: "chaos:rerun:etl:daily:2026-03-14:2026-03-14T11:00:00Z", Value: "reason2"},
				// Different date — should not match.
				{Key: "chaos:rerun:etl:daily:2026-03-15:2026-03-15T10:00:00Z", Value: "other"},
			}, nil
		},
	})

	// Write a rerun (no-op in mock, but exercises the path).
	if err := store.WriteRerun(context.Background(), "etl", "daily", "2026-03-14", "test"); err != nil {
		t.Fatalf("WriteRerun: %v", err)
	}

	count, err := store.CountReruns(context.Background(), "etl", "daily", "2026-03-14")
	if err != nil {
		t.Fatalf("CountReruns: %v", err)
	}
	if count != 2 {
		t.Errorf("count = %d, want 2", count)
	}
}

// ---------------------------------------------------------------------------
// DeleteByPrefix
// ---------------------------------------------------------------------------

func TestAirflowVariableState_DeleteByPrefix(t *testing.T) {
	t.Parallel()

	var deleted []string
	store := airflow.NewAirflowVariableState(&mockAirflowVariableAPI{
		listVariablesFn: func(_ context.Context) ([]airflow.Variable, error) {
			return []airflow.Variable{
				{Key: "chaos:sensor:etl:landing"},
				{Key: "chaos:sensor:etl:raw"},
				{Key: "chaos:trigger:other:daily:2026-03-14"},
			}, nil
		},
		deleteVariableFn: func(_ context.Context, key string) error {
			deleted = append(deleted, key)
			return nil
		},
	})

	if err := store.DeleteByPrefix(context.Background(), "sensor:etl:"); err != nil {
		t.Fatalf("DeleteByPrefix: %v", err)
	}

	if len(deleted) != 2 {
		t.Fatalf("deleted %d keys, want 2", len(deleted))
	}
	for _, key := range deleted {
		if !strings.HasPrefix(key, "chaos:sensor:etl:") {
			t.Errorf("deleted unexpected key %q", key)
		}
	}
}

// ---------------------------------------------------------------------------
// ReadJobEvents
// ---------------------------------------------------------------------------

func TestAirflowVariableState_ReadJobEvents(t *testing.T) {
	t.Parallel()

	ts1 := time.Date(2026, 3, 14, 10, 0, 0, 0, time.UTC)
	ts2 := time.Date(2026, 3, 14, 11, 0, 0, 0, time.UTC)

	je1 := adapter.JobEvent{
		Pipeline: "etl", Schedule: "daily", Date: "2026-03-14",
		Event: "start", RunID: "r1", Timestamp: ts1,
	}
	je2 := adapter.JobEvent{
		Pipeline: "etl", Schedule: "daily", Date: "2026-03-14",
		Event: "success", RunID: "r2", Timestamp: ts2,
	}

	raw1, _ := json.Marshal(je1)
	raw2, _ := json.Marshal(je2)

	store := airflow.NewAirflowVariableState(&mockAirflowVariableAPI{
		listVariablesFn: func(_ context.Context) ([]airflow.Variable, error) {
			return []airflow.Variable{
				// Return in ascending order to test descending sort.
				{Key: airflow.JobEventKey("etl", "daily", "2026-03-14", ts1, "r1"), Value: string(raw1)},
				{Key: airflow.JobEventKey("etl", "daily", "2026-03-14", ts2, "r2"), Value: string(raw2)},
				// Unrelated.
				{Key: "chaos:sensor:etl:landing", Value: "{}"},
			}, nil
		},
	})

	events, err := store.ReadJobEvents(context.Background(), "etl", "daily", "2026-03-14")
	if err != nil {
		t.Fatalf("ReadJobEvents: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("got %d events, want 2", len(events))
	}
	// Should be descending — ts2 first.
	if events[0].RunID != "r2" {
		t.Errorf("events[0].RunID = %q, want %q (most recent first)", events[0].RunID, "r2")
	}
	if events[1].RunID != "r1" {
		t.Errorf("events[1].RunID = %q, want %q", events[1].RunID, "r1")
	}
}

func TestAirflowVariableState_ReadJobEvents_Empty(t *testing.T) {
	t.Parallel()

	store := airflow.NewAirflowVariableState(&mockAirflowVariableAPI{
		listVariablesFn: func(_ context.Context) ([]airflow.Variable, error) {
			return nil, nil
		},
	})

	events, err := store.ReadJobEvents(context.Background(), "etl", "daily", "2026-03-14")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if events == nil {
		t.Fatal("expected non-nil empty slice, got nil")
	}
	if len(events) != 0 {
		t.Errorf("got %d events, want 0", len(events))
	}
}
