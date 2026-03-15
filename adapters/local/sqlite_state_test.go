package local_test

import (
	"context"
	"testing"
	"time"

	"github.com/dwsmith1983/chaos-data/adapters/local"
	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

func newTestSQLiteState(t *testing.T) *local.SQLiteState {
	t.Helper()
	s, err := local.NewSQLiteState(":memory:")
	if err != nil {
		t.Fatalf("NewSQLiteState(:memory:) error = %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestSQLiteState_ReadSensor_NotFound(t *testing.T) {
	t.Parallel()
	s := newTestSQLiteState(t)

	got, err := s.ReadSensor(context.Background(), "pipeline-a", "key-1")
	if err != nil {
		t.Fatalf("ReadSensor() error = %v, want nil", err)
	}
	if got.Pipeline != "" || got.Key != "" || got.Status != "" || !got.LastUpdated.IsZero() || got.Metadata != nil {
		t.Errorf("ReadSensor() = %+v, want zero-value SensorData", got)
	}
}

func TestSQLiteState_WriteThenReadSensor(t *testing.T) {
	t.Parallel()
	s := newTestSQLiteState(t)

	now := time.Now().UTC().Truncate(time.Nanosecond)
	want := adapter.SensorData{
		Pipeline:    "pipeline-a",
		Key:         "key-1",
		Status:      types.SensorStatusReady,
		LastUpdated: now,
		Metadata:    nil,
	}

	ctx := context.Background()
	if err := s.WriteSensor(ctx, "pipeline-a", "key-1", want); err != nil {
		t.Fatalf("WriteSensor() error = %v", err)
	}

	got, err := s.ReadSensor(ctx, "pipeline-a", "key-1")
	if err != nil {
		t.Fatalf("ReadSensor() error = %v", err)
	}

	if got.Pipeline != want.Pipeline {
		t.Errorf("Pipeline = %q, want %q", got.Pipeline, want.Pipeline)
	}
	if got.Key != want.Key {
		t.Errorf("Key = %q, want %q", got.Key, want.Key)
	}
	if got.Status != want.Status {
		t.Errorf("Status = %q, want %q", got.Status, want.Status)
	}
	if !got.LastUpdated.Equal(want.LastUpdated) {
		t.Errorf("LastUpdated = %v, want %v", got.LastUpdated, want.LastUpdated)
	}
}

func TestSQLiteState_WriteSensorWithMetadata(t *testing.T) {
	t.Parallel()
	s := newTestSQLiteState(t)

	now := time.Now().UTC().Truncate(time.Nanosecond)
	meta := map[string]string{"region": "us-east-1", "env": "staging"}
	want := adapter.SensorData{
		Pipeline:    "pipeline-b",
		Key:         "sensor-x",
		Status:      types.SensorStatusPending,
		LastUpdated: now,
		Metadata:    meta,
	}

	ctx := context.Background()
	if err := s.WriteSensor(ctx, "pipeline-b", "sensor-x", want); err != nil {
		t.Fatalf("WriteSensor() error = %v", err)
	}

	got, err := s.ReadSensor(ctx, "pipeline-b", "sensor-x")
	if err != nil {
		t.Fatalf("ReadSensor() error = %v", err)
	}

	if len(got.Metadata) != len(meta) {
		t.Fatalf("Metadata length = %d, want %d", len(got.Metadata), len(meta))
	}
	for k, v := range meta {
		if got.Metadata[k] != v {
			t.Errorf("Metadata[%q] = %q, want %q", k, got.Metadata[k], v)
		}
	}
}

func TestSQLiteState_DeleteSensor(t *testing.T) {
	t.Parallel()
	s := newTestSQLiteState(t)

	ctx := context.Background()
	data := adapter.SensorData{
		Pipeline:    "pipeline-a",
		Key:         "key-del",
		Status:      types.SensorStatusReady,
		LastUpdated: time.Now().UTC(),
	}
	if err := s.WriteSensor(ctx, "pipeline-a", "key-del", data); err != nil {
		t.Fatalf("WriteSensor() error = %v", err)
	}

	if err := s.DeleteSensor(ctx, "pipeline-a", "key-del"); err != nil {
		t.Fatalf("DeleteSensor() error = %v", err)
	}

	got, err := s.ReadSensor(ctx, "pipeline-a", "key-del")
	if err != nil {
		t.Fatalf("ReadSensor() after delete error = %v", err)
	}
	if got.Pipeline != "" || got.Key != "" || got.Status != "" || !got.LastUpdated.IsZero() || got.Metadata != nil {
		t.Errorf("ReadSensor() after delete = %+v, want zero-value", got)
	}
}

func TestSQLiteState_ReadTriggerStatus_NotFound(t *testing.T) {
	t.Parallel()
	s := newTestSQLiteState(t)

	key := adapter.TriggerKey{
		Pipeline: "pipeline-a",
		Schedule: "daily",
		Date:     "2025-01-01",
	}
	got, err := s.ReadTriggerStatus(context.Background(), key)
	if err != nil {
		t.Fatalf("ReadTriggerStatus() error = %v, want nil", err)
	}
	if got != "" {
		t.Errorf("ReadTriggerStatus() = %q, want empty string", got)
	}
}

func TestSQLiteState_WriteThenReadTriggerStatus(t *testing.T) {
	t.Parallel()
	s := newTestSQLiteState(t)

	key := adapter.TriggerKey{
		Pipeline: "pipeline-a",
		Schedule: "daily",
		Date:     "2025-07-01",
	}
	ctx := context.Background()

	if err := s.WriteTriggerStatus(ctx, key, "fired"); err != nil {
		t.Fatalf("WriteTriggerStatus() error = %v", err)
	}

	got, err := s.ReadTriggerStatus(ctx, key)
	if err != nil {
		t.Fatalf("ReadTriggerStatus() error = %v", err)
	}
	if got != "fired" {
		t.Errorf("ReadTriggerStatus() = %q, want %q", got, "fired")
	}
}

func TestSQLiteState_WriteEvent(t *testing.T) {
	t.Parallel()
	s := newTestSQLiteState(t)

	event := types.ChaosEvent{
		ID:           "evt-001",
		ExperimentID: "exp-001",
		Scenario:     "delay-injection",
		Category:     "latency",
		Severity:     types.SeverityModerate,
		Target:       "orders-pipeline",
		Mutation:     "add-delay",
		Params:       map[string]string{"delay_ms": "500"},
		Timestamp:    time.Now().UTC().Truncate(time.Nanosecond),
		Mode:         "deterministic",
	}

	if err := s.WriteEvent(context.Background(), event); err != nil {
		t.Fatalf("WriteEvent() error = %v", err)
	}
}

func TestSQLiteState_ReadChaosEvents(t *testing.T) {
	t.Parallel()
	s := newTestSQLiteState(t)

	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Nanosecond)

	events := []types.ChaosEvent{
		{
			ID:           "evt-a1",
			ExperimentID: "exp-a",
			Scenario:     "null-injection",
			Category:     "data-quality",
			Severity:     types.SeverityLow,
			Target:       "users-pipeline",
			Mutation:     "set-null",
			Params:       map[string]string{"column": "email"},
			Timestamp:    now,
			Mode:         "deterministic",
		},
		{
			ID:           "evt-a2",
			ExperimentID: "exp-a",
			Scenario:     "drop-column",
			Category:     "schema",
			Severity:     types.SeverityCritical,
			Target:       "users-pipeline",
			Mutation:     "remove-column",
			Params:       nil,
			Timestamp:    now.Add(time.Second),
			Mode:         "replay",
		},
		{
			ID:           "evt-b1",
			ExperimentID: "exp-b",
			Scenario:     "other-scenario",
			Category:     "other",
			Severity:     types.SeveritySevere,
			Target:       "other-pipeline",
			Mutation:     "corrupt",
			Params:       map[string]string{"rate": "0.1"},
			Timestamp:    now.Add(2 * time.Second),
			Mode:         "probabilistic",
		},
	}

	for _, e := range events {
		if err := s.WriteEvent(ctx, e); err != nil {
			t.Fatalf("WriteEvent(%s) error = %v", e.ID, err)
		}
	}

	got, err := s.ReadChaosEvents(ctx, "exp-a")
	if err != nil {
		t.Fatalf("ReadChaosEvents() error = %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("ReadChaosEvents() returned %d events, want 2", len(got))
	}

	// Verify first event fields.
	first := got[0]
	if first.ID != "evt-a1" {
		t.Errorf("first event ID = %q, want %q", first.ID, "evt-a1")
	}
	if first.ExperimentID != "exp-a" {
		t.Errorf("first event ExperimentID = %q, want %q", first.ExperimentID, "exp-a")
	}
	if first.Scenario != "null-injection" {
		t.Errorf("first event Scenario = %q, want %q", first.Scenario, "null-injection")
	}
	if first.Category != "data-quality" {
		t.Errorf("first event Category = %q, want %q", first.Category, "data-quality")
	}
	if first.Severity != types.SeverityLow {
		t.Errorf("first event Severity = %d, want %d", first.Severity, types.SeverityLow)
	}
	if first.Target != "users-pipeline" {
		t.Errorf("first event Target = %q, want %q", first.Target, "users-pipeline")
	}
	if first.Mutation != "set-null" {
		t.Errorf("first event Mutation = %q, want %q", first.Mutation, "set-null")
	}
	if first.Params["column"] != "email" {
		t.Errorf("first event Params[column] = %q, want %q", first.Params["column"], "email")
	}
	if !first.Timestamp.Equal(now) {
		t.Errorf("first event Timestamp = %v, want %v", first.Timestamp, now)
	}
	if first.Mode != "deterministic" {
		t.Errorf("first event Mode = %q, want %q", first.Mode, "deterministic")
	}

	// Verify second event.
	second := got[1]
	if second.ID != "evt-a2" {
		t.Errorf("second event ID = %q, want %q", second.ID, "evt-a2")
	}
	if second.Severity != types.SeverityCritical {
		t.Errorf("second event Severity = %d, want %d", second.Severity, types.SeverityCritical)
	}
	if second.Mode != "replay" {
		t.Errorf("second event Mode = %q, want %q", second.Mode, "replay")
	}
	// nil params should round-trip as nil or empty map.
	if second.Params != nil && len(second.Params) != 0 {
		t.Errorf("second event Params = %v, want nil or empty", second.Params)
	}
}

func TestSQLiteState_ReadChaosEvents_Empty(t *testing.T) {
	t.Parallel()
	s := newTestSQLiteState(t)

	got, err := s.ReadChaosEvents(context.Background(), "nonexistent-experiment")
	if err != nil {
		t.Fatalf("ReadChaosEvents() error = %v, want nil", err)
	}
	if got == nil {
		t.Fatal("ReadChaosEvents() returned nil, want non-nil empty slice")
	}
	if len(got) != 0 {
		t.Errorf("ReadChaosEvents() returned %d events, want 0", len(got))
	}
}
