package local_test

import (
	"context"
	"testing"
	"time"

	"github.com/dwsmith1983/chaos-data/adapters/local"
	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

func insertJobEvent(t *testing.T, s *local.SQLiteState, pipeline, schedule, date, event, runID, timestamp string) {
	t.Helper()
	_, err := s.DBForTest().Exec(
		`INSERT INTO job_events (pipeline, schedule, date, event, run_id, timestamp) VALUES (?, ?, ?, ?, ?, ?)`,
		pipeline, schedule, date, event, runID, timestamp,
	)
	if err != nil {
		t.Fatalf("insert job event: %v", err)
	}
}

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

func TestSQLiteState_WritePipelineConfig_ReadPipelineConfig(t *testing.T) {
	t.Parallel()
	s := newTestSQLiteState(t)
	ctx := context.Background()

	config := []byte(`{"pipeline":{"id":"test-pipe","schedule":"daily"}}`)
	if err := s.WritePipelineConfig(ctx, "test-pipe", config); err != nil {
		t.Fatalf("WritePipelineConfig() error = %v", err)
	}

	got, err := s.ReadPipelineConfig(ctx, "test-pipe")
	if err != nil {
		t.Fatalf("ReadPipelineConfig() error = %v", err)
	}
	if string(got) != string(config) {
		t.Errorf("ReadPipelineConfig() = %q, want %q", got, config)
	}
}

func TestSQLiteState_WritePipelineConfig_Overwrite(t *testing.T) {
	t.Parallel()
	s := newTestSQLiteState(t)
	ctx := context.Background()

	config1 := []byte(`{"version":1}`)
	config2 := []byte(`{"version":2}`)

	if err := s.WritePipelineConfig(ctx, "test-pipe", config1); err != nil {
		t.Fatalf("WritePipelineConfig(v1) error = %v", err)
	}
	if err := s.WritePipelineConfig(ctx, "test-pipe", config2); err != nil {
		t.Fatalf("WritePipelineConfig(v2) error = %v", err)
	}

	got, err := s.ReadPipelineConfig(ctx, "test-pipe")
	if err != nil {
		t.Fatalf("ReadPipelineConfig() error = %v", err)
	}
	if string(got) != string(config2) {
		t.Errorf("ReadPipelineConfig() = %q, want %q", got, config2)
	}
}

func TestSQLiteState_ReadPipelineConfig_NotFound(t *testing.T) {
	t.Parallel()
	s := newTestSQLiteState(t)
	ctx := context.Background()

	got, err := s.ReadPipelineConfig(ctx, "nonexistent-pipe")
	if err != nil {
		t.Fatalf("ReadPipelineConfig() error = %v, want nil", err)
	}
	if got != nil {
		t.Errorf("ReadPipelineConfig() = %q, want nil", got)
	}
}

func TestSQLiteState_DeleteByPrefix(t *testing.T) {
	t.Parallel()
	s := newTestSQLiteState(t)
	ctx := context.Background()

	// Write sensors for two different pipeline prefixes.
	sensorA := adapter.SensorData{
		Pipeline:    "suite-001-pipe-a",
		Key:         "sensor-1",
		Status:      types.SensorStatusReady,
		LastUpdated: time.Now().UTC().Truncate(time.Nanosecond),
	}
	sensorB := adapter.SensorData{
		Pipeline:    "other-pipe",
		Key:         "sensor-2",
		Status:      types.SensorStatusReady,
		LastUpdated: time.Now().UTC().Truncate(time.Nanosecond),
	}
	if err := s.WriteSensor(ctx, sensorA.Pipeline, sensorA.Key, sensorA); err != nil {
		t.Fatalf("WriteSensor(suite-001) error = %v", err)
	}
	if err := s.WriteSensor(ctx, sensorB.Pipeline, sensorB.Key, sensorB); err != nil {
		t.Fatalf("WriteSensor(other) error = %v", err)
	}

	// Write trigger statuses for both prefixes.
	trigA := adapter.TriggerKey{Pipeline: "suite-001-pipe-a", Schedule: "daily", Date: "2025-01-01"}
	trigB := adapter.TriggerKey{Pipeline: "other-pipe", Schedule: "daily", Date: "2025-01-01"}
	if err := s.WriteTriggerStatus(ctx, trigA, "fired"); err != nil {
		t.Fatalf("WriteTriggerStatus(suite-001) error = %v", err)
	}
	if err := s.WriteTriggerStatus(ctx, trigB, "fired"); err != nil {
		t.Fatalf("WriteTriggerStatus(other) error = %v", err)
	}

	// Write pipeline configs for both prefixes.
	if err := s.WritePipelineConfig(ctx, "suite-001-pipe-a", []byte(`{"id":"a"}`)); err != nil {
		t.Fatalf("WritePipelineConfig(suite-001) error = %v", err)
	}
	if err := s.WritePipelineConfig(ctx, "other-pipe", []byte(`{"id":"b"}`)); err != nil {
		t.Fatalf("WritePipelineConfig(other) error = %v", err)
	}

	// Write reruns for both prefixes.
	if err := s.WriteRerun(ctx, "suite-001-pipe-a", "daily", "2025-01-01", "flaky"); err != nil {
		t.Fatalf("WriteRerun(suite-001) error = %v", err)
	}
	if err := s.WriteRerun(ctx, "other-pipe", "daily", "2025-01-01", "flaky"); err != nil {
		t.Fatalf("WriteRerun(other) error = %v", err)
	}

	// Delete everything with prefix "suite-001-".
	if err := s.DeleteByPrefix(ctx, "suite-001-"); err != nil {
		t.Fatalf("DeleteByPrefix() error = %v", err)
	}

	// suite-001 sensor should be gone.
	gotA, err := s.ReadSensor(ctx, "suite-001-pipe-a", "sensor-1")
	if err != nil {
		t.Fatalf("ReadSensor(suite-001) after delete error = %v", err)
	}
	if gotA.Key != "" {
		t.Errorf("ReadSensor(suite-001) after delete: got Key=%q, want empty (deleted)", gotA.Key)
	}

	// other-pipe sensor should remain.
	gotB, err := s.ReadSensor(ctx, "other-pipe", "sensor-2")
	if err != nil {
		t.Fatalf("ReadSensor(other) after delete error = %v", err)
	}
	if gotB.Key != "sensor-2" {
		t.Errorf("ReadSensor(other) after delete: got Key=%q, want %q", gotB.Key, "sensor-2")
	}

	// suite-001 trigger should be gone.
	trigStatus, err := s.ReadTriggerStatus(ctx, trigA)
	if err != nil {
		t.Fatalf("ReadTriggerStatus(suite-001) after delete error = %v", err)
	}
	if trigStatus != "" {
		t.Errorf("ReadTriggerStatus(suite-001) after delete = %q, want empty", trigStatus)
	}

	// other-pipe trigger should remain.
	trigStatus, err = s.ReadTriggerStatus(ctx, trigB)
	if err != nil {
		t.Fatalf("ReadTriggerStatus(other) after delete error = %v", err)
	}
	if trigStatus != "fired" {
		t.Errorf("ReadTriggerStatus(other) after delete = %q, want %q", trigStatus, "fired")
	}

	// suite-001 pipeline config should be gone.
	cfgA, err := s.ReadPipelineConfig(ctx, "suite-001-pipe-a")
	if err != nil {
		t.Fatalf("ReadPipelineConfig(suite-001) after delete error = %v", err)
	}
	if cfgA != nil {
		t.Errorf("ReadPipelineConfig(suite-001) after delete = %q, want nil", cfgA)
	}

	// other-pipe pipeline config should remain.
	cfgB, err := s.ReadPipelineConfig(ctx, "other-pipe")
	if err != nil {
		t.Fatalf("ReadPipelineConfig(other) after delete error = %v", err)
	}
	if string(cfgB) != `{"id":"b"}` {
		t.Errorf("ReadPipelineConfig(other) after delete = %q, want %q", cfgB, `{"id":"b"}`)
	}

	// suite-001 reruns should be gone.
	countA, err := s.CountReruns(ctx, "suite-001-pipe-a", "daily", "2025-01-01")
	if err != nil {
		t.Fatalf("CountReruns(suite-001) after delete error = %v", err)
	}
	if countA != 0 {
		t.Errorf("CountReruns(suite-001) after delete = %d, want 0", countA)
	}

	// other-pipe reruns should remain.
	countB, err := s.CountReruns(ctx, "other-pipe", "daily", "2025-01-01")
	if err != nil {
		t.Fatalf("CountReruns(other) after delete error = %v", err)
	}
	if countB != 1 {
		t.Errorf("CountReruns(other) after delete = %d, want 1", countB)
	}
}

func TestSQLiteState_WriteRerun_CountReruns(t *testing.T) {
	t.Parallel()
	s := newTestSQLiteState(t)
	ctx := context.Background()

	// No reruns initially.
	count, err := s.CountReruns(ctx, "pipe-a", "daily", "2025-07-01")
	if err != nil {
		t.Fatalf("CountReruns() initial error = %v", err)
	}
	if count != 0 {
		t.Errorf("CountReruns() initial = %d, want 0", count)
	}

	// Write two reruns.
	if err := s.WriteRerun(ctx, "pipe-a", "daily", "2025-07-01", "flaky test"); err != nil {
		t.Fatalf("WriteRerun(1) error = %v", err)
	}
	if err := s.WriteRerun(ctx, "pipe-a", "daily", "2025-07-01", "timeout"); err != nil {
		t.Fatalf("WriteRerun(2) error = %v", err)
	}

	count, err = s.CountReruns(ctx, "pipe-a", "daily", "2025-07-01")
	if err != nil {
		t.Fatalf("CountReruns() after writes error = %v", err)
	}
	if count != 2 {
		t.Errorf("CountReruns() after writes = %d, want 2", count)
	}

	// Different date should have zero reruns.
	count, err = s.CountReruns(ctx, "pipe-a", "daily", "2025-07-02")
	if err != nil {
		t.Fatalf("CountReruns() different date error = %v", err)
	}
	if count != 0 {
		t.Errorf("CountReruns() different date = %d, want 0", count)
	}
}

func TestSQLiteState_ReadJobEvents(t *testing.T) {
	t.Parallel()
	s := newTestSQLiteState(t)
	ctx := context.Background()

	// Insert job events directly via the database for test setup,
	// since there is no WriteJobEvent method on the interface.
	// We access the DB through WriteSensor to ensure tables are created,
	// then use the exported DB method or raw SQL.
	// Since SQLiteState.db is unexported, we insert via ExecSQL helper or
	// by using the WriteRerun method to ensure the DB is initialized, then
	// we can test ReadJobEvents if we can insert test data.
	//
	// Actually, the job_events table is created in createTables. We need
	// a way to insert test data. Let's use a helper approach.

	// We'll test that ReadJobEvents returns empty for no rows.
	got, err := s.ReadJobEvents(ctx, "pipe-a", "daily", "2025-07-01")
	if err != nil {
		t.Fatalf("ReadJobEvents() empty error = %v", err)
	}
	if got == nil {
		t.Fatal("ReadJobEvents() returned nil, want non-nil empty slice")
	}
	if len(got) != 0 {
		t.Errorf("ReadJobEvents() returned %d events, want 0", len(got))
	}
}

func TestSQLiteState_ReadJobEvents_WithData(t *testing.T) {
	t.Parallel()
	// Since job_events has no public write method, we use a helper
	// that creates an SQLiteState and manually inserts rows.
	s, err := local.NewSQLiteState(":memory:")
	if err != nil {
		t.Fatalf("NewSQLiteState error = %v", err)
	}
	t.Cleanup(func() { s.Close() })
	ctx := context.Background()

	// Insert test rows using helper (fatals on error).
	insertJobEvent(t, s, "pipe-a", "daily", "2025-07-01", "started", "run-1", "2025-07-01T10:00:00Z")
	insertJobEvent(t, s, "pipe-a", "daily", "2025-07-01", "completed", "run-1", "2025-07-01T10:05:00Z")
	insertJobEvent(t, s, "pipe-a", "daily", "2025-07-01", "failed", "run-2", "2025-07-01T10:10:00Z")
	// Different pipeline — should not appear.
	insertJobEvent(t, s, "pipe-b", "daily", "2025-07-01", "started", "run-3", "2025-07-01T11:00:00Z")

	got, err := s.ReadJobEvents(ctx, "pipe-a", "daily", "2025-07-01")
	if err != nil {
		t.Fatalf("ReadJobEvents() error = %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("ReadJobEvents() returned %d events, want 3", len(got))
	}

	// Should be ordered by timestamp DESC.
	if got[0].Event != "failed" {
		t.Errorf("first event = %q, want %q (most recent)", got[0].Event, "failed")
	}
	if got[0].RunID != "run-2" {
		t.Errorf("first event RunID = %q, want %q", got[0].RunID, "run-2")
	}
	if got[1].Event != "completed" {
		t.Errorf("second event = %q, want %q", got[1].Event, "completed")
	}
	if got[2].Event != "started" {
		t.Errorf("third event = %q, want %q (oldest)", got[2].Event, "started")
	}

	// Verify fields on first event.
	if got[0].Pipeline != "pipe-a" {
		t.Errorf("first event Pipeline = %q, want %q", got[0].Pipeline, "pipe-a")
	}
	if got[0].Schedule != "daily" {
		t.Errorf("first event Schedule = %q, want %q", got[0].Schedule, "daily")
	}
	if got[0].Date != "2025-07-01" {
		t.Errorf("first event Date = %q, want %q", got[0].Date, "2025-07-01")
	}
}
