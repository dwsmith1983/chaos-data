package interlock_test

import (
	"context"
	"testing"

	"github.com/dwsmith1983/chaos-data/adapters/interlock"
	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

func TestInterlockPhantomTrigger_Type(t *testing.T) {
	t.Parallel()

	store := newMockStateStore()
	cfg := interlock.Config{PipelinePrefix: "prod/", DefaultSchedule: "daily"}
	m := interlock.NewInterlockPhantomTrigger(store, cfg)

	if got := m.Type(); got != "interlock-phantom-trigger" {
		t.Errorf("Type() = %q, want %q", got, "interlock-phantom-trigger")
	}
}

func TestInterlockPhantomTrigger_Apply_EnrichesParams(t *testing.T) {
	t.Parallel()

	store := newMockStateStore()
	cfg := interlock.Config{PipelinePrefix: "prod/", DefaultSchedule: "daily"}
	m := interlock.NewInterlockPhantomTrigger(store, cfg)

	params := map[string]string{
		"pipeline": "etl-daily",
		"date":     "2026-03-14",
	}

	obj := types.DataObject{Key: "test-obj"}
	transport := newMockTransport()

	rec, err := m.Apply(context.Background(), obj, transport, params, adapter.NewWallClock())
	if err != nil {
		t.Fatalf("Apply() error = %v, want nil", err)
	}
	if !rec.Applied {
		t.Fatal("Apply() record.Applied = false, want true")
	}

	// Verify the pipeline was enriched with the prefix.
	calls := store.getStateCalls()
	if len(calls) == 0 {
		t.Fatal("expected state store calls, got none")
	}
	if calls[0].Trigger.Pipeline != "prod/etl-daily" {
		t.Errorf("WriteTriggerStatus pipeline = %q, want %q", calls[0].Trigger.Pipeline, "prod/etl-daily")
	}
	// Verify the default schedule was injected.
	if calls[0].Trigger.Schedule != "daily" {
		t.Errorf("WriteTriggerStatus schedule = %q, want %q", calls[0].Trigger.Schedule, "daily")
	}
}

func TestInterlockPhantomTrigger_Apply_ExplicitScheduleOverridesDefault(t *testing.T) {
	t.Parallel()

	store := newMockStateStore()
	cfg := interlock.Config{PipelinePrefix: "prod/", DefaultSchedule: "daily"}
	m := interlock.NewInterlockPhantomTrigger(store, cfg)

	params := map[string]string{
		"pipeline": "etl-daily",
		"schedule": "hourly",
		"date":     "2026-03-14",
	}

	obj := types.DataObject{Key: "test-obj"}
	transport := newMockTransport()

	rec, err := m.Apply(context.Background(), obj, transport, params, adapter.NewWallClock())
	if err != nil {
		t.Fatalf("Apply() error = %v, want nil", err)
	}
	if !rec.Applied {
		t.Fatal("Apply() record.Applied = false, want true")
	}

	calls := store.getStateCalls()
	if len(calls) == 0 {
		t.Fatal("expected state store calls, got none")
	}
	if calls[0].Trigger.Schedule != "hourly" {
		t.Errorf("WriteTriggerStatus schedule = %q, want %q", calls[0].Trigger.Schedule, "hourly")
	}
}

func TestInterlockPhantomTrigger_Apply_ErrorPropagation(t *testing.T) {
	t.Parallel()

	store := newMockStateStore()
	store.WriteTriggerStatusErr = true

	cfg := interlock.Config{PipelinePrefix: "prod/", DefaultSchedule: "daily"}
	m := interlock.NewInterlockPhantomTrigger(store, cfg)

	params := map[string]string{
		"pipeline": "etl-daily",
		"schedule": "daily",
		"date":     "2026-03-14",
	}

	obj := types.DataObject{Key: "test-obj"}
	transport := newMockTransport()

	_, err := m.Apply(context.Background(), obj, transport, params, adapter.NewWallClock())
	if err == nil {
		t.Fatal("Apply() error = nil, want error")
	}
}

func TestInterlockJobKill_Type(t *testing.T) {
	t.Parallel()

	store := newMockStateStore()
	cfg := interlock.Config{PipelinePrefix: "prod/", DefaultSchedule: "daily"}
	m := interlock.NewInterlockJobKill(store, cfg)

	if got := m.Type(); got != "interlock-job-kill" {
		t.Errorf("Type() = %q, want %q", got, "interlock-job-kill")
	}
}

func TestInterlockJobKill_Apply_EnrichesParams(t *testing.T) {
	t.Parallel()

	store := newMockStateStore()
	cfg := interlock.Config{PipelinePrefix: "staging/", DefaultSchedule: "hourly"}
	m := interlock.NewInterlockJobKill(store, cfg)

	params := map[string]string{
		"pipeline": "data-ingest",
		"date":     "2026-03-14",
	}

	obj := types.DataObject{Key: "test-obj"}
	transport := newMockTransport()

	rec, err := m.Apply(context.Background(), obj, transport, params, adapter.NewWallClock())
	if err != nil {
		t.Fatalf("Apply() error = %v, want nil", err)
	}
	if !rec.Applied {
		t.Fatal("Apply() record.Applied = false, want true")
	}

	calls := store.getStateCalls()
	if len(calls) == 0 {
		t.Fatal("expected state store calls, got none")
	}
	if calls[0].Trigger.Pipeline != "staging/data-ingest" {
		t.Errorf("WriteTriggerStatus pipeline = %q, want %q", calls[0].Trigger.Pipeline, "staging/data-ingest")
	}
	if calls[0].Trigger.Schedule != "hourly" {
		t.Errorf("WriteTriggerStatus schedule = %q, want %q", calls[0].Trigger.Schedule, "hourly")
	}
}

func TestInterlockJobKill_Apply_ErrorPropagation(t *testing.T) {
	t.Parallel()

	store := newMockStateStore()
	store.WriteTriggerStatusErr = true

	cfg := interlock.Config{PipelinePrefix: "prod/", DefaultSchedule: "daily"}
	m := interlock.NewInterlockJobKill(store, cfg)

	params := map[string]string{
		"pipeline": "data-ingest",
		"schedule": "daily",
		"date":     "2026-03-14",
	}

	obj := types.DataObject{Key: "test-obj"}
	transport := newMockTransport()

	_, err := m.Apply(context.Background(), obj, transport, params, adapter.NewWallClock())
	if err == nil {
		t.Fatal("Apply() error = nil, want error")
	}
}

func TestInterlockTriggerTimeout_Type(t *testing.T) {
	t.Parallel()

	store := newMockStateStore()
	cfg := interlock.Config{PipelinePrefix: "prod/", DefaultSchedule: "daily"}
	m := interlock.NewInterlockTriggerTimeout(store, cfg)

	if got := m.Type(); got != "interlock-trigger-timeout" {
		t.Errorf("Type() = %q, want %q", got, "interlock-trigger-timeout")
	}
}

func TestInterlockTriggerTimeout_Apply_EnrichesParams(t *testing.T) {
	t.Parallel()

	store := newMockStateStore()
	cfg := interlock.Config{PipelinePrefix: "prod/", DefaultSchedule: "daily"}
	m := interlock.NewInterlockTriggerTimeout(store, cfg)

	params := map[string]string{
		"pipeline": "slow-pipeline",
		"date":     "2026-03-14",
	}

	obj := types.DataObject{Key: "test-obj"}
	transport := newMockTransport()

	rec, err := m.Apply(context.Background(), obj, transport, params, adapter.NewWallClock())
	if err != nil {
		t.Fatalf("Apply() error = %v, want nil", err)
	}
	if !rec.Applied {
		t.Fatal("Apply() record.Applied = false, want true")
	}

	calls := store.getStateCalls()
	if len(calls) == 0 {
		t.Fatal("expected state store calls, got none")
	}
	if calls[0].Trigger.Pipeline != "prod/slow-pipeline" {
		t.Errorf("WriteTriggerStatus pipeline = %q, want %q", calls[0].Trigger.Pipeline, "prod/slow-pipeline")
	}
}

func TestInterlockTriggerTimeout_Apply_ErrorPropagation(t *testing.T) {
	t.Parallel()

	store := newMockStateStore()
	store.WriteTriggerStatusErr = true

	cfg := interlock.Config{PipelinePrefix: "prod/", DefaultSchedule: "daily"}
	m := interlock.NewInterlockTriggerTimeout(store, cfg)

	params := map[string]string{
		"pipeline": "slow-pipeline",
		"schedule": "daily",
		"date":     "2026-03-14",
	}

	obj := types.DataObject{Key: "test-obj"}
	transport := newMockTransport()

	_, err := m.Apply(context.Background(), obj, transport, params, adapter.NewWallClock())
	if err == nil {
		t.Fatal("Apply() error = nil, want error")
	}
}

func TestInterlockFalseSuccess_Type(t *testing.T) {
	t.Parallel()

	store := newMockStateStore()
	cfg := interlock.Config{PipelinePrefix: "prod/", DefaultSchedule: "daily"}
	m := interlock.NewInterlockFalseSuccess(store, cfg)

	if got := m.Type(); got != "interlock-false-success" {
		t.Errorf("Type() = %q, want %q", got, "interlock-false-success")
	}
}

func TestInterlockFalseSuccess_Apply_EnrichesParams(t *testing.T) {
	t.Parallel()

	store := newMockStateStore()
	cfg := interlock.Config{PipelinePrefix: "prod/", DefaultSchedule: "daily"}
	m := interlock.NewInterlockFalseSuccess(store, cfg)

	params := map[string]string{
		"pipeline": "flaky-job",
		"date":     "2026-03-14",
	}

	obj := types.DataObject{Key: "test-obj"}
	transport := newMockTransport()

	rec, err := m.Apply(context.Background(), obj, transport, params, adapter.NewWallClock())
	if err != nil {
		t.Fatalf("Apply() error = %v, want nil", err)
	}
	if !rec.Applied {
		t.Fatal("Apply() record.Applied = false, want true")
	}

	calls := store.getStateCalls()
	if len(calls) == 0 {
		t.Fatal("expected state store calls, got none")
	}
	if calls[0].Trigger.Pipeline != "prod/flaky-job" {
		t.Errorf("WriteTriggerStatus pipeline = %q, want %q", calls[0].Trigger.Pipeline, "prod/flaky-job")
	}
}

func TestInterlockFalseSuccess_Apply_ErrorPropagation(t *testing.T) {
	t.Parallel()

	store := newMockStateStore()
	store.WriteTriggerStatusErr = true

	cfg := interlock.Config{PipelinePrefix: "prod/", DefaultSchedule: "daily"}
	m := interlock.NewInterlockFalseSuccess(store, cfg)

	params := map[string]string{
		"pipeline": "flaky-job",
		"schedule": "daily",
		"date":     "2026-03-14",
	}

	obj := types.DataObject{Key: "test-obj"}
	transport := newMockTransport()

	_, err := m.Apply(context.Background(), obj, transport, params, adapter.NewWallClock())
	if err == nil {
		t.Fatal("Apply() error = nil, want error")
	}
}

func TestInterlockPhantomTrigger_Apply_RecordMutationName(t *testing.T) {
	t.Parallel()

	store := newMockStateStore()
	cfg := interlock.Config{PipelinePrefix: "prod/", DefaultSchedule: "daily"}
	m := interlock.NewInterlockPhantomTrigger(store, cfg)

	params := map[string]string{
		"pipeline": "etl-daily",
		"schedule": "hourly",
		"date":     "2026-03-14",
	}

	obj := types.DataObject{Key: "test-obj"}
	transport := newMockTransport()

	rec, err := m.Apply(context.Background(), obj, transport, params, adapter.NewWallClock())
	if err != nil {
		t.Fatalf("Apply() error = %v, want nil", err)
	}
	if rec.Mutation != "interlock-phantom-trigger" {
		t.Errorf("rec.Mutation = %q, want %q", rec.Mutation, "interlock-phantom-trigger")
	}
}

func TestInterlockJobKill_Apply_RecordMutationName(t *testing.T) {
	t.Parallel()

	store := newMockStateStore()
	cfg := interlock.Config{PipelinePrefix: "prod/", DefaultSchedule: "daily"}
	m := interlock.NewInterlockJobKill(store, cfg)

	params := map[string]string{
		"pipeline": "etl-daily",
		"schedule": "hourly",
		"date":     "2026-03-14",
	}

	obj := types.DataObject{Key: "test-obj"}
	transport := newMockTransport()

	rec, err := m.Apply(context.Background(), obj, transport, params, adapter.NewWallClock())
	if err != nil {
		t.Fatalf("Apply() error = %v, want nil", err)
	}
	if rec.Mutation != "interlock-job-kill" {
		t.Errorf("rec.Mutation = %q, want %q", rec.Mutation, "interlock-job-kill")
	}
}

func TestInterlockTriggerTimeout_Apply_RecordMutationName(t *testing.T) {
	t.Parallel()

	store := newMockStateStore()
	cfg := interlock.Config{PipelinePrefix: "prod/", DefaultSchedule: "daily"}
	m := interlock.NewInterlockTriggerTimeout(store, cfg)

	params := map[string]string{
		"pipeline": "etl-daily",
		"schedule": "hourly",
		"date":     "2026-03-14",
	}

	obj := types.DataObject{Key: "test-obj"}
	transport := newMockTransport()

	rec, err := m.Apply(context.Background(), obj, transport, params, adapter.NewWallClock())
	if err != nil {
		t.Fatalf("Apply() error = %v, want nil", err)
	}
	if rec.Mutation != "interlock-trigger-timeout" {
		t.Errorf("rec.Mutation = %q, want %q", rec.Mutation, "interlock-trigger-timeout")
	}
}

func TestInterlockFalseSuccess_Apply_RecordMutationName(t *testing.T) {
	t.Parallel()

	store := newMockStateStore()
	cfg := interlock.Config{PipelinePrefix: "prod/", DefaultSchedule: "daily"}
	m := interlock.NewInterlockFalseSuccess(store, cfg)

	params := map[string]string{
		"pipeline": "etl-daily",
		"schedule": "hourly",
		"date":     "2026-03-14",
	}

	obj := types.DataObject{Key: "test-obj"}
	transport := newMockTransport()

	rec, err := m.Apply(context.Background(), obj, transport, params, adapter.NewWallClock())
	if err != nil {
		t.Fatalf("Apply() error = %v, want nil", err)
	}
	if rec.Mutation != "interlock-false-success" {
		t.Errorf("rec.Mutation = %q, want %q", rec.Mutation, "interlock-false-success")
	}
}

func TestInterlockJobKill_Apply_ExplicitScheduleOverridesDefault(t *testing.T) {
	t.Parallel()

	store := newMockStateStore()
	cfg := interlock.Config{PipelinePrefix: "prod/", DefaultSchedule: "daily"}
	m := interlock.NewInterlockJobKill(store, cfg)

	params := map[string]string{
		"pipeline": "data-ingest",
		"schedule": "weekly",
		"date":     "2026-03-14",
	}

	obj := types.DataObject{Key: "test-obj"}
	transport := newMockTransport()

	_, err := m.Apply(context.Background(), obj, transport, params, adapter.NewWallClock())
	if err != nil {
		t.Fatalf("Apply() error = %v, want nil", err)
	}

	calls := store.getStateCalls()
	if len(calls) == 0 {
		t.Fatal("expected state store calls, got none")
	}
	if calls[0].Trigger.Schedule != "weekly" {
		t.Errorf("schedule = %q, want %q (explicit should be preserved)", calls[0].Trigger.Schedule, "weekly")
	}
}

func TestInterlockTriggerTimeout_Apply_ExplicitScheduleOverridesDefault(t *testing.T) {
	t.Parallel()

	store := newMockStateStore()
	cfg := interlock.Config{PipelinePrefix: "prod/", DefaultSchedule: "daily"}
	m := interlock.NewInterlockTriggerTimeout(store, cfg)

	params := map[string]string{
		"pipeline": "slow-pipeline",
		"schedule": "bi-weekly",
		"date":     "2026-03-14",
	}

	obj := types.DataObject{Key: "test-obj"}
	transport := newMockTransport()

	_, err := m.Apply(context.Background(), obj, transport, params, adapter.NewWallClock())
	if err != nil {
		t.Fatalf("Apply() error = %v, want nil", err)
	}

	calls := store.getStateCalls()
	if len(calls) == 0 {
		t.Fatal("expected state store calls, got none")
	}
	if calls[0].Trigger.Schedule != "bi-weekly" {
		t.Errorf("schedule = %q, want %q (explicit should be preserved)", calls[0].Trigger.Schedule, "bi-weekly")
	}
}

func TestInterlockFalseSuccess_Apply_ExplicitScheduleOverridesDefault(t *testing.T) {
	t.Parallel()

	store := newMockStateStore()
	cfg := interlock.Config{PipelinePrefix: "prod/", DefaultSchedule: "daily"}
	m := interlock.NewInterlockFalseSuccess(store, cfg)

	params := map[string]string{
		"pipeline": "flaky-job",
		"schedule": "every-6h",
		"date":     "2026-03-14",
	}

	obj := types.DataObject{Key: "test-obj"}
	transport := newMockTransport()

	_, err := m.Apply(context.Background(), obj, transport, params, adapter.NewWallClock())
	if err != nil {
		t.Fatalf("Apply() error = %v, want nil", err)
	}

	calls := store.getStateCalls()
	if len(calls) == 0 {
		t.Fatal("expected state store calls, got none")
	}
	if calls[0].Trigger.Schedule != "every-6h" {
		t.Errorf("schedule = %q, want %q (explicit should be preserved)", calls[0].Trigger.Schedule, "every-6h")
	}
}

func TestInterlockPhantomTrigger_Apply_EmptyScheduleUsesDefault(t *testing.T) {
	t.Parallel()

	store := newMockStateStore()
	cfg := interlock.Config{PipelinePrefix: "prod/", DefaultSchedule: "daily"}
	m := interlock.NewInterlockPhantomTrigger(store, cfg)

	params := map[string]string{
		"pipeline": "etl-daily",
		"schedule": "", // present but empty
		"date":     "2026-03-14",
	}

	obj := types.DataObject{Key: "test-obj"}
	transport := newMockTransport()

	_, err := m.Apply(context.Background(), obj, transport, params, adapter.NewWallClock())
	if err != nil {
		t.Fatalf("Apply() error = %v, want nil", err)
	}

	calls := store.getStateCalls()
	if len(calls) == 0 {
		t.Fatal("expected state store calls, got none")
	}
	if calls[0].Trigger.Schedule != "daily" {
		t.Errorf("schedule = %q, want %q (empty should use default)", calls[0].Trigger.Schedule, "daily")
	}
}

func TestInterlockJobKill_Apply_EmptyScheduleUsesDefault(t *testing.T) {
	t.Parallel()

	store := newMockStateStore()
	cfg := interlock.Config{PipelinePrefix: "prod/", DefaultSchedule: "hourly"}
	m := interlock.NewInterlockJobKill(store, cfg)

	params := map[string]string{
		"pipeline": "data-ingest",
		"schedule": "", // present but empty
		"date":     "2026-03-14",
	}

	obj := types.DataObject{Key: "test-obj"}
	transport := newMockTransport()

	_, err := m.Apply(context.Background(), obj, transport, params, adapter.NewWallClock())
	if err != nil {
		t.Fatalf("Apply() error = %v, want nil", err)
	}

	calls := store.getStateCalls()
	if len(calls) == 0 {
		t.Fatal("expected state store calls, got none")
	}
	if calls[0].Trigger.Schedule != "hourly" {
		t.Errorf("schedule = %q, want %q (empty should use default)", calls[0].Trigger.Schedule, "hourly")
	}
}

func TestInterlockTrigger_DelegatesWriteStatus(t *testing.T) {
	t.Parallel()

	store := newMockStateStore()
	cfg := interlock.Config{PipelinePrefix: "", DefaultSchedule: "daily"}

	tests := []struct {
		name       string
		wrapper    interface {
			Apply(context.Context, types.DataObject, interface{ Hold(context.Context, string, interface{}) error }, map[string]string) (types.MutationRecord, error)
		}
		wantStatus string
	}{
		// We test delegation by verifying the correct status is written
		// through the mock state store.
	}
	_ = tests // placeholder — delegation is verified through the EnrichesParams tests above

	// Verify phantom-trigger writes "triggered"
	m1 := interlock.NewInterlockPhantomTrigger(store, cfg)
	rec1, err := m1.Apply(context.Background(), types.DataObject{Key: "k"}, newMockTransport(), map[string]string{
		"pipeline": "p", "schedule": "s", "date": "d",
	}, adapter.NewWallClock())
	if err != nil {
		t.Fatalf("PhantomTrigger Apply error = %v", err)
	}
	if !rec1.Applied {
		t.Error("PhantomTrigger rec.Applied = false")
	}
	found := false
	for _, c := range store.getStateCalls() {
		if c.Method == "WriteTriggerStatus" && c.Status == "triggered" {
			found = true
		}
	}
	if !found {
		t.Error("PhantomTrigger did not write 'triggered' status")
	}
}

func TestInterlockTrigger_NoPrefixWhenEmpty(t *testing.T) {
	t.Parallel()

	store := newMockStateStore()
	cfg := interlock.Config{PipelinePrefix: "", DefaultSchedule: "daily"}
	m := interlock.NewInterlockPhantomTrigger(store, cfg)

	params := map[string]string{
		"pipeline": "etl-daily",
		"date":     "2026-03-14",
	}

	obj := types.DataObject{Key: "test-obj"}
	transport := newMockTransport()

	_, err := m.Apply(context.Background(), obj, transport, params, adapter.NewWallClock())
	if err != nil {
		t.Fatalf("Apply() error = %v, want nil", err)
	}

	calls := store.getStateCalls()
	if len(calls) == 0 {
		t.Fatal("expected state store calls, got none")
	}
	if calls[0].Trigger.Pipeline != "etl-daily" {
		t.Errorf("WriteTriggerStatus pipeline = %q, want %q (no prefix)", calls[0].Trigger.Pipeline, "etl-daily")
	}
}
