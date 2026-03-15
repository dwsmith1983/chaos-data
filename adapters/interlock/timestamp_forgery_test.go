package interlock_test

import (
	"context"
	"testing"
	"time"

	"github.com/dwsmith1983/chaos-data/adapters/interlock"
	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

func TestInterlockTimestampForgery_Type(t *testing.T) {
	t.Parallel()

	store := newMockStateStore()
	cfg := interlock.Config{PipelinePrefix: "prod/"}
	m := interlock.NewInterlockTimestampForgery(store, cfg)

	if got := m.Type(); got != "interlock-timestamp-forgery" {
		t.Errorf("Type() = %q, want %q", got, "interlock-timestamp-forgery")
	}
}

func TestInterlockTimestampForgery_Apply_EnrichesPipeline(t *testing.T) {
	t.Parallel()

	store := newMockStateStore()
	store.sensors["prod/my-pipeline/clock-sensor"] = adapter.SensorData{
		Pipeline:    "prod/my-pipeline",
		Key:         "clock-sensor",
		Status:      types.SensorStatusReady,
		LastUpdated: time.Now(),
	}

	cfg := interlock.Config{PipelinePrefix: "prod/"}
	m := interlock.NewInterlockTimestampForgery(store, cfg)

	params := map[string]string{
		"sensor_key":          "clock-sensor",
		"pipeline":            "my-pipeline",
		"last_updated_offset": "-6h",
	}

	obj := types.DataObject{Key: "test-obj"}
	transport := newMockTransport()

	rec, err := m.Apply(context.Background(), obj, transport, params)
	if err != nil {
		t.Fatalf("Apply() error = %v, want nil", err)
	}
	if !rec.Applied {
		t.Fatal("Apply() record.Applied = false, want true")
	}

	// Verify pipeline was enriched with prefix.
	calls := store.getStateCalls()
	if len(calls) == 0 {
		t.Fatal("expected state store calls, got none")
	}
	if calls[0].Pipeline != "prod/my-pipeline" {
		t.Errorf("ReadSensor pipeline = %q, want %q", calls[0].Pipeline, "prod/my-pipeline")
	}
}

func TestInterlockTimestampForgery_Apply_NoPrefixWhenEmpty(t *testing.T) {
	t.Parallel()

	store := newMockStateStore()
	store.sensors["my-pipeline/clock-sensor"] = adapter.SensorData{
		Pipeline:    "my-pipeline",
		Key:         "clock-sensor",
		Status:      types.SensorStatusReady,
		LastUpdated: time.Now(),
	}

	cfg := interlock.Config{PipelinePrefix: ""}
	m := interlock.NewInterlockTimestampForgery(store, cfg)

	params := map[string]string{
		"sensor_key":          "clock-sensor",
		"pipeline":            "my-pipeline",
		"last_updated_offset": "-1h",
	}

	obj := types.DataObject{Key: "test-obj"}
	transport := newMockTransport()

	rec, err := m.Apply(context.Background(), obj, transport, params)
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
	if calls[0].Pipeline != "my-pipeline" {
		t.Errorf("ReadSensor pipeline = %q, want %q (no prefix)", calls[0].Pipeline, "my-pipeline")
	}
}

func TestInterlockTimestampForgery_Apply_RecordMutationName(t *testing.T) {
	t.Parallel()

	store := newMockStateStore()
	store.sensors["prod/pipe/ts-sensor"] = adapter.SensorData{
		Pipeline:    "prod/pipe",
		Key:         "ts-sensor",
		Status:      types.SensorStatusReady,
		LastUpdated: time.Now(),
	}

	cfg := interlock.Config{PipelinePrefix: "prod/"}
	m := interlock.NewInterlockTimestampForgery(store, cfg)

	params := map[string]string{
		"sensor_key":               "ts-sensor",
		"pipeline":                 "pipe",
		"payload_timestamp_offset": "+2h",
	}

	obj := types.DataObject{Key: "test-obj"}
	transport := newMockTransport()

	rec, err := m.Apply(context.Background(), obj, transport, params)
	if err != nil {
		t.Fatalf("Apply() error = %v, want nil", err)
	}
	if rec.Mutation != "interlock-timestamp-forgery" {
		t.Errorf("record.Mutation = %q, want %q", rec.Mutation, "interlock-timestamp-forgery")
	}
}
