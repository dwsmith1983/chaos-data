package interlock_test

import (
	"context"
	"testing"

	"github.com/dwsmith1983/chaos-data/adapters/interlock"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

func TestInterlockSensorFlapping_Type(t *testing.T) {
	t.Parallel()

	store := newMockStateStore()
	cfg := interlock.Config{PipelinePrefix: "prod/"}
	m := interlock.NewInterlockSensorFlapping(store, cfg)

	if got := m.Type(); got != "interlock-sensor-flapping" {
		t.Errorf("Type() = %q, want %q", got, "interlock-sensor-flapping")
	}
}

func TestInterlockSensorFlapping_Apply_EnrichesPipeline(t *testing.T) {
	t.Parallel()

	store := newMockStateStore()
	cfg := interlock.Config{PipelinePrefix: "prod/"}
	m := interlock.NewInterlockSensorFlapping(store, cfg)

	params := map[string]string{
		"sensor_key": "flap-key",
		"pipeline":   "my-pipeline",
		"flap_count": "2",
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

	// Verify the pipeline was enriched with the prefix.
	calls := store.getStateCalls()
	if len(calls) == 0 {
		t.Fatal("expected state store calls, got none")
	}
	if calls[0].Pipeline != "prod/my-pipeline" {
		t.Errorf("WriteSensor pipeline = %q, want %q", calls[0].Pipeline, "prod/my-pipeline")
	}
}

func TestInterlockSensorFlapping_Apply_NoPrefixWhenEmpty(t *testing.T) {
	t.Parallel()

	store := newMockStateStore()
	cfg := interlock.Config{PipelinePrefix: ""}
	m := interlock.NewInterlockSensorFlapping(store, cfg)

	params := map[string]string{
		"sensor_key": "flap-key",
		"pipeline":   "my-pipeline",
		"flap_count": "2",
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
		t.Errorf("WriteSensor pipeline = %q, want %q (no prefix)", calls[0].Pipeline, "my-pipeline")
	}
}

func TestInterlockSensorFlapping_Apply_RecordMutationName(t *testing.T) {
	t.Parallel()

	store := newMockStateStore()
	cfg := interlock.Config{PipelinePrefix: "staging/"}
	m := interlock.NewInterlockSensorFlapping(store, cfg)

	params := map[string]string{
		"sensor_key": "flap-key",
		"pipeline":   "etl",
		"flap_count": "2",
	}

	obj := types.DataObject{Key: "test-obj"}
	transport := newMockTransport()

	rec, err := m.Apply(context.Background(), obj, transport, params)
	if err != nil {
		t.Fatalf("Apply() error = %v, want nil", err)
	}
	if rec.Mutation != "interlock-sensor-flapping" {
		t.Errorf("rec.Mutation = %q, want %q", rec.Mutation, "interlock-sensor-flapping")
	}
}
