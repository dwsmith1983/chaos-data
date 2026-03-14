package interlock_test

import (
	"context"
	"testing"
	"time"

	"github.com/dwsmith1983/chaos-data/adapters/interlock"
	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

func TestInterlockStaleSensor_Type(t *testing.T) {
	t.Parallel()

	store := newMockStateStore()
	cfg := interlock.Config{PipelinePrefix: "prod/"}
	m := interlock.NewInterlockStaleSensor(store, cfg)

	if got := m.Type(); got != "interlock-stale-sensor" {
		t.Errorf("Type() = %q, want %q", got, "interlock-stale-sensor")
	}
}

func TestInterlockStaleSensor_Apply_EnrichesPipelinePrefix(t *testing.T) {
	t.Parallel()

	store := newMockStateStore()
	store.sensors["prod/my-pipeline/sensor-1"] = adapter.SensorData{
		Pipeline:    "prod/my-pipeline",
		Key:         "sensor-1",
		Status:      types.SensorStatusReady,
		LastUpdated: time.Now(),
	}

	cfg := interlock.Config{PipelinePrefix: "prod/"}
	m := interlock.NewInterlockStaleSensor(store, cfg)

	params := map[string]string{
		"sensor_key":      "sensor-1",
		"pipeline":        "my-pipeline",
		"last_update_age": "1h",
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
		t.Errorf("ReadSensor pipeline = %q, want %q", calls[0].Pipeline, "prod/my-pipeline")
	}
}

func TestInterlockStaleSensor_Apply_ErrorPropagation(t *testing.T) {
	t.Parallel()

	store := newMockStateStore()
	store.readSensorErr = true

	cfg := interlock.Config{PipelinePrefix: "prod/"}
	m := interlock.NewInterlockStaleSensor(store, cfg)

	params := map[string]string{
		"sensor_key":      "sensor-1",
		"pipeline":        "my-pipeline",
		"last_update_age": "1h",
	}

	obj := types.DataObject{Key: "test-obj"}
	transport := newMockTransport()

	_, err := m.Apply(context.Background(), obj, transport, params)
	if err == nil {
		t.Fatal("Apply() error = nil, want error")
	}
}

func TestInterlockPhantomSensor_Type(t *testing.T) {
	t.Parallel()

	store := newMockStateStore()
	cfg := interlock.Config{PipelinePrefix: "staging/"}
	m := interlock.NewInterlockPhantomSensor(store, cfg)

	if got := m.Type(); got != "interlock-phantom-sensor" {
		t.Errorf("Type() = %q, want %q", got, "interlock-phantom-sensor")
	}
}

func TestInterlockPhantomSensor_Apply_EnrichesPipelinePrefix(t *testing.T) {
	t.Parallel()

	store := newMockStateStore()
	cfg := interlock.Config{PipelinePrefix: "staging/"}
	m := interlock.NewInterlockPhantomSensor(store, cfg)

	params := map[string]string{
		"pipeline":   "etl-daily",
		"sensor_key": "phantom-key",
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
	if calls[0].Pipeline != "staging/etl-daily" {
		t.Errorf("WriteSensor pipeline = %q, want %q", calls[0].Pipeline, "staging/etl-daily")
	}
}

func TestInterlockPhantomSensor_Apply_ErrorPropagation(t *testing.T) {
	t.Parallel()

	store := newMockStateStore()
	store.writeSensorErr = true

	cfg := interlock.Config{PipelinePrefix: "staging/"}
	m := interlock.NewInterlockPhantomSensor(store, cfg)

	params := map[string]string{
		"pipeline":   "etl-daily",
		"sensor_key": "phantom-key",
	}

	obj := types.DataObject{Key: "test-obj"}
	transport := newMockTransport()

	_, err := m.Apply(context.Background(), obj, transport, params)
	if err == nil {
		t.Fatal("Apply() error = nil, want error")
	}
}

func TestInterlockSplitSensor_Type(t *testing.T) {
	t.Parallel()

	store := newMockStateStore()
	cfg := interlock.Config{PipelinePrefix: "dev/"}
	m := interlock.NewInterlockSplitSensor(store, cfg)

	if got := m.Type(); got != "interlock-split-sensor" {
		t.Errorf("Type() = %q, want %q", got, "interlock-split-sensor")
	}
}

func TestInterlockSplitSensor_Apply_EnrichesPipelinePrefix(t *testing.T) {
	t.Parallel()

	store := newMockStateStore()
	cfg := interlock.Config{PipelinePrefix: "dev/"}
	m := interlock.NewInterlockSplitSensor(store, cfg)

	params := map[string]string{
		"sensor_key":         "split-key",
		"pipeline":           "ingestion",
		"conflicting_values": "ready,pending,ready",
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
	if calls[0].Pipeline != "dev/ingestion" {
		t.Errorf("WriteSensor pipeline = %q, want %q", calls[0].Pipeline, "dev/ingestion")
	}
}

func TestInterlockSplitSensor_Apply_ErrorPropagation(t *testing.T) {
	t.Parallel()

	store := newMockStateStore()
	store.writeSensorErr = true

	cfg := interlock.Config{PipelinePrefix: "dev/"}
	m := interlock.NewInterlockSplitSensor(store, cfg)

	params := map[string]string{
		"sensor_key":         "split-key",
		"pipeline":           "ingestion",
		"conflicting_values": "ready,pending",
	}

	obj := types.DataObject{Key: "test-obj"}
	transport := newMockTransport()

	_, err := m.Apply(context.Background(), obj, transport, params)
	if err == nil {
		t.Fatal("Apply() error = nil, want error")
	}
}

func TestInterlockStaleSensor_Apply_NoPrefixWhenEmpty(t *testing.T) {
	t.Parallel()

	store := newMockStateStore()
	store.sensors["my-pipeline/sensor-1"] = adapter.SensorData{
		Pipeline:    "my-pipeline",
		Key:         "sensor-1",
		Status:      types.SensorStatusReady,
		LastUpdated: time.Now(),
	}

	cfg := interlock.Config{PipelinePrefix: ""}
	m := interlock.NewInterlockStaleSensor(store, cfg)

	params := map[string]string{
		"sensor_key":      "sensor-1",
		"pipeline":        "my-pipeline",
		"last_update_age": "1h",
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
