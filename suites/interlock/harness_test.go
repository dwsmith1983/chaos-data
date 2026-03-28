package interlocksuite

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/dwsmith1983/chaos-data/adapters/local"
	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// newTestSQLiteStore creates an in-memory SQLite state store for testing.
func newTestSQLiteStore(t *testing.T) *local.SQLiteState {
	t.Helper()
	store, err := local.NewSQLiteState(":memory:")
	if err != nil {
		t.Fatalf("newTestSQLiteStore: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

func TestHarness_NamespacedPipeline(t *testing.T) {
	t.Parallel()
	h := NewHarness(nil, nil, "042")
	got := h.NamespacedPipeline("bronze-cdr")
	want := "suite-042-bronze-cdr"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestHarness_Setup_WritesNamespacedSensor(t *testing.T) {
	t.Parallel()
	store := newTestSQLiteStore(t)
	clk := adapter.NewTestClock(time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC))
	h := NewHarness(store, clk, "001")

	spec := SetupSpec{
		Pipeline: "bronze-cdr",
		Sensors: map[string]map[string]interface{}{
			"hourly-status": {"status": "COMPLETE", "sensor_count": 1000},
		},
	}

	if err := h.Setup(context.Background(), spec); err != nil {
		t.Fatalf("Setup: %v", err)
	}

	sensor, err := store.ReadSensor(context.Background(), "suite-001-bronze-cdr", "hourly-status")
	if err != nil {
		t.Fatalf("ReadSensor: %v", err)
	}
	if sensor.Status != types.SensorStatus("COMPLETE") {
		t.Errorf("sensor status = %q, want %q", sensor.Status, "COMPLETE")
	}
	if sensor.LastUpdated != clk.Now() {
		t.Errorf("sensor last_updated = %v, want %v", sensor.LastUpdated, clk.Now())
	}
}

func TestHarness_Setup_WritesTriggerStatus(t *testing.T) {
	t.Parallel()
	store := newTestSQLiteStore(t)
	clk := adapter.NewWallClock()
	h := NewHarness(store, clk, "002")

	spec := SetupSpec{
		Pipeline:      "silver-events",
		TriggerStatus: "COMPLETED",
	}

	if err := h.Setup(context.Background(), spec); err != nil {
		t.Fatalf("Setup: %v", err)
	}

	key := adapter.TriggerKey{Pipeline: "suite-002-silver-events", Schedule: "default", Date: "default"}
	status, err := store.ReadTriggerStatus(context.Background(), key)
	if err != nil {
		t.Fatalf("ReadTriggerStatus: %v", err)
	}
	if status != "COMPLETED" {
		t.Errorf("trigger status = %q, want %q", status, "COMPLETED")
	}
}

func TestHarness_Setup_WritesPipelineConfig(t *testing.T) {
	t.Parallel()
	store := newTestSQLiteStore(t)
	clk := adapter.NewWallClock()
	h := NewHarness(store, clk, "003")

	spec := SetupSpec{
		Pipeline:       "gold-agg",
		PipelineConfig: map[string]interface{}{"schedule": "hourly", "retries": 3},
	}

	if err := h.Setup(context.Background(), spec); err != nil {
		t.Fatalf("Setup: %v", err)
	}

	config, err := store.ReadPipelineConfig(context.Background(), "suite-003-gold-agg")
	if err != nil {
		t.Fatalf("ReadPipelineConfig: %v", err)
	}
	if config == nil {
		t.Fatal("expected pipeline config to be written, got nil")
	}
}

func TestHarness_Setup_NilPipelineConfig_Skipped(t *testing.T) {
	t.Parallel()
	store := newTestSQLiteStore(t)
	clk := adapter.NewWallClock()
	h := NewHarness(store, clk, "004")

	spec := SetupSpec{
		Pipeline: "pipe-skip",
	}

	if err := h.Setup(context.Background(), spec); err != nil {
		t.Fatalf("Setup: %v", err)
	}

	config, err := store.ReadPipelineConfig(context.Background(), "suite-004-pipe-skip")
	if err != nil {
		t.Fatalf("ReadPipelineConfig: %v", err)
	}
	if config != nil {
		t.Errorf("expected nil pipeline config, got %s", config)
	}
}

func TestHarness_Setup_MultipleSensors(t *testing.T) {
	t.Parallel()
	store := newTestSQLiteStore(t)
	clk := adapter.NewTestClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	h := NewHarness(store, clk, "005")

	spec := SetupSpec{
		Pipeline: "multi",
		Sensors: map[string]map[string]interface{}{
			"sensor-a": {"status": "READY"},
			"sensor-b": {"status": "PENDING"},
		},
	}

	if err := h.Setup(context.Background(), spec); err != nil {
		t.Fatalf("Setup: %v", err)
	}

	for _, tc := range []struct {
		key        string
		wantStatus types.SensorStatus
	}{
		{"sensor-a", "READY"},
		{"sensor-b", "PENDING"},
	} {
		sensor, err := store.ReadSensor(context.Background(), "suite-005-multi", tc.key)
		if err != nil {
			t.Fatalf("ReadSensor(%q): %v", tc.key, err)
		}
		if sensor.Status != tc.wantStatus {
			t.Errorf("sensor %q status = %q, want %q", tc.key, sensor.Status, tc.wantStatus)
		}
	}
}

func TestHarness_Teardown_CleansNamespacedState(t *testing.T) {
	t.Parallel()
	store := newTestSQLiteStore(t)
	clk := adapter.NewWallClock()
	h := NewHarness(store, clk, "001")

	// Setup some state under the namespace.
	spec := SetupSpec{Pipeline: "pipe-a", TriggerStatus: "READY"}
	if err := h.Setup(context.Background(), spec); err != nil {
		t.Fatalf("Setup: %v", err)
	}

	// Write state for a different pipeline (should NOT be deleted).
	if err := store.WriteSensor(context.Background(), "other-pipe", "s1", adapter.SensorData{
		Pipeline: "other-pipe",
		Key:      "s1",
		Status:   types.SensorStatus("READY"),
	}); err != nil {
		t.Fatalf("WriteSensor other-pipe: %v", err)
	}

	if err := h.Teardown(context.Background()); err != nil {
		t.Fatalf("Teardown: %v", err)
	}

	// Namespaced trigger state should be gone.
	key := adapter.TriggerKey{Pipeline: "suite-001-pipe-a", Schedule: "default", Date: "default"}
	status, err := store.ReadTriggerStatus(context.Background(), key)
	if err != nil {
		t.Fatalf("ReadTriggerStatus: %v", err)
	}
	if status != "" {
		t.Errorf("expected suite-001- trigger state to be deleted, got %q", status)
	}

	// Other-pipe state should remain.
	other, err := store.ReadSensor(context.Background(), "other-pipe", "s1")
	if err != nil {
		t.Fatalf("ReadSensor other-pipe: %v", err)
	}
	if other.Key != "s1" {
		t.Error("other-pipe state should still exist after teardown")
	}
}

func TestHarness_Setup_BaselineSensorCount(t *testing.T) {
	t.Parallel()
	store := newTestSQLiteStore(t)
	clk := adapter.NewTestClock(time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC))
	h := NewHarness(store, clk, "007")

	spec := SetupSpec{
		Pipeline: "bronze-cdr",
		Sensors: map[string]map[string]interface{}{
			"hourly-status": {"status": "COMPLETE", "sensor_count": 1000},
		},
	}

	if err := h.Setup(context.Background(), spec); err != nil {
		t.Fatalf("Setup: %v", err)
	}

	sensor, err := store.ReadSensor(context.Background(), "suite-007-bronze-cdr", "hourly-status")
	if err != nil {
		t.Fatalf("ReadSensor: %v", err)
	}
	if got := sensor.Metadata["__baseline_sensor_count"]; got != "1000" {
		t.Errorf("metadata[__baseline_sensor_count] = %q, want %q", got, "1000")
	}
	if got := sensor.Metadata["sensor_count"]; got != "1000" {
		t.Errorf("metadata[sensor_count] = %q, want %q", got, "1000")
	}
}

func TestHarness_Setup_NoBaselineWithoutSensorCount(t *testing.T) {
	t.Parallel()
	store := newTestSQLiteStore(t)
	clk := adapter.NewTestClock(time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC))
	h := NewHarness(store, clk, "008")

	spec := SetupSpec{
		Pipeline: "silver-events",
		Sensors: map[string]map[string]interface{}{
			"daily-check": {"status": "READY"},
		},
	}

	if err := h.Setup(context.Background(), spec); err != nil {
		t.Fatalf("Setup: %v", err)
	}

	sensor, err := store.ReadSensor(context.Background(), "suite-008-silver-events", "daily-check")
	if err != nil {
		t.Fatalf("ReadSensor: %v", err)
	}
	if _, ok := sensor.Metadata["__baseline_sensor_count"]; ok {
		t.Error("expected no __baseline_sensor_count when sensor_count is absent")
	}
}

func TestHarness_Setup_SensorMetadataStringConversion(t *testing.T) {
	t.Parallel()
	store := newTestSQLiteStore(t)
	clk := adapter.NewTestClock(time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC))
	h := NewHarness(store, clk, "006")

	spec := SetupSpec{
		Pipeline: "meta-test",
		Sensors: map[string]map[string]interface{}{
			"s1": {"status": "OK", "count": 42, "flag": true},
		},
	}

	if err := h.Setup(context.Background(), spec); err != nil {
		t.Fatalf("Setup: %v", err)
	}

	sensor, err := store.ReadSensor(context.Background(), "suite-006-meta-test", "s1")
	if err != nil {
		t.Fatalf("ReadSensor: %v", err)
	}
	// Metadata values are converted to strings via fmt.Sprintf.
	if got := sensor.Metadata["count"]; got != fmt.Sprintf("%v", 42) {
		t.Errorf("metadata[count] = %q, want %q", got, "42")
	}
	if got := sensor.Metadata["flag"]; got != fmt.Sprintf("%v", true) {
		t.Errorf("metadata[flag] = %q, want %q", got, "true")
	}
}
