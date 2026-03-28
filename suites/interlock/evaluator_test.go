package interlocksuite

import (
	"context"
	"testing"
	"time"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
)

// Compile-time interface satisfaction checks.
var _ InterlockEvaluator = (*AWSInterlockEvaluator)(nil)
var _ InterlockEvaluator = (*LocalInterlockEvaluator)(nil)

func TestAWSInterlockEvaluator_NoOp(t *testing.T) {
	t.Parallel()
	e := NewAWSInterlockEvaluator()
	err := e.EvaluateAfterInjection(context.Background(), "pipe-a", "hourly", "2026-03-27T10", nil)
	if err != nil {
		t.Fatalf("expected nil error for AWS no-op, got: %v", err)
	}
}

func TestLocalInterlockEvaluator_NoPipelineConfig(t *testing.T) {
	t.Parallel()
	store := newTestSQLiteStore(t)
	clk := adapter.NewTestClock(time.Date(2026, 3, 27, 10, 0, 0, 0, time.UTC))
	reader := NewLocalEventReader()
	e := NewLocalInterlockEvaluator(store, reader, clk)

	err := e.EvaluateAfterInjection(context.Background(), "pipe-a", "hourly", "2026-03-27T10", nil)
	if err != nil {
		t.Fatalf("expected nil error, got: %v", err)
	}

	// No config => should emit VALIDATION_EXHAUSTED.
	events, err := reader.ReadEvents(context.Background(), "pipe-a", "VALIDATION_EXHAUSTED")
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 VALIDATION_EXHAUSTED event, got %d", len(events))
	}
}

func TestLocalInterlockEvaluator_RulesPass(t *testing.T) {
	t.Parallel()
	store := newTestSQLiteStore(t)
	clk := adapter.NewTestClock(time.Date(2026, 3, 27, 10, 0, 0, 0, time.UTC))
	reader := NewLocalEventReader()
	e := NewLocalInterlockEvaluator(store, reader, clk)

	ctx := context.Background()

	// Write a pipeline config with a validation rule.
	config := []byte(`
validation:
  trigger: ALL
  rules:
    - key: hourly-status
      check: equals
      field: status
      value: COMPLETE
`)
	if err := store.WritePipelineConfig(ctx, "pipe-b", config); err != nil {
		t.Fatalf("WritePipelineConfig: %v", err)
	}

	// Write matching sensor data.
	if err := store.WriteSensor(ctx, "pipe-b", "hourly-status", adapter.SensorData{
		Pipeline: "pipe-b",
		Key:      "hourly-status",
		Status:   "COMPLETE",
		Metadata: map[string]string{"status": "COMPLETE"},
	}); err != nil {
		t.Fatalf("WriteSensor: %v", err)
	}

	err := e.EvaluateAfterInjection(ctx, "pipe-b", "hourly", "2026-03-27T10", nil)
	if err != nil {
		t.Fatalf("EvaluateAfterInjection: %v", err)
	}

	// Rules pass => should emit JOB_TRIGGERED.
	events, err := reader.ReadEvents(ctx, "pipe-b", "JOB_TRIGGERED")
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 JOB_TRIGGERED event, got %d", len(events))
	}
}

func TestLocalInterlockEvaluator_PrefixedKeys(t *testing.T) {
	t.Parallel()
	store := newTestSQLiteStore(t)
	clk := adapter.NewTestClock(time.Date(2026, 3, 27, 10, 0, 0, 0, time.UTC))
	reader := NewLocalEventReader()
	e := NewLocalInterlockEvaluator(store, reader, clk)

	ctx := context.Background()

	// Config uses SENSOR# prefixed keys (interlock convention).
	config := []byte(`
validation:
  trigger: ALL
  rules:
    - key: "SENSOR#hourly-status"
      check: equals
      field: status
      value: COMPLETE
`)
	if err := store.WritePipelineConfig(ctx, "pipe-prefix", config); err != nil {
		t.Fatalf("WritePipelineConfig: %v", err)
	}

	// Sensor written with bare key (harness convention).
	if err := store.WriteSensor(ctx, "pipe-prefix", "hourly-status", adapter.SensorData{
		Pipeline: "pipe-prefix",
		Key:      "hourly-status",
		Status:   "COMPLETE",
		Metadata: map[string]string{"status": "COMPLETE"},
	}); err != nil {
		t.Fatalf("WriteSensor: %v", err)
	}

	err := e.EvaluateAfterInjection(ctx, "pipe-prefix", "hourly", "2026-03-27T10", nil)
	if err != nil {
		t.Fatalf("EvaluateAfterInjection: %v", err)
	}

	// Despite SENSOR# prefix in rule key, evaluator should strip it and find
	// the sensor written with bare key → rules pass → JOB_TRIGGERED.
	events, err := reader.ReadEvents(ctx, "pipe-prefix", "JOB_TRIGGERED")
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 JOB_TRIGGERED event, got %d", len(events))
	}
}

func TestLocalInterlockEvaluator_RulesFail(t *testing.T) {
	t.Parallel()
	store := newTestSQLiteStore(t)
	clk := adapter.NewTestClock(time.Date(2026, 3, 27, 10, 0, 0, 0, time.UTC))
	reader := NewLocalEventReader()
	e := NewLocalInterlockEvaluator(store, reader, clk)

	ctx := context.Background()

	// Write a pipeline config with a validation rule.
	config := []byte(`
validation:
  trigger: ALL
  rules:
    - key: hourly-status
      check: equals
      field: status
      value: COMPLETE
`)
	if err := store.WritePipelineConfig(ctx, "pipe-c", config); err != nil {
		t.Fatalf("WritePipelineConfig: %v", err)
	}

	// Write NON-matching sensor data (status is PENDING, not COMPLETE).
	if err := store.WriteSensor(ctx, "pipe-c", "hourly-status", adapter.SensorData{
		Pipeline: "pipe-c",
		Key:      "hourly-status",
		Status:   "PENDING",
		Metadata: map[string]string{"status": "PENDING"},
	}); err != nil {
		t.Fatalf("WriteSensor: %v", err)
	}

	err := e.EvaluateAfterInjection(ctx, "pipe-c", "hourly", "2026-03-27T10", nil)
	if err != nil {
		t.Fatalf("EvaluateAfterInjection: %v", err)
	}

	// Rules fail => should emit VALIDATION_EXHAUSTED.
	events, err := reader.ReadEvents(ctx, "pipe-c", "VALIDATION_EXHAUSTED")
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 VALIDATION_EXHAUSTED event, got %d", len(events))
	}
}
