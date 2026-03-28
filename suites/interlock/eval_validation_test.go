package interlocksuite

import (
	"context"
	"testing"
	"time"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
)

func TestValidationModule_RulesPass(t *testing.T) {
	t.Parallel()
	store := newTestSQLiteStore(t)
	clk := adapter.NewTestClock(time.Date(2026, 3, 28, 10, 0, 0, 0, time.UTC))
	reader := NewLocalEventReader()
	mod := NewValidationModule()

	ctx := context.Background()
	pipeline := "test-pipe"

	// Config with validation rule requiring status=COMPLETE.
	config := map[string]any{
		"validation": map[string]any{
			"trigger": "ALL",
			"rules": []any{
				map[string]any{
					"key":   "SENSOR#hourly-status",
					"check": "equals",
					"field": "status",
					"value": "COMPLETE",
				},
			},
		},
	}

	// Write matching sensor with bare key.
	if err := store.WriteSensor(ctx, pipeline, "hourly-status", adapter.SensorData{
		Pipeline: pipeline,
		Key:      "hourly-status",
		Status:   "COMPLETE",
		Metadata: map[string]string{"status": "COMPLETE"},
	}); err != nil {
		t.Fatalf("WriteSensor: %v", err)
	}

	err := mod.Evaluate(ctx, EvalParams{
		Pipeline:    pipeline,
		Config:      config,
		Store:       store,
		EventWriter: reader,
		Clock:       clk,
	})
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}

	events, _ := reader.ReadEvents(ctx, pipeline, "JOB_TRIGGERED")
	if len(events) != 1 {
		t.Fatalf("expected 1 JOB_TRIGGERED event, got %d", len(events))
	}
}

func TestValidationModule_RulesFail(t *testing.T) {
	t.Parallel()
	store := newTestSQLiteStore(t)
	clk := adapter.NewTestClock(time.Date(2026, 3, 28, 10, 0, 0, 0, time.UTC))
	reader := NewLocalEventReader()
	mod := NewValidationModule()

	ctx := context.Background()
	pipeline := "test-pipe-fail"

	config := map[string]any{
		"validation": map[string]any{
			"trigger": "ALL",
			"rules": []any{
				map[string]any{
					"key":   "SENSOR#hourly-status",
					"check": "equals",
					"field": "status",
					"value": "COMPLETE",
				},
			},
		},
	}

	// Write non-matching sensor.
	if err := store.WriteSensor(ctx, pipeline, "hourly-status", adapter.SensorData{
		Pipeline: pipeline,
		Key:      "hourly-status",
		Status:   "PENDING",
		Metadata: map[string]string{"status": "PENDING"},
	}); err != nil {
		t.Fatalf("WriteSensor: %v", err)
	}

	err := mod.Evaluate(ctx, EvalParams{
		Pipeline:    pipeline,
		Config:      config,
		Store:       store,
		EventWriter: reader,
		Clock:       clk,
	})
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}

	events, _ := reader.ReadEvents(ctx, pipeline, "VALIDATION_EXHAUSTED")
	if len(events) != 1 {
		t.Fatalf("expected 1 VALIDATION_EXHAUSTED event, got %d", len(events))
	}
}

func TestValidationModule_TerminalTrigger_Silent(t *testing.T) {
	t.Parallel()
	store := newTestSQLiteStore(t)
	clk := adapter.NewTestClock(time.Date(2026, 3, 28, 10, 0, 0, 0, time.UTC))
	reader := NewLocalEventReader()
	mod := NewValidationModule()

	ctx := context.Background()
	pipeline := "test-pipe-terminal"

	// Config with validation rules that would normally emit JOB_TRIGGERED.
	config := map[string]any{
		"validation": map[string]any{
			"trigger": "ALL",
			"rules": []any{
				map[string]any{
					"key":   "SENSOR#hourly-status",
					"check": "equals",
					"field": "status",
					"value": "COMPLETE",
				},
			},
		},
	}

	// Write matching sensor so rules would pass.
	if err := store.WriteSensor(ctx, pipeline, "hourly-status", adapter.SensorData{
		Pipeline: pipeline,
		Key:      "hourly-status",
		Status:   "COMPLETE",
		Metadata: map[string]string{"status": "COMPLETE"},
	}); err != nil {
		t.Fatalf("WriteSensor: %v", err)
	}

	// Set trigger to terminal state — module should exit silently.
	trigKey := adapter.TriggerKey{Pipeline: pipeline, Schedule: "default", Date: "default"}
	if err := store.WriteTriggerStatus(ctx, trigKey, "completed"); err != nil {
		t.Fatalf("WriteTriggerStatus: %v", err)
	}

	err := mod.Evaluate(ctx, EvalParams{
		Pipeline:    pipeline,
		Config:      config,
		Store:       store,
		EventWriter: reader,
		Clock:       clk,
	})
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}

	// No events should be emitted — terminal trigger silences the module.
	events, _ := reader.ReadEvents(ctx, pipeline, "")
	if len(events) != 0 {
		t.Fatalf("expected 0 events for terminal trigger, got %d: %+v", len(events), events)
	}
}

func TestValidationModule_NoRules_Skips(t *testing.T) {
	t.Parallel()
	store := newTestSQLiteStore(t)
	clk := adapter.NewTestClock(time.Date(2026, 3, 28, 10, 0, 0, 0, time.UTC))
	reader := NewLocalEventReader()
	mod := NewValidationModule()

	ctx := context.Background()

	// Config with no validation section.
	config := map[string]any{
		"job": map[string]any{"type": "command"},
	}

	err := mod.Evaluate(ctx, EvalParams{
		Pipeline:    "test-skip",
		Config:      config,
		Store:       store,
		EventWriter: reader,
		Clock:       clk,
	})
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}

	// No events should be emitted — module skipped.
	events, _ := reader.ReadEvents(ctx, "test-skip", "")
	if len(events) != 0 {
		t.Fatalf("expected 0 events when no rules, got %d", len(events))
	}
}
