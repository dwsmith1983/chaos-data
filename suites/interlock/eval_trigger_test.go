package interlocksuite

import (
	"context"
	"testing"
	"time"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

func TestTriggerModule(t *testing.T) {
	t.Parallel()

	baseTime := time.Date(2026, 3, 28, 10, 0, 0, 0, time.UTC)

	tests := []struct {
		name           string
		sensorKeys     []string
		sensorStatuses map[string]string // key -> status to write
		configRules    bool              // whether config has validation rules
		preEmitEvent   string            // event to pre-emit (short-circuit test)
		wantEventType  string            // expected emitted event type ("" = none)
		wantEventCount int               // expected total event count
	}{
		{
			name:       "SensorsPending",
			sensorKeys: []string{"hourly-status"},
			sensorStatuses: map[string]string{
				"hourly-status": "PENDING",
			},
			wantEventType:  "VALIDATION_EXHAUSTED",
			wantEventCount: 1,
		},
		{
			name:       "SensorsReady",
			sensorKeys: []string{"hourly-status"},
			sensorStatuses: map[string]string{
				"hourly-status": "COMPLETE",
			},
			wantEventType:  "JOB_TRIGGERED",
			wantEventCount: 1,
		},
		{
			name:       "HasValidationRules_Skips",
			sensorKeys: []string{"hourly-status"},
			sensorStatuses: map[string]string{
				"hourly-status": "COMPLETE",
			},
			configRules:    true,
			wantEventType:  "",
			wantEventCount: 0,
		},
		{
			name:           "ExistingEvents_ShortCircuits",
			sensorKeys:     []string{"hourly-status"},
			sensorStatuses: map[string]string{"hourly-status": "COMPLETE"},
			preEmitEvent:   "JOB_TRIGGERED",
			wantEventType:  "",
			wantEventCount: 1, // only the pre-emitted event
		},
		{
			name:           "NoSensorKeys",
			sensorKeys:     nil,
			sensorStatuses: nil,
			wantEventType:  "VALIDATION_EXHAUSTED",
			wantEventCount: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			store := newTestSQLiteStore(t)
			clk := adapter.NewTestClock(baseTime)
			reader := NewLocalEventReader()
			mod := NewTriggerModule()

			ctx := context.Background()
			pipeline := "trigger-test-" + tc.name

			// Write sensor data if specified.
			for key, status := range tc.sensorStatuses {
				if err := store.WriteSensor(ctx, pipeline, key, adapter.SensorData{
					Pipeline: pipeline,
					Key:      key,
					Status:   types.SensorStatus(status),
					Metadata: map[string]string{"status": status},
				}); err != nil {
					t.Fatalf("WriteSensor(%q): %v", key, err)
				}
			}

			// Build config.
			config := map[string]any{
				"job": map[string]any{"type": "command"},
			}
			if tc.configRules {
				config["validation"] = map[string]any{
					"trigger": "ALL",
					"rules": []any{
						map[string]any{
							"key":   "SENSOR#hourly-status",
							"check": "equals",
							"field": "status",
							"value": "COMPLETE",
						},
					},
				}
			}

			// Pre-emit an event if the test requires short-circuit behavior.
			if tc.preEmitEvent != "" {
				reader.Emit(InterlockEventRecord{
					PipelineID: pipeline,
					EventType:  tc.preEmitEvent,
					Timestamp:  clk.Now(),
				})
			}

			err := mod.Evaluate(ctx, EvalParams{
				Pipeline:    pipeline,
				Config:      config,
				Store:       store,
				EventWriter: reader,
				Clock:       clk,
				SensorKeys:  tc.sensorKeys,
			})
			if err != nil {
				t.Fatalf("Evaluate: %v", err)
			}

			// Check total event count.
			allEvents, _ := reader.ReadEvents(ctx, pipeline, "")
			if len(allEvents) != tc.wantEventCount {
				t.Fatalf("expected %d total events, got %d: %+v",
					tc.wantEventCount, len(allEvents), allEvents)
			}

			// Check specific event type when expected.
			if tc.wantEventType != "" && tc.preEmitEvent == "" {
				typed, _ := reader.ReadEvents(ctx, pipeline, tc.wantEventType)
				if len(typed) != 1 {
					t.Fatalf("expected 1 %s event, got %d", tc.wantEventType, len(typed))
				}
			}
		})
	}
}

func TestTriggerModule_TerminalTrigger_Silent(t *testing.T) {
	t.Parallel()
	store := newTestSQLiteStore(t)
	clk := adapter.NewTestClock(time.Date(2026, 3, 28, 10, 0, 0, 0, time.UTC))
	reader := NewLocalEventReader()
	mod := NewTriggerModule()

	ctx := context.Background()
	pipeline := "trigger-test-terminal"

	// Write ready sensors so normally JOB_TRIGGERED would fire.
	if err := store.WriteSensor(ctx, pipeline, "hourly-status", adapter.SensorData{
		Pipeline: pipeline,
		Key:      "hourly-status",
		Status:   types.SensorStatusComplete,
		Metadata: map[string]string{"status": "COMPLETE"},
	}); err != nil {
		t.Fatalf("WriteSensor: %v", err)
	}

	// Set trigger to terminal state — module should exit silently.
	trigKey := adapter.TriggerKey{Pipeline: pipeline, Schedule: "default", Date: "default"}
	if err := store.WriteTriggerStatus(ctx, trigKey, "completed"); err != nil {
		t.Fatalf("WriteTriggerStatus: %v", err)
	}

	config := map[string]any{
		"job": map[string]any{"type": "command"},
	}

	err := mod.Evaluate(ctx, EvalParams{
		Pipeline:    pipeline,
		Config:      config,
		Store:       store,
		EventWriter: reader,
		Clock:       clk,
		SensorKeys:  []string{"hourly-status"},
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
