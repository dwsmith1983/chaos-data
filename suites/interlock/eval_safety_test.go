package interlocksuite

import (
	"context"
	"testing"
	"time"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

func TestSafetyModule(t *testing.T) {
	t.Parallel()

	// Clock pinned to 2026-03-28 so calendar tests are deterministic.
	baseTime := time.Date(2026, 3, 28, 10, 0, 0, 0, time.UTC)

	tests := []struct {
		name           string
		config         map[string]any
		sensorKeys     []string
		sensorStatuses map[string]adapter.SensorData
		triggerStatus  string // pre-written trigger status
		preEmitEvent   string // event to pre-emit (short-circuit test)
		wantEventType  string // expected emitted event type ("" = none)
		wantEventCount int
	}{
		{
			name: "ExclusionCalendarMatch",
			config: map[string]any{
				"schedule": map[string]any{
					"type":           "exclusion_calendar",
					"excluded_dates": []any{"2026-03-28", "2026-12-25"},
				},
			},
			wantEventType:  "PIPELINE_EXCLUDED",
			wantEventCount: 1,
		},
		{
			name: "ExclusionCalendarNoMatch",
			config: map[string]any{
				"schedule": map[string]any{
					"type":           "exclusion_calendar",
					"excluded_dates": []any{"2026-12-25"},
				},
			},
			wantEventType:  "",
			wantEventCount: 0,
		},
		{
			name: "InclusionCalendarTodayNotInDates",
			config: map[string]any{
				"schedule": map[string]any{
					"type":  "inclusion_calendar",
					"dates": []any{"2026-03-01", "2026-03-15"},
				},
			},
			wantEventType:  "PIPELINE_EXCLUDED",
			wantEventCount: 1,
		},
		{
			name: "InclusionCalendarTodayInDates",
			config: map[string]any{
				"schedule": map[string]any{
					"type":  "inclusion_calendar",
					"dates": []any{"2026-03-28"},
				},
			},
			wantEventType:  "",
			wantEventCount: 0,
		},
		{
			name: "DryRunAllSensorsReady",
			config: map[string]any{
				"dryrun": true,
			},
			sensorKeys: []string{"hourly-status"},
			sensorStatuses: map[string]adapter.SensorData{
				"hourly-status": {
					Key:    "hourly-status",
					Status: types.SensorStatusComplete,
					Metadata: map[string]string{
						"status":       "COMPLETE",
						"sensor_count": "1000",
					},
				},
			},
			wantEventType:  "DRY_RUN_WOULD_TRIGGER",
			wantEventCount: 1,
		},
		{
			name: "DryRunTriggerCompletedWithDrift",
			config: map[string]any{
				"dryrun": true,
			},
			sensorKeys: []string{"hourly-status"},
			sensorStatuses: map[string]adapter.SensorData{
				"hourly-status": {
					Key:    "hourly-status",
					Status: types.SensorStatusComplete,
					Metadata: map[string]string{
						"status":                  "COMPLETE",
						"sensor_count":            "1500",
						"__baseline_sensor_count": "1000",
					},
				},
			},
			triggerStatus:  "COMPLETED",
			wantEventType:  "DRY_RUN_DRIFT",
			wantEventCount: 1,
		},
		{
			name: "DryRunMarkerExistsSuppresses",
			config: map[string]any{
				"dryrun":               true,
				"dryrun_marker_exists": true,
			},
			sensorKeys: []string{"hourly-status"},
			sensorStatuses: map[string]adapter.SensorData{
				"hourly-status": {
					Key:      "hourly-status",
					Status:   types.SensorStatusComplete,
					Metadata: map[string]string{"status": "COMPLETE"},
				},
			},
			wantEventType:  "",
			wantEventCount: 0,
		},
		{
			name:           "NoScheduleNoDryrun",
			config:         map[string]any{},
			wantEventType:  "",
			wantEventCount: 0,
		},
		{
			name: "ShortCircuitWhenEventsExist",
			config: map[string]any{
				"schedule": map[string]any{
					"type":           "exclusion_calendar",
					"excluded_dates": []any{"2026-03-28"},
				},
			},
			preEmitEvent:   "JOB_TRIGGERED",
			wantEventType:  "",
			wantEventCount: 1, // only the pre-emitted event
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			store := newTestSQLiteStore(t)
			clk := adapter.NewTestClock(baseTime)
			reader := NewLocalEventReader()
			mod := NewSafetyModule()

			ctx := context.Background()
			pipeline := "safety-test-" + tc.name

			// Write sensor data if specified.
			for key, sd := range tc.sensorStatuses {
				sd.Pipeline = pipeline
				if err := store.WriteSensor(ctx, pipeline, key, sd); err != nil {
					t.Fatalf("WriteSensor(%q): %v", key, err)
				}
			}

			// Write trigger status if specified.
			if tc.triggerStatus != "" {
				trigKey := adapter.TriggerKey{
					Pipeline: pipeline,
					Schedule: "default",
					Date:     "default",
				}
				if err := store.WriteTriggerStatus(ctx, trigKey, tc.triggerStatus); err != nil {
					t.Fatalf("WriteTriggerStatus: %v", err)
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
				Config:      tc.config,
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
