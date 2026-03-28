package interlocksuite

import (
	"context"
	"testing"
	"time"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

func TestWatchdogModule(t *testing.T) {
	t.Parallel()

	baseTime := time.Date(2026, 3, 28, 10, 0, 0, 0, time.UTC)

	tests := []struct {
		name string
		// Clock time for the evaluation.
		clockTime time.Time
		// Config map passed to the module.
		config map[string]any
		// Sensors to write before evaluation.
		sensors map[string]adapter.SensorData
		// SensorKeys passed in EvalParams.
		sensorKeys []string
		// Trigger status to pre-write ("" = none).
		triggerStatus string
		// Pre-emit an event to test short-circuit.
		preEmitEvent string
		// Expected emitted event type ("" = none).
		wantEventType string
		// Expected trigger status after evaluation ("" = don't check).
		wantTriggerStatus string
	}{
		{
			name:      "SensorDeadlineExpired_PastDeadline_SensorPending",
			clockTime: time.Date(2026, 3, 28, 6, 0, 0, 0, time.UTC), // 06:00, past 05:00
			config: map[string]any{
				"sensor_deadline": "05:00",
			},
			sensors: map[string]adapter.SensorData{
				"hourly-status": {
					Key:         "hourly-status",
					Status:      types.SensorStatusPending,
					LastUpdated: time.Date(2026, 3, 28, 4, 0, 0, 0, time.UTC),
				},
			},
			sensorKeys:    []string{"hourly-status"},
			wantEventType: "SENSOR_DEADLINE_EXPIRED",
		},
		{
			name:      "SensorDeadlineExpired_PastDeadline_SensorStaleLastUpdated",
			clockTime: time.Date(2026, 3, 28, 6, 0, 0, 0, time.UTC),
			config: map[string]any{
				"sensor_deadline": "05:00",
			},
			sensors: map[string]adapter.SensorData{
				"hourly-status": {
					Key:         "hourly-status",
					Status:      types.SensorStatusReady,
					LastUpdated: time.Date(2026, 3, 28, 4, 0, 0, 0, time.UTC), // before 05:00 deadline
				},
			},
			sensorKeys:    []string{"hourly-status"},
			wantEventType: "SENSOR_DEADLINE_EXPIRED",
		},
		{
			name:      "SensorDeadlineNotExpired_BeforeDeadline",
			clockTime: time.Date(2026, 3, 28, 4, 0, 0, 0, time.UTC), // 04:00, before 05:00
			config: map[string]any{
				"sensor_deadline": "05:00",
			},
			sensors: map[string]adapter.SensorData{
				"hourly-status": {
					Key:         "hourly-status",
					Status:      types.SensorStatusPending,
					LastUpdated: time.Date(2026, 3, 28, 3, 0, 0, 0, time.UTC),
				},
			},
			sensorKeys:    []string{"hourly-status"},
			wantEventType: "", // no event — not past deadline
		},
		{
			name:      "ScheduleMissed_LastScheduleTooOld",
			clockTime: baseTime,
			config: map[string]any{
				"schedule": map[string]any{
					"cron":              "0 8 * * *",
					"tolerance_minutes": float64(30),
				},
			},
			sensors: map[string]adapter.SensorData{
				"hourly-status": {
					Key:         "hourly-status",
					Status:      types.SensorStatusReady,
					LastUpdated: baseTime,
					Metadata: map[string]string{
						"last_schedule_time": baseTime.Add(-60 * time.Minute).Format(time.RFC3339Nano), // 60 min ago, tolerance 30
					},
				},
			},
			sensorKeys:    []string{"hourly-status"},
			wantEventType: "SCHEDULE_MISSED",
		},
		{
			name:      "ScheduleNotMissed_WithinTolerance",
			clockTime: baseTime,
			config: map[string]any{
				"schedule": map[string]any{
					"cron":              "0 8 * * *",
					"tolerance_minutes": float64(30),
				},
			},
			sensors: map[string]adapter.SensorData{
				"hourly-status": {
					Key:         "hourly-status",
					Status:      types.SensorStatusReady,
					LastUpdated: baseTime,
					Metadata: map[string]string{
						"last_schedule_time": baseTime.Add(-10 * time.Minute).Format(time.RFC3339Nano), // 10 min ago, tolerance 30
					},
				},
			},
			sensorKeys:    []string{"hourly-status"},
			wantEventType: "", // within tolerance
		},
		{
			name:      "IrregularScheduleMissed_DatePassedWithoutExecution",
			clockTime: time.Date(2026, 3, 28, 12, 0, 0, 0, time.UTC), // noon on 2026-03-28
			config: map[string]any{
				"schedule": map[string]any{
					"type":              "inclusion_calendar",
					"dates":             []any{"2026-03-25", "2026-03-28"},
					"tolerance_minutes": float64(120), // 2 hours
				},
			},
			sensors: map[string]adapter.SensorData{
				"hourly-status": {
					Key:         "hourly-status",
					Status:      types.SensorStatusReady,
					LastUpdated: time.Date(2026, 3, 25, 10, 0, 0, 0, time.UTC),
					Metadata: map[string]string{
						// last_schedule_time is before the expected date of 2026-03-28
						"last_schedule_time": time.Date(2026, 3, 25, 10, 0, 0, 0, time.UTC).Format(time.RFC3339Nano),
					},
				},
			},
			sensorKeys:    []string{"hourly-status"},
			wantEventType: "IRREGULAR_SCHEDULE_MISSED",
		},
		{
			name:      "SFNTimeout_RunningJobExceedsTimeout",
			clockTime: baseTime.Add(120 * time.Second), // 120s after start
			config: map[string]any{
				"job": map[string]any{
					"timeout_seconds": float64(60), // 60s timeout
				},
			},
			sensors: map[string]adapter.SensorData{
				"hourly-status": {
					Key:         "hourly-status",
					Status:      types.SensorStatusReady,
					LastUpdated: baseTime,
					Metadata: map[string]string{
						"trigger_time": baseTime.Format(time.RFC3339Nano),
					},
				},
			},
			sensorKeys:    []string{"hourly-status"},
			triggerStatus: "running",
			wantEventType: "SFN_TIMEOUT",
		},
		{
			name:      "SFNTimeout_NotRunning_Skips",
			clockTime: baseTime.Add(120 * time.Second),
			config: map[string]any{
				"job": map[string]any{
					"timeout_seconds": float64(60),
				},
			},
			sensors: map[string]adapter.SensorData{
				"hourly-status": {
					Key:         "hourly-status",
					Status:      types.SensorStatusReady,
					LastUpdated: baseTime,
					Metadata: map[string]string{
						"trigger_time": baseTime.Format(time.RFC3339Nano),
					},
				},
			},
			sensorKeys:    []string{"hourly-status"},
			triggerStatus: "succeeded",
			wantEventType: "", // not running, so no timeout
		},
		{
			name:      "StaleTrigger_RunningWithStaleSensor",
			clockTime: baseTime.Add(20 * time.Minute), // 20 min after sensor update
			config: map[string]any{
				"job": map[string]any{
					"stale_threshold_minutes": float64(10), // 10 min threshold
				},
			},
			sensors: map[string]adapter.SensorData{
				"hourly-status": {
					Key:         "hourly-status",
					Status:      types.SensorStatusReady,
					LastUpdated: baseTime, // 20 min stale, threshold 10
				},
			},
			sensorKeys:        []string{"hourly-status"},
			triggerStatus:     "running",
			wantEventType:     "",      // no event emitted for stale trigger
			wantTriggerStatus: "stale", // writes "stale" to trigger store
		},
		{
			name:      "StaleTrigger_NotRunning_Skips",
			clockTime: baseTime.Add(20 * time.Minute),
			config: map[string]any{
				"job": map[string]any{
					"stale_threshold_minutes": float64(10),
				},
			},
			sensors: map[string]adapter.SensorData{
				"hourly-status": {
					Key:         "hourly-status",
					Status:      types.SensorStatusReady,
					LastUpdated: baseTime,
				},
			},
			sensorKeys:        []string{"hourly-status"},
			triggerStatus:     "succeeded",
			wantEventType:     "",
			wantTriggerStatus: "succeeded", // unchanged
		},
		{
			name:      "NoWatchdogConfig_Skips",
			clockTime: baseTime,
			config: map[string]any{
				"job": map[string]any{"type": "command"},
			},
			sensorKeys:    []string{},
			wantEventType: "",
		},
		{
			name:      "ShortCircuit_ExistingEvents",
			clockTime: time.Date(2026, 3, 28, 6, 0, 0, 0, time.UTC),
			config: map[string]any{
				"sensor_deadline": "05:00",
			},
			sensors: map[string]adapter.SensorData{
				"hourly-status": {
					Key:         "hourly-status",
					Status:      types.SensorStatusPending,
					LastUpdated: time.Date(2026, 3, 28, 4, 0, 0, 0, time.UTC),
				},
			},
			sensorKeys:    []string{"hourly-status"},
			preEmitEvent:  "JOB_TRIGGERED",
			wantEventType: "", // short-circuited
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			store := newTestSQLiteStore(t)
			clk := adapter.NewTestClock(tc.clockTime)
			reader := NewLocalEventReader()
			mod := NewWatchdogModule()

			ctx := context.Background()
			pipeline := "watchdog-test-" + tc.name

			// Write sensor data.
			for key, sd := range tc.sensors {
				sd.Pipeline = pipeline
				if sd.Key == "" {
					sd.Key = key
				}
				if err := store.WriteSensor(ctx, pipeline, key, sd); err != nil {
					t.Fatalf("WriteSensor(%q): %v", key, err)
				}
			}

			// Write trigger status if specified.
			if tc.triggerStatus != "" {
				if err := store.WriteTriggerStatus(ctx, adapter.TriggerKey{
					Pipeline: pipeline,
					Schedule: "default",
					Date:     "default",
				}, tc.triggerStatus); err != nil {
					t.Fatalf("WriteTriggerStatus: %v", err)
				}
			}

			// Pre-emit event for short-circuit test.
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

			// Check emitted events.
			if tc.wantEventType != "" {
				events, _ := reader.ReadEvents(ctx, pipeline, tc.wantEventType)
				if len(events) != 1 {
					allEvents, _ := reader.ReadEvents(ctx, pipeline, "")
					t.Fatalf("expected 1 %s event, got %d; all events: %+v",
						tc.wantEventType, len(events), allEvents)
				}
			} else if tc.preEmitEvent == "" {
				// No events should be emitted (unless pre-emitted).
				allEvents, _ := reader.ReadEvents(ctx, pipeline, "")
				if len(allEvents) != 0 {
					t.Fatalf("expected 0 events, got %d: %+v", len(allEvents), allEvents)
				}
			}

			// Check trigger status when specified.
			if tc.wantTriggerStatus != "" {
				status, readErr := store.ReadTriggerStatus(ctx, adapter.TriggerKey{
					Pipeline: pipeline,
					Schedule: "default",
					Date:     "default",
				})
				if readErr != nil {
					t.Fatalf("ReadTriggerStatus: %v", readErr)
				}
				if status != tc.wantTriggerStatus {
					t.Fatalf("expected trigger status %q, got %q", tc.wantTriggerStatus, status)
				}
			}
		})
	}
}

func TestParseTimeOfDay(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 3, 28, 10, 30, 0, 0, time.UTC)

	tests := []struct {
		name    string
		tod     string
		wantH   int
		wantM   int
		wantErr bool
	}{
		{"valid_morning", "05:00", 5, 0, false},
		{"valid_afternoon", "14:30", 14, 30, false},
		{"valid_midnight", "00:00", 0, 0, false},
		{"invalid_format", "5pm", 0, 0, true},
		{"invalid_hour", "xx:00", 0, 0, true},
		{"invalid_minute", "05:yy", 0, 0, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := parseTimeOfDay(now, tc.tod)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got.Hour() != tc.wantH || got.Minute() != tc.wantM {
				t.Fatalf("got %02d:%02d, want %02d:%02d", got.Hour(), got.Minute(), tc.wantH, tc.wantM)
			}
			// Should share the same date.
			if got.Year() != 2026 || got.Month() != 3 || got.Day() != 28 {
				t.Fatalf("date mismatch: got %v", got)
			}
		})
	}
}

func TestConfigFloat64(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		m       map[string]any
		key     string
		want    float64
		wantErr bool
	}{
		{"float64_value", map[string]any{"x": float64(42.5)}, "x", 42.5, false},
		{"int_value", map[string]any{"x": 10}, "x", 10.0, false},
		{"string_value", map[string]any{"x": "3.14"}, "x", 3.14, false},
		{"missing_key", map[string]any{}, "x", 0, true},
		{"unsupported_type", map[string]any{"x": true}, "x", 0, true},
		{"bad_string", map[string]any{"x": "abc"}, "x", 0, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := configFloat64(tc.m, tc.key)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
		})
	}
}
