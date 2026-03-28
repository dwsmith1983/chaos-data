package interlocksuite

import (
	"context"
	"testing"
	"time"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

func TestPostRunModule(t *testing.T) {
	t.Parallel()

	baseTime := time.Date(2026, 3, 28, 10, 0, 0, 0, time.UTC)

	tests := []struct {
		name           string
		sensors        map[string]adapter.SensorData
		sensorKeys     []string
		triggerStatus  string
		preEmitEvent   string // event to pre-emit (short-circuit test)
		wantEventType  string // expected emitted event type ("" = none)
		wantEventCount int
	}{
		{
			name: "DriftDetected_Completed_CountChanged",
			sensors: map[string]adapter.SensorData{
				"hourly-status": {
					Key:         "hourly-status",
					Status:      types.SensorStatusComplete,
					LastUpdated: baseTime,
					Metadata: map[string]string{
						"sensor_count":            "1500",
						"__baseline_sensor_count": "1000",
					},
				},
			},
			sensorKeys:     []string{"hourly-status"},
			triggerStatus:  "COMPLETED",
			wantEventType:  "POST_RUN_DRIFT",
			wantEventCount: 1,
		},
		{
			name: "DriftDetected_Running_CountChanged",
			sensors: map[string]adapter.SensorData{
				"hourly-status": {
					Key:         "hourly-status",
					Status:      types.SensorStatusComplete,
					LastUpdated: baseTime,
					Metadata: map[string]string{
						"sensor_count":            "1500",
						"__baseline_sensor_count": "1000",
					},
				},
			},
			sensorKeys:     []string{"hourly-status"},
			triggerStatus:  "running",
			wantEventType:  "POST_RUN_DRIFT_INFLIGHT",
			wantEventCount: 1,
		},
		{
			name: "PostRunRulesPassed_StatusComplete",
			sensors: map[string]adapter.SensorData{
				"post-run-status": {
					Key:         "post-run-status",
					Status:      types.SensorStatusComplete,
					LastUpdated: baseTime,
					Metadata: map[string]string{
						"status": "COMPLETE",
					},
				},
			},
			sensorKeys:     []string{"post-run-status"},
			triggerStatus:  "COMPLETED",
			wantEventType:  "POST_RUN_PASSED",
			wantEventCount: 1,
		},
		{
			name: "PostRunRulesFailed_StatusPartial",
			sensors: map[string]adapter.SensorData{
				"post-run-status": {
					Key:         "post-run-status",
					Status:      types.SensorStatusComplete,
					LastUpdated: baseTime,
					Metadata: map[string]string{
						"status": "PARTIAL",
					},
				},
			},
			sensorKeys:     []string{"post-run-status"},
			triggerStatus:  "COMPLETED",
			wantEventType:  "POST_RUN_FAILED",
			wantEventCount: 1,
		},
		{
			name: "SensorMissingTimeout_PostRunStatusStale",
			sensors: map[string]adapter.SensorData{
				"post-run-status": {
					Key:         "post-run-status",
					Status:      types.SensorStatusComplete,
					LastUpdated: baseTime.Add(-3600 * time.Second), // 1 hour stale
					Metadata: map[string]string{
						"status": "COMPLETE",
					},
				},
			},
			sensorKeys:     []string{"post-run-status"},
			triggerStatus:  "COMPLETED",
			wantEventType:  "POST_RUN_SENSOR_MISSING",
			wantEventCount: 1,
		},
		{
			name: "BaselineCaptured_AllSensorsFresh",
			sensors: map[string]adapter.SensorData{
				"hourly-status": {
					Key:         "hourly-status",
					Status:      types.SensorStatusComplete,
					LastUpdated: baseTime,
					Metadata: map[string]string{
						"status":       "COMPLETE",
						"sensor_count": "1000",
					},
				},
			},
			sensorKeys:     []string{"hourly-status"},
			triggerStatus:  "COMPLETED",
			wantEventType:  "POST_RUN_BASELINE_CAPTURED",
			wantEventCount: 1,
		},
		{
			name: "BaselineCaptureFailed_SensorStale",
			sensors: map[string]adapter.SensorData{
				"hourly-status": {
					Key:         "hourly-status",
					Status:      types.SensorStatusComplete,
					LastUpdated: baseTime.Add(-7200 * time.Second), // 2 hours stale
					Metadata: map[string]string{
						"status":       "COMPLETE",
						"sensor_count": "1000",
					},
				},
			},
			sensorKeys:     []string{"hourly-status"},
			triggerStatus:  "COMPLETED",
			wantEventType:  "BASELINE_CAPTURE_FAILED",
			wantEventCount: 1,
		},
		{
			name:           "TriggerNotTerminalOrRunning_Skip",
			sensors:        map[string]adapter.SensorData{},
			sensorKeys:     []string{},
			triggerStatus:  "pending",
			wantEventType:  "",
			wantEventCount: 0,
		},
		{
			name: "ShortCircuit_ExistingEvents",
			sensors: map[string]adapter.SensorData{
				"hourly-status": {
					Key:         "hourly-status",
					Status:      types.SensorStatusComplete,
					LastUpdated: baseTime,
					Metadata: map[string]string{
						"sensor_count":            "1500",
						"__baseline_sensor_count": "1000",
					},
				},
			},
			sensorKeys:     []string{"hourly-status"},
			triggerStatus:  "COMPLETED",
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
			mod := NewPostRunModule()

			ctx := context.Background()
			pipeline := "postrun-test-" + tc.name

			// Write sensor data.
			for key, sd := range tc.sensors {
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

			// Pre-emit an event for short-circuit test.
			if tc.preEmitEvent != "" {
				reader.Emit(InterlockEventRecord{
					PipelineID: pipeline,
					EventType:  tc.preEmitEvent,
					Timestamp:  clk.Now(),
				})
			}

			err := mod.Evaluate(ctx, EvalParams{
				Pipeline:    pipeline,
				Config:      map[string]any{},
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
					t.Fatalf("expected 1 %s event, got %d; all events: %+v",
						tc.wantEventType, len(typed), allEvents)
				}
			}
		})
	}
}

func TestPostRunModule_Name(t *testing.T) {
	t.Parallel()
	mod := NewPostRunModule()
	if got := mod.Name(); got != "postrun" {
		t.Errorf("Name() = %q, want %q", got, "postrun")
	}
}
