package interlocksuite

import (
	"context"
	"testing"
	"time"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

func TestRecoveryModule(t *testing.T) {
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
		// Pre-emit events to test short-circuit / reactive behavior.
		preEmitEvents []string
		// Expected emitted event type ("" = none).
		wantEventType string
		// Whether to verify rerun was written.
		wantRerun bool
	}{
		{
			name:      "RerunAccepted_CurrentLessThanMax",
			clockTime: baseTime,
			config: map[string]any{
				"recovery": map[string]any{
					"max_drift_reruns":     float64(3),
					"current_drift_reruns": float64(1),
				},
			},
			triggerStatus: "COMPLETED",
			wantEventType: "RERUN_ACCEPTED",
		},
		{
			name:      "RerunRejected_CurrentEqualsMax",
			clockTime: baseTime,
			config: map[string]any{
				"recovery": map[string]any{
					"max_drift_reruns":     float64(3),
					"current_drift_reruns": float64(3),
				},
			},
			triggerStatus: "COMPLETED",
			wantEventType: "RERUN_REJECTED",
		},
		{
			name:      "RetryExhausted_CurrentExceedsMax",
			clockTime: baseTime,
			config: map[string]any{
				"recovery": map[string]any{
					"max_retries":     float64(5),
					"current_retries": float64(5),
				},
			},
			triggerStatus: "FAILED",
			wantEventType: "RETRY_EXHAUSTED",
		},
		{
			name:      "PollTimeoutExceeded",
			clockTime: baseTime.Add(120 * time.Minute), // 120 min after start
			config: map[string]any{
				"recovery": map[string]any{
					"poll_timeout_minutes": float64(60),
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
			triggerStatus: "RUNNING",
			wantEventType: "JOB_POLL_EXHAUSTED",
		},
		{
			name:      "JobFailureRetry_FalseSuccessDetected",
			clockTime: baseTime,
			config: map[string]any{
				"recovery": map[string]any{
					"max_code_retries": float64(3),
					"current_retries":  float64(1),
				},
			},
			triggerStatus: "succeeded",
			wantEventType: "JOB_FAILED",
			wantRerun:     true,
		},
		{
			name:      "TriggerRecovered_SensorsReady",
			clockTime: baseTime,
			config: map[string]any{
				"recovery": map[string]any{
					"stale_threshold_minutes": float64(30),
				},
			},
			sensors: map[string]adapter.SensorData{
				"hourly-status": {
					Key:         "hourly-status",
					Status:      types.SensorStatusComplete,
					LastUpdated: baseTime,
				},
			},
			sensorKeys:    []string{"hourly-status"},
			triggerStatus: "RUNNING",
			wantEventType: "TRIGGER_RECOVERED",
		},
		{
			name:      "JobFailed_KilledTrigger",
			clockTime: baseTime,
			config: map[string]any{
				"recovery": map[string]any{
					"max_retries":     float64(5),
					"current_retries": float64(0),
				},
			},
			triggerStatus: "killed",
			wantEventType: "JOB_FAILED",
		},
		{
			name:      "JobFailed_TimeoutTrigger",
			clockTime: baseTime,
			config: map[string]any{
				"recovery": map[string]any{
					"max_retries":     float64(5),
					"current_retries": float64(0),
				},
			},
			triggerStatus: "timeout",
			wantEventType: "JOB_FAILED",
		},
		{
			name:      "JobFailed_KilledUppercase",
			clockTime: baseTime,
			config: map[string]any{
				"recovery": map[string]any{
					"max_retries":     float64(3),
					"current_retries": float64(1),
				},
			},
			triggerStatus: "KILLED",
			wantEventType: "JOB_FAILED",
		},
		{
			name:      "JobFailed_TimeoutUppercase",
			clockTime: baseTime,
			config: map[string]any{
				"recovery": map[string]any{
					"max_retries":     float64(3),
					"current_retries": float64(1),
				},
			},
			triggerStatus: "TIMEOUT",
			wantEventType: "JOB_FAILED",
		},
		{
			name:      "NoRecoveryConfig_Skips",
			clockTime: baseTime,
			config: map[string]any{
				"job": map[string]any{"type": "command"},
			},
			wantEventType: "",
		},
		{
			name:      "TerminalEvent_ShortCircuits",
			clockTime: baseTime,
			config: map[string]any{
				"recovery": map[string]any{
					"max_drift_reruns":     float64(3),
					"current_drift_reruns": float64(1),
				},
			},
			triggerStatus: "COMPLETED",
			preEmitEvents: []string{"RERUN_REJECTED"},
			wantEventType: "", // short-circuited by terminal event
		},
		{
			name:      "RecoverableEvent_DoesNotShortCircuit",
			clockTime: baseTime,
			config: map[string]any{
				"recovery": map[string]any{
					"max_drift_reruns":     float64(3),
					"current_drift_reruns": float64(1),
				},
			},
			triggerStatus: "COMPLETED",
			preEmitEvents: []string{"POST_RUN_DRIFT"},
			wantEventType: "RERUN_ACCEPTED", // NOT short-circuited
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			store := newTestSQLiteStore(t)
			clk := adapter.NewTestClock(tc.clockTime)
			reader := NewLocalEventReader()
			mod := NewRecoveryModule()

			ctx := context.Background()
			pipeline := "recovery-test-" + tc.name

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

			// Pre-emit events for short-circuit / reactive tests.
			for _, evt := range tc.preEmitEvents {
				reader.Emit(InterlockEventRecord{
					PipelineID: pipeline,
					EventType:  evt,
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
			} else if len(tc.preEmitEvents) == 0 {
				// No events should be emitted.
				allEvents, _ := reader.ReadEvents(ctx, pipeline, "")
				if len(allEvents) != 0 {
					t.Fatalf("expected 0 events, got %d: %+v", len(allEvents), allEvents)
				}
			}

			// Check rerun was written if expected.
			if tc.wantRerun {
				count, rerr := store.CountReruns(ctx, pipeline, "default", "default")
				if rerr != nil {
					t.Fatalf("CountReruns: %v", rerr)
				}
				if count < 1 {
					t.Fatalf("expected at least 1 rerun, got %d", count)
				}
			}
		})
	}
}

func TestConfigInt(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		m          map[string]any
		key        string
		defaultVal int
		want       int
	}{
		{"float64_value", map[string]any{"x": float64(42)}, "x", 0, 42},
		{"int_value", map[string]any{"x": 10}, "x", 0, 10},
		{"string_value", map[string]any{"x": "7"}, "x", 0, 7},
		{"missing_key", map[string]any{}, "x", 5, 5},
		{"bad_string", map[string]any{"x": "abc"}, "x", 3, 3},
		{"bool_value", map[string]any{"x": true}, "x", 9, 9},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := configInt(tc.m, tc.key, tc.defaultVal)
			if got != tc.want {
				t.Fatalf("got %d, want %d", got, tc.want)
			}
		})
	}
}
