package interlocksuite

import (
	"context"
	"testing"
	"time"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

func TestSLAModule(t *testing.T) {
	t.Parallel()

	// Deadline is "06:00" UTC. Position the clock relative to that.
	deadline := "06:00"

	tests := []struct {
		name           string
		clockTime      time.Time
		triggerStatus  string
		slaConfig      map[string]any
		sensorKeys     []string
		sensorMeta     map[string]string // metadata for first sensor
		preEmitEvent   string            // pre-emit to test short-circuit
		wantEventType  string            // expected event ("" = none)
		wantEventCount int
	}{
		{
			name:      "SLA_WARNING_approaching_deadline",
			clockTime: time.Date(2026, 3, 28, 5, 45, 0, 0, time.UTC), // 15 min before 06:00
			slaConfig: map[string]any{
				"deadline":        deadline,
				"warning_minutes": 30,
			},
			sensorKeys:     []string{"hourly-status"},
			sensorMeta:     map[string]string{"status": "PENDING"},
			wantEventType:  "SLA_WARNING",
			wantEventCount: 1,
		},
		{
			name:      "SLA_BREACH_past_deadline",
			clockTime: time.Date(2026, 3, 28, 6, 30, 0, 0, time.UTC), // 30 min after 06:00
			slaConfig: map[string]any{
				"deadline":        deadline,
				"warning_minutes": 30,
			},
			sensorKeys:     []string{"hourly-status"},
			sensorMeta:     map[string]string{"status": "PENDING"},
			wantEventType:  "SLA_BREACH",
			wantEventCount: 1,
		},
		{
			name:          "SLA_MET_completed_before_deadline",
			clockTime:     time.Date(2026, 3, 28, 5, 45, 0, 0, time.UTC), // before 06:00
			triggerStatus: "COMPLETED",
			slaConfig: map[string]any{
				"deadline":        deadline,
				"warning_minutes": 30,
			},
			sensorKeys:     []string{"hourly-status"},
			sensorMeta:     map[string]string{"status": "COMPLETE"},
			wantEventType:  "SLA_MET",
			wantEventCount: 1,
		},
		{
			name:      "SLA_WARNING_suppressed_breach_fired",
			clockTime: time.Date(2026, 3, 28, 5, 45, 0, 0, time.UTC), // in warning window
			slaConfig: map[string]any{
				"deadline":        deadline,
				"warning_minutes": 30,
				"breach_fired":    true,
			},
			sensorKeys:     []string{"hourly-status"},
			sensorMeta:     map[string]string{"status": "PENDING"},
			wantEventType:  "",
			wantEventCount: 0,
		},
		{
			name:          "SLA_BREACH_suppressed_terminal_trigger",
			clockTime:     time.Date(2026, 3, 28, 6, 30, 0, 0, time.UTC), // past deadline
			triggerStatus: "COMPLETED",
			slaConfig: map[string]any{
				"deadline":        deadline,
				"warning_minutes": 30,
			},
			sensorKeys:     []string{"hourly-status"},
			sensorMeta:     map[string]string{"status": "COMPLETE"},
			wantEventType:  "",
			wantEventCount: 0,
		},
		{
			name:      "RELATIVE_SLA_WARNING_approaching_max_duration",
			clockTime: time.Date(2026, 3, 28, 10, 50, 0, 0, time.UTC), // 50 min after start
			slaConfig: map[string]any{
				"max_duration_minutes": 60,
				"warning_pct":          80,
			},
			sensorKeys: []string{"hourly-status"},
			sensorMeta: map[string]string{
				"status":        "COMPLETE",
				"schedule_time": "2026-03-28T10:00:00Z", // started 50 min ago, > 80% of 60 min
			},
			wantEventType:  "RELATIVE_SLA_WARNING",
			wantEventCount: 1,
		},
		{
			name:      "RELATIVE_SLA_BREACH_past_max_duration",
			clockTime: time.Date(2026, 3, 28, 11, 05, 0, 0, time.UTC), // 65 min after start
			slaConfig: map[string]any{
				"max_duration_minutes": 60,
				"warning_pct":          80,
			},
			sensorKeys: []string{"hourly-status"},
			sensorMeta: map[string]string{
				"status":        "COMPLETE",
				"schedule_time": "2026-03-28T10:00:00Z", // started 65 min ago
			},
			wantEventType:  "RELATIVE_SLA_BREACH",
			wantEventCount: 1,
		},
		{
			name:           "NoSLASection_skips",
			clockTime:      time.Date(2026, 3, 28, 5, 45, 0, 0, time.UTC),
			slaConfig:      nil, // no sla section
			sensorKeys:     []string{"hourly-status"},
			sensorMeta:     map[string]string{"status": "PENDING"},
			wantEventType:  "",
			wantEventCount: 0,
		},
		{
			name:      "ShortCircuit_existing_events",
			clockTime: time.Date(2026, 3, 28, 5, 45, 0, 0, time.UTC),
			slaConfig: map[string]any{
				"deadline":        deadline,
				"warning_minutes": 30,
			},
			sensorKeys:     []string{"hourly-status"},
			sensorMeta:     map[string]string{"status": "PENDING"},
			preEmitEvent:   "JOB_TRIGGERED",
			wantEventType:  "",
			wantEventCount: 1, // only the pre-emitted event
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			store := newTestSQLiteStore(t)
			clk := adapter.NewTestClock(tc.clockTime)
			reader := NewLocalEventReader()
			mod := NewSLAModule()

			ctx := context.Background()
			pipeline := "sla-test-" + tc.name

			// Write sensor data.
			for _, key := range tc.sensorKeys {
				meta := tc.sensorMeta
				if meta == nil {
					meta = map[string]string{}
				}
				status := types.SensorStatus(meta["status"])
				if err := store.WriteSensor(ctx, pipeline, key, adapter.SensorData{
					Pipeline:    pipeline,
					Key:         key,
					Status:      status,
					LastUpdated: clk.Now(),
					Metadata:    meta,
				}); err != nil {
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

			// Build config.
			config := map[string]any{}
			if tc.slaConfig != nil {
				config["sla"] = tc.slaConfig
			}

			// Pre-emit an event for short-circuit testing.
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
					t.Fatalf("expected 1 %s event, got %d: %+v",
						tc.wantEventType, len(typed), allEvents)
				}
			}
		})
	}
}

func TestSLAModule_Helpers(t *testing.T) {
	t.Parallel()

	t.Run("parseDeadline", func(t *testing.T) {
		t.Parallel()
		now := time.Date(2026, 3, 28, 10, 0, 0, 0, time.UTC)

		got, err := parseDeadline(now, "06:00")
		if err != nil {
			t.Fatalf("parseDeadline: %v", err)
		}
		want := time.Date(2026, 3, 28, 6, 0, 0, 0, time.UTC)
		if !got.Equal(want) {
			t.Errorf("parseDeadline = %v, want %v", got, want)
		}
	})

	t.Run("parseDeadline_invalid", func(t *testing.T) {
		t.Parallel()
		now := time.Date(2026, 3, 28, 10, 0, 0, 0, time.UTC)
		if _, err := parseDeadline(now, "bad"); err == nil {
			t.Error("expected error for invalid deadline format")
		}
	})

	t.Run("isTerminalStatus", func(t *testing.T) {
		t.Parallel()
		terminals := []string{"completed", "COMPLETED", "failed", "FAILED", "succeeded", "killed", "timeout"}
		for _, s := range terminals {
			if !isTerminalStatus(s) {
				t.Errorf("isTerminalStatus(%q) = false, want true", s)
			}
		}
		nonTerminals := []string{"RUNNING", "PENDING", "", "READY"}
		for _, s := range nonTerminals {
			if isTerminalStatus(s) {
				t.Errorf("isTerminalStatus(%q) = true, want false", s)
			}
		}
	})

	t.Run("getConfigFloat_defaults", func(t *testing.T) {
		t.Parallel()
		m := map[string]any{"a": 10, "b": float64(3.14)}
		if got := getConfigFloat(m, "a", 0); got != 10 {
			t.Errorf("getConfigFloat(a) = %v, want 10", got)
		}
		if got := getConfigFloat(m, "b", 0); got != 3.14 {
			t.Errorf("getConfigFloat(b) = %v, want 3.14", got)
		}
		if got := getConfigFloat(m, "missing", 42); got != 42 {
			t.Errorf("getConfigFloat(missing) = %v, want 42", got)
		}
	})

	t.Run("getConfigBool", func(t *testing.T) {
		t.Parallel()
		m := map[string]any{"yes": true, "no": false, "str": "true"}
		if !getConfigBool(m, "yes") {
			t.Error("getConfigBool(yes) = false, want true")
		}
		if getConfigBool(m, "no") {
			t.Error("getConfigBool(no) = true, want false")
		}
		if getConfigBool(m, "str") {
			t.Error("getConfigBool(str) should return false for non-bool")
		}
		if getConfigBool(m, "missing") {
			t.Error("getConfigBool(missing) should return false")
		}
	})

	t.Run("getConfigString", func(t *testing.T) {
		t.Parallel()
		m := map[string]any{"key": "val", "num": 42}
		if got := getConfigString(m, "key"); got != "val" {
			t.Errorf("getConfigString(key) = %q, want %q", got, "val")
		}
		if got := getConfigString(m, "num"); got != "" {
			t.Errorf("getConfigString(num) = %q, want empty", got)
		}
		if got := getConfigString(m, "missing"); got != "" {
			t.Errorf("getConfigString(missing) = %q, want empty", got)
		}
	})
}

func TestSLAModule_RelativeDuration_FallbackToLastUpdated(t *testing.T) {
	t.Parallel()

	// When no schedule_time or trigger_time in metadata, the module should
	// fall back to sensor.LastUpdated as the trigger start time.
	startTime := time.Date(2026, 3, 28, 10, 0, 0, 0, time.UTC)
	now := time.Date(2026, 3, 28, 10, 50, 0, 0, time.UTC) // 50 min elapsed

	store := newTestSQLiteStore(t)
	clk := adapter.NewTestClock(now)
	reader := NewLocalEventReader()
	mod := NewSLAModule()

	ctx := context.Background()
	pipeline := "sla-relative-fallback"

	// Write sensor WITHOUT schedule_time/trigger_time, but with LastUpdated
	// set to the start time.
	if err := store.WriteSensor(ctx, pipeline, "s1", adapter.SensorData{
		Pipeline:    pipeline,
		Key:         "s1",
		Status:      types.SensorStatusComplete,
		LastUpdated: startTime,
		Metadata:    map[string]string{"status": "COMPLETE"},
	}); err != nil {
		t.Fatalf("WriteSensor: %v", err)
	}

	config := map[string]any{
		"sla": map[string]any{
			"max_duration_minutes": 60,
			"warning_pct":          80,
		},
	}

	err := mod.Evaluate(ctx, EvalParams{
		Pipeline:    pipeline,
		Config:      config,
		Store:       store,
		EventWriter: reader,
		Clock:       clk,
		SensorKeys:  []string{"s1"},
	})
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}

	// 50 min of 60 = 83.3%, above 80% warning threshold -> RELATIVE_SLA_WARNING
	events, _ := reader.ReadEvents(ctx, pipeline, "RELATIVE_SLA_WARNING")
	if len(events) != 1 {
		all, _ := reader.ReadEvents(ctx, pipeline, "")
		t.Fatalf("expected 1 RELATIVE_SLA_WARNING, got %d: %+v", len(events), all)
	}
}
