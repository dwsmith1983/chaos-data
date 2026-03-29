package mutation_test

import (
	"context"
	"testing"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/mutation"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

func TestTriggerTimeoutMutation_Type(t *testing.T) {
	store := newMockStateStore()
	m := mutation.NewTriggerTimeoutMutation(store)
	if got := m.Type(); got != "trigger-timeout" {
		t.Errorf("Type() = %q, want %q", got, "trigger-timeout")
	}
}

func TestTriggerTimeoutMutation_Apply(t *testing.T) {
	tests := []struct {
		name        string
		params      map[string]string
		wantApplied bool
		wantErr     bool
		wantStatus  string
		wantTrigger adapter.TriggerKey
	}{
		{
			name: "writes timeout status with all params",
			params: map[string]string{
				"pipeline":         "etl-daily",
				"schedule":         "daily-06",
				"date":             "2026-03-14",
				"timeout_duration": "1h",
			},
			wantApplied: true,
			wantStatus:  "timeout",
			wantTrigger: adapter.TriggerKey{
				Pipeline: "etl-daily",
				Schedule: "daily-06",
				Date:     "2026-03-14",
			},
		},
		{
			name: "uses default timeout_duration 30m",
			params: map[string]string{
				"pipeline": "etl-daily",
				"schedule": "daily-06",
				"date":     "2026-03-14",
			},
			wantApplied: true,
			wantStatus:  "timeout",
			wantTrigger: adapter.TriggerKey{
				Pipeline: "etl-daily",
				Schedule: "daily-06",
				Date:     "2026-03-14",
			},
		},
		{
			name: "missing pipeline returns error",
			params: map[string]string{
				"schedule": "daily-06",
				"date":     "2026-03-14",
			},
			wantApplied: false,
			wantErr:     true,
		},
		{
			name: "missing schedule returns error",
			params: map[string]string{
				"pipeline": "etl-daily",
				"date":     "2026-03-14",
			},
			wantApplied: false,
			wantErr:     true,
		},
		{
			name: "missing date returns error",
			params: map[string]string{
				"pipeline": "etl-daily",
				"schedule": "daily-06",
			},
			wantApplied: false,
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newMockStateStore()
			transport := newMockTransport()
			obj := types.DataObject{Key: "test/data.jsonl"}

			m := mutation.NewTriggerTimeoutMutation(store)
			record, err := m.Apply(context.Background(), obj, transport, tt.params, adapter.NewWallClock())

			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if record.Applied {
					t.Error("expected Applied=false on error")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if record.Applied != tt.wantApplied {
				t.Errorf("Applied = %v, want %v", record.Applied, tt.wantApplied)
			}
			if record.Mutation != "trigger-timeout" {
				t.Errorf("Mutation = %q, want %q", record.Mutation, "trigger-timeout")
			}

			// Verify WriteTriggerStatus was called with "timeout".
			if got := store.stateCallCount("WriteTriggerStatus"); got != 1 {
				t.Fatalf("WriteTriggerStatus call count = %d, want 1", got)
			}

			calls := store.getStateCalls()
			for _, c := range calls {
				if c.Method == "WriteTriggerStatus" {
					if c.Status != tt.wantStatus {
						t.Errorf("status = %q, want %q", c.Status, tt.wantStatus)
					}
					if c.Trigger != tt.wantTrigger {
						t.Errorf("trigger key = %+v, want %+v", c.Trigger, tt.wantTrigger)
					}
				}
			}
		})
	}
}

func TestTriggerTimeoutMutation_DefaultTimeoutDuration(t *testing.T) {
	store := newMockStateStore()
	transport := newMockTransport()
	obj := types.DataObject{Key: "test/data.jsonl"}

	m := mutation.NewTriggerTimeoutMutation(store)
	record, err := m.Apply(context.Background(), obj, transport, map[string]string{
		"pipeline": "etl-daily",
		"schedule": "daily-06",
		"date":     "2026-03-14",
	}, adapter.NewWallClock())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if record.Params["timeout_duration"] != "30m" {
		t.Errorf("timeout_duration = %q, want %q", record.Params["timeout_duration"], "30m")
	}
}

func TestTriggerTimeoutMutation_WriteTriggerStatusError(t *testing.T) {
	store := newMockStateStore()
	store.WriteTriggerStatusErr = true
	transport := newMockTransport()
	obj := types.DataObject{Key: "test/data.jsonl"}

	m := mutation.NewTriggerTimeoutMutation(store)
	record, err := m.Apply(context.Background(), obj, transport, map[string]string{
		"pipeline": "etl-daily",
		"schedule": "daily-06",
		"date":     "2026-03-14",
	}, adapter.NewWallClock())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if record.Applied {
		t.Error("expected Applied=false on error")
	}
}
