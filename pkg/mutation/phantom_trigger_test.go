package mutation_test

import (
	"context"
	"testing"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/mutation"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

func TestPhantomTriggerMutation_Type(t *testing.T) {
	store := newMockStateStore()
	m := mutation.NewPhantomTriggerMutation(store)
	if got := m.Type(); got != "phantom-trigger" {
		t.Errorf("Type() = %q, want %q", got, "phantom-trigger")
	}
}

func TestPhantomTriggerMutation_Apply(t *testing.T) {
	tests := []struct {
		name        string
		params      map[string]string
		wantApplied bool
		wantErr     bool
		wantStatus  string
		wantTrigger adapter.TriggerKey
	}{
		{
			name: "writes triggered status with all params",
			params: map[string]string{
				"pipeline":     "etl-daily",
				"schedule":     "daily-06",
				"date":         "2026-03-14",
				"trigger_type": "manual",
			},
			wantApplied: true,
			wantStatus:  "triggered",
			wantTrigger: adapter.TriggerKey{
				Pipeline: "etl-daily",
				Schedule: "daily-06",
				Date:     "2026-03-14",
			},
		},
		{
			name: "uses default trigger_type scheduled",
			params: map[string]string{
				"pipeline": "etl-daily",
				"schedule": "daily-06",
				"date":     "2026-03-14",
			},
			wantApplied: true,
			wantStatus:  "triggered",
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

			m := mutation.NewPhantomTriggerMutation(store)
			record, err := m.Apply(context.Background(), obj, transport, tt.params)

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
			if record.Mutation != "phantom-trigger" {
				t.Errorf("Mutation = %q, want %q", record.Mutation, "phantom-trigger")
			}

			// Verify WriteTriggerStatus was called.
			if got := store.stateCallCount("WriteTriggerStatus"); got != 1 {
				t.Fatalf("WriteTriggerStatus call count = %d, want 1", got)
			}

			// Verify the trigger key and status.
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

func TestPhantomTriggerMutation_WriteTriggerStatusError(t *testing.T) {
	store := newMockStateStore()
	store.writeTriggerStatusErr = true
	transport := newMockTransport()
	obj := types.DataObject{Key: "test/data.jsonl"}

	m := mutation.NewPhantomTriggerMutation(store)
	record, err := m.Apply(context.Background(), obj, transport, map[string]string{
		"pipeline": "etl-daily",
		"schedule": "daily-06",
		"date":     "2026-03-14",
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if record.Applied {
		t.Error("expected Applied=false on error")
	}
}

func TestPhantomTriggerMutation_ParamsRecorded(t *testing.T) {
	store := newMockStateStore()
	transport := newMockTransport()
	obj := types.DataObject{Key: "test/data.jsonl"}

	params := map[string]string{
		"pipeline":     "etl-daily",
		"schedule":     "daily-06",
		"date":         "2026-03-14",
		"trigger_type": "manual",
	}

	m := mutation.NewPhantomTriggerMutation(store)
	record, err := m.Apply(context.Background(), obj, transport, params)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify params are recorded in the mutation record.
	if record.Params["trigger_type"] != "manual" {
		t.Errorf("recorded trigger_type = %q, want %q", record.Params["trigger_type"], "manual")
	}
	if record.Params["pipeline"] != "etl-daily" {
		t.Errorf("recorded pipeline = %q, want %q", record.Params["pipeline"], "etl-daily")
	}
}
