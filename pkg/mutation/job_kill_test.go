package mutation_test

import (
	"context"
	"testing"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/mutation"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

func TestJobKillMutation_Type(t *testing.T) {
	store := newMockStateStore()
	m := mutation.NewJobKillMutation(store)
	if got := m.Type(); got != "job-kill" {
		t.Errorf("Type() = %q, want %q", got, "job-kill")
	}
}

func TestJobKillMutation_Apply(t *testing.T) {
	tests := []struct {
		name        string
		params      map[string]string
		wantApplied bool
		wantErr     bool
		wantStatus  string
		wantTrigger adapter.TriggerKey
	}{
		{
			name: "writes killed status with all params",
			params: map[string]string{
				"pipeline":       "etl-daily",
				"schedule":       "daily-06",
				"date":           "2026-03-14",
				"kill_after_pct": "75",
				"job_type":       "spark",
			},
			wantApplied: true,
			wantStatus:  "killed",
			wantTrigger: adapter.TriggerKey{
				Pipeline: "etl-daily",
				Schedule: "daily-06",
				Date:     "2026-03-14",
			},
		},
		{
			name: "uses default kill_after_pct and job_type",
			params: map[string]string{
				"pipeline": "etl-daily",
				"schedule": "daily-06",
				"date":     "2026-03-14",
			},
			wantApplied: true,
			wantStatus:  "killed",
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

			m := mutation.NewJobKillMutation(store)
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
			if record.Mutation != "job-kill" {
				t.Errorf("Mutation = %q, want %q", record.Mutation, "job-kill")
			}

			// Verify WriteTriggerStatus was called with "killed".
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

func TestJobKillMutation_DefaultParams(t *testing.T) {
	store := newMockStateStore()
	transport := newMockTransport()
	obj := types.DataObject{Key: "test/data.jsonl"}

	m := mutation.NewJobKillMutation(store)
	record, err := m.Apply(context.Background(), obj, transport, map[string]string{
		"pipeline": "etl-daily",
		"schedule": "daily-06",
		"date":     "2026-03-14",
	}, adapter.NewWallClock())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify default params are recorded.
	if record.Params["kill_after_pct"] != "50" {
		t.Errorf("kill_after_pct = %q, want %q", record.Params["kill_after_pct"], "50")
	}
	if record.Params["job_type"] != "glue" {
		t.Errorf("job_type = %q, want %q", record.Params["job_type"], "glue")
	}
}

func TestJobKillMutation_CustomParams(t *testing.T) {
	store := newMockStateStore()
	transport := newMockTransport()
	obj := types.DataObject{Key: "test/data.jsonl"}

	m := mutation.NewJobKillMutation(store)
	record, err := m.Apply(context.Background(), obj, transport, map[string]string{
		"pipeline":       "etl-daily",
		"schedule":       "daily-06",
		"date":           "2026-03-14",
		"kill_after_pct": "25",
		"job_type":       "emr",
	}, adapter.NewWallClock())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if record.Params["kill_after_pct"] != "25" {
		t.Errorf("kill_after_pct = %q, want %q", record.Params["kill_after_pct"], "25")
	}
	if record.Params["job_type"] != "emr" {
		t.Errorf("job_type = %q, want %q", record.Params["job_type"], "emr")
	}
}

func TestJobKillMutation_WriteTriggerStatusError(t *testing.T) {
	store := newMockStateStore()
	store.WriteTriggerStatusErr = true
	transport := newMockTransport()
	obj := types.DataObject{Key: "test/data.jsonl"}

	m := mutation.NewJobKillMutation(store)
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
