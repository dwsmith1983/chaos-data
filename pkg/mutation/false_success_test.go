package mutation_test

import (
	"context"
	"testing"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/mutation"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

func TestFalseSuccessMutation_Type(t *testing.T) {
	store := newMockStateStore()
	m := mutation.NewFalseSuccessMutation(store)
	if got := m.Type(); got != "false-success" {
		t.Errorf("Type() = %q, want %q", got, "false-success")
	}
}

func TestFalseSuccessMutation_Apply(t *testing.T) {
	tests := []struct {
		name        string
		params      map[string]string
		wantApplied bool
		wantErr     bool
		wantStatus  string
		wantTrigger adapter.TriggerKey
	}{
		{
			name: "writes succeeded status with all params",
			params: map[string]string{
				"pipeline":       "etl-daily",
				"schedule":       "daily-06",
				"date":           "2026-03-14",
				"job_type":       "spark",
				"missing_output": "s3://bucket/output/report.parquet",
			},
			wantApplied: true,
			wantStatus:  "succeeded",
			wantTrigger: adapter.TriggerKey{
				Pipeline: "etl-daily",
				Schedule: "daily-06",
				Date:     "2026-03-14",
			},
		},
		{
			name: "uses default job_type glue",
			params: map[string]string{
				"pipeline": "etl-daily",
				"schedule": "daily-06",
				"date":     "2026-03-14",
			},
			wantApplied: true,
			wantStatus:  "succeeded",
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

			m := mutation.NewFalseSuccessMutation(store)
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
			if record.Mutation != "false-success" {
				t.Errorf("Mutation = %q, want %q", record.Mutation, "false-success")
			}

			// Verify WriteTriggerStatus was called with "succeeded".
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

func TestFalseSuccessMutation_DefaultParams(t *testing.T) {
	store := newMockStateStore()
	transport := newMockTransport()
	obj := types.DataObject{Key: "test/data.jsonl"}

	m := mutation.NewFalseSuccessMutation(store)
	record, err := m.Apply(context.Background(), obj, transport, map[string]string{
		"pipeline": "etl-daily",
		"schedule": "daily-06",
		"date":     "2026-03-14",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if record.Params["job_type"] != "glue" {
		t.Errorf("job_type = %q, want %q", record.Params["job_type"], "glue")
	}
}

func TestFalseSuccessMutation_MissingOutputRecorded(t *testing.T) {
	store := newMockStateStore()
	transport := newMockTransport()
	obj := types.DataObject{Key: "test/data.jsonl"}

	m := mutation.NewFalseSuccessMutation(store)
	record, err := m.Apply(context.Background(), obj, transport, map[string]string{
		"pipeline":       "etl-daily",
		"schedule":       "daily-06",
		"date":           "2026-03-14",
		"missing_output": "s3://bucket/output/report.parquet",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if record.Params["missing_output"] != "s3://bucket/output/report.parquet" {
		t.Errorf("missing_output = %q, want %q", record.Params["missing_output"], "s3://bucket/output/report.parquet")
	}
}

func TestFalseSuccessMutation_WriteTriggerStatusError(t *testing.T) {
	store := newMockStateStore()
	store.writeTriggerStatusErr = true
	transport := newMockTransport()
	obj := types.DataObject{Key: "test/data.jsonl"}

	m := mutation.NewFalseSuccessMutation(store)
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
