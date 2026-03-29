package mutation_test

import (
	"context"
	"strings"
	"testing"

	"github.com/dwsmith1983/chaos-data/pkg/mutation"
	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

func TestMultiDayMutation_Type(t *testing.T) {
	m := &mutation.MultiDayMutation{}
	if got := m.Type(); got != "multi-day" {
		t.Errorf("Type() = %q, want %q", got, "multi-day")
	}
}

func TestMultiDayMutation_Apply(t *testing.T) {
	inputData := []byte(`{"id":1,"name":"Alice"}` + "\n")

	tests := []struct {
		name           string
		params         map[string]string
		wantApplied    bool
		wantErr        bool
		wantWriteCount int
		wantKeys       []string
	}{
		{
			name:           "three days produces three writes",
			params:         map[string]string{"days": "2024-01-15,2024-01-16,2024-01-17"},
			wantApplied:    true,
			wantWriteCount: 3,
			wantKeys: []string{
				"date=2024-01-15/data/records.jsonl",
				"date=2024-01-16/data/records.jsonl",
				"date=2024-01-17/data/records.jsonl",
			},
		},
		{
			name:           "single day produces one write",
			params:         map[string]string{"days": "2024-03-01"},
			wantApplied:    true,
			wantWriteCount: 1,
			wantKeys:       []string{"date=2024-03-01/data/records.jsonl"},
		},
		{
			name:           "with prefix",
			params:         map[string]string{"days": "2024-01-15", "prefix": "staging"},
			wantApplied:    true,
			wantWriteCount: 1,
			wantKeys:       []string{"staging/date=2024-01-15/data/records.jsonl"},
		},
		{
			name:        "missing days returns error",
			params:      map[string]string{},
			wantApplied: false,
			wantErr:     true,
		},
		{
			name:        "empty days returns error",
			params:      map[string]string{"days": ""},
			wantApplied: false,
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transport := newMockTransport()
			obj := types.DataObject{Key: "data/records.jsonl"}
			transport.ReadData[obj.Key] = inputData

			m := &mutation.MultiDayMutation{}
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
			if record.Mutation != "multi-day" {
				t.Errorf("Mutation = %q, want %q", record.Mutation, "multi-day")
			}
			if got := transport.callCount("Write"); got != tt.wantWriteCount {
				t.Errorf("Write call count = %d, want %d", got, tt.wantWriteCount)
			}

			// Verify Write keys.
			if len(tt.wantKeys) > 0 {
				calls := transport.getCalls()
				writeKeys := make(map[string]bool)
				for _, c := range calls {
					if c.Method == "Write" {
						writeKeys[c.Key] = true
					}
				}
				for _, wantKey := range tt.wantKeys {
					if !writeKeys[wantKey] {
						t.Errorf("expected Write to key %q, got keys: %v", wantKey, writeKeys)
					}
				}
			}
		})
	}
}

func TestMultiDayMutation_ReadError(t *testing.T) {
	transport := newMockTransport()
	obj := types.DataObject{Key: "data/missing.jsonl"}

	m := &mutation.MultiDayMutation{}
	record, err := m.Apply(context.Background(), obj, transport, map[string]string{
		"days": "2024-01-15",
	}, adapter.NewWallClock())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if record.Applied {
		t.Error("expected Applied=false on error")
	}
	if !strings.Contains(err.Error(), "read failed") {
		t.Errorf("error should mention read failed, got: %v", err)
	}
}
