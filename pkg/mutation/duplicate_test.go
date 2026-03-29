package mutation_test

import (
	"context"
	"testing"

	"github.com/dwsmith1983/chaos-data/pkg/mutation"
	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

func TestDuplicateMutation_Type(t *testing.T) {
	d := &mutation.DuplicateMutation{}
	if got := d.Type(); got != "duplicate" {
		t.Errorf("Type() = %q, want %q", got, "duplicate")
	}
}

func TestDuplicateMutation_Apply(t *testing.T) {
	inputData := []byte(`{"id":1,"name":"Alice"}` + "\n" + `{"id":2,"name":"Bob"}` + "\n")

	tests := []struct {
		name           string
		params         map[string]string
		wantApplied    bool
		wantErr        bool
		wantWriteCount int
	}{
		{
			name:           "default exact duplicate writes twice",
			params:         map[string]string{},
			wantApplied:    true,
			wantWriteCount: 2,
		},
		{
			name:           "explicit exact=true writes twice",
			params:         map[string]string{"exact": "true"},
			wantApplied:    true,
			wantWriteCount: 2,
		},
		{
			name:           "exact=false writes twice",
			params:         map[string]string{"exact": "false"},
			wantApplied:    true,
			wantWriteCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transport := newMockTransport()
			obj := types.DataObject{Key: "data/records.jsonl"}
			transport.ReadData[obj.Key] = inputData

			d := &mutation.DuplicateMutation{}
			record, err := d.Apply(context.Background(), obj, transport, tt.params, adapter.NewWallClock())

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
			if record.Mutation != "duplicate" {
				t.Errorf("Mutation = %q, want %q", record.Mutation, "duplicate")
			}
			if got := transport.callCount("Write"); got != tt.wantWriteCount {
				t.Errorf("Write call count = %d, want %d", got, tt.wantWriteCount)
			}
		})
	}
}

func TestDuplicateMutation_DupKeySuffix(t *testing.T) {
	inputData := []byte(`{"id":1}` + "\n")

	transport := newMockTransport()
	obj := types.DataObject{Key: "data/records.jsonl"}
	transport.ReadData[obj.Key] = inputData

	d := &mutation.DuplicateMutation{}
	_, err := d.Apply(context.Background(), obj, transport, map[string]string{}, adapter.NewWallClock())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	calls := transport.getCalls()
	writeKeys := make([]string, 0)
	for _, c := range calls {
		if c.Method == "Write" {
			writeKeys = append(writeKeys, c.Key)
		}
	}
	if len(writeKeys) != 2 {
		t.Fatalf("expected 2 Write calls, got %d", len(writeKeys))
	}

	// One should be the original key, one should have .dup suffix.
	foundOriginal := false
	foundDup := false
	for _, k := range writeKeys {
		if k == "data/records.jsonl" {
			foundOriginal = true
		}
		if k == "data/records.jsonl.dup" {
			foundDup = true
		}
	}
	if !foundOriginal {
		t.Errorf("expected Write to original key %q, got keys: %v", obj.Key, writeKeys)
	}
	if !foundDup {
		t.Errorf("expected Write to dup key %q, got keys: %v", obj.Key+".dup", writeKeys)
	}
}

func TestDuplicateMutation_ReadError(t *testing.T) {
	transport := newMockTransport()
	obj := types.DataObject{Key: "data/missing.jsonl"}
	// No readData set, so Read will return "key not found" error.

	d := &mutation.DuplicateMutation{}
	record, err := d.Apply(context.Background(), obj, transport, map[string]string{}, adapter.NewWallClock())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if record.Applied {
		t.Error("expected Applied=false on error")
	}
}
