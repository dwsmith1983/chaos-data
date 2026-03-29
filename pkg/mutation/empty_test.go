package mutation_test

import (
	"context"
	"testing"

	"github.com/dwsmith1983/chaos-data/pkg/mutation"
	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

func TestEmptyMutation_Type(t *testing.T) {
	e := &mutation.EmptyMutation{}
	if got := e.Type(); got != "empty" {
		t.Errorf("Type() = %q, want %q", got, "empty")
	}
}

func TestEmptyMutation_Apply(t *testing.T) {
	inputData := []byte("header_line\nrow1\nrow2\n")

	tests := []struct {
		name            string
		params          map[string]string
		wantApplied     bool
		wantErr         bool
		wantWriteCount  int
		wantContentSize int // -1 to skip check
		wantContent     string
	}{
		{
			name:            "default writes zero bytes",
			params:          map[string]string{},
			wantApplied:     true,
			wantWriteCount:  1,
			wantContentSize: 0,
		},
		{
			name:            "preserve_header=false writes zero bytes",
			params:          map[string]string{"preserve_header": "false"},
			wantApplied:     true,
			wantWriteCount:  1,
			wantContentSize: 0,
		},
		{
			name:           "preserve_header=true writes first line only",
			params:         map[string]string{"preserve_header": "true"},
			wantApplied:    true,
			wantWriteCount: 1,
			wantContent:    "header_line\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transport := newMockTransport()
			obj := types.DataObject{Key: "data/records.csv"}
			transport.ReadData[obj.Key] = inputData

			e := &mutation.EmptyMutation{}
			record, err := e.Apply(context.Background(), obj, transport, tt.params, adapter.NewWallClock())

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
			if record.Mutation != "empty" {
				t.Errorf("Mutation = %q, want %q", record.Mutation, "empty")
			}
			if got := transport.callCount("Write"); got != tt.wantWriteCount {
				t.Errorf("Write call count = %d, want %d", got, tt.wantWriteCount)
			}

			// Verify content.
			calls := transport.getCalls()
			for _, c := range calls {
				if c.Method == "Write" {
					if tt.wantContentSize >= 0 && tt.wantContent == "" {
						if len(c.Data) != tt.wantContentSize {
							t.Errorf("Write data size = %d, want %d", len(c.Data), tt.wantContentSize)
						}
					}
					if tt.wantContent != "" {
						if string(c.Data) != tt.wantContent {
							t.Errorf("Write data = %q, want %q", string(c.Data), tt.wantContent)
						}
					}
					break
				}
			}
		})
	}
}

func TestEmptyMutation_PreserveHeaderReadError(t *testing.T) {
	transport := newMockTransport()
	obj := types.DataObject{Key: "data/missing.csv"}

	e := &mutation.EmptyMutation{}
	record, err := e.Apply(context.Background(), obj, transport, map[string]string{
		"preserve_header": "true",
	}, adapter.NewWallClock())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if record.Applied {
		t.Error("expected Applied=false on error")
	}
}

func TestEmptyMutation_NoReadWhenNotPreservingHeader(t *testing.T) {
	transport := newMockTransport()
	obj := types.DataObject{Key: "data/records.csv"}
	// Note: no readData set. Since preserve_header=false, Read should NOT be called.

	e := &mutation.EmptyMutation{}
	record, err := e.Apply(context.Background(), obj, transport, map[string]string{}, adapter.NewWallClock())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !record.Applied {
		t.Error("expected Applied=true")
	}
	if got := transport.callCount("Read"); got != 0 {
		t.Errorf("Read call count = %d, want 0 (should not read when not preserving header)", got)
	}
}
