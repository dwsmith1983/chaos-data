package mutation_test

import (
	"context"
	"strings"
	"testing"

	"github.com/dwsmith1983/chaos-data/pkg/mutation"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

func TestPartialMutation_Type(t *testing.T) {
	p := &mutation.PartialMutation{}
	if got := p.Type(); got != "partial" {
		t.Errorf("Type() = %q, want %q", got, "partial")
	}
}

func TestPartialMutation_Apply(t *testing.T) {
	// 100-byte input for easy percentage math.
	inputData := []byte(strings.Repeat("A", 100))

	tests := []struct {
		name           string
		params         map[string]string
		wantApplied    bool
		wantErr        bool
		wantWriteCount int
		wantDataLen    int
	}{
		{
			name:           "50% delivers half the bytes",
			params:         map[string]string{"delivery_pct": "50"},
			wantApplied:    true,
			wantWriteCount: 1,
			wantDataLen:    50,
		},
		{
			name:           "100% delivers full content",
			params:         map[string]string{"delivery_pct": "100"},
			wantApplied:    true,
			wantWriteCount: 1,
			wantDataLen:    100,
		},
		{
			name:           "0% delivers empty",
			params:         map[string]string{"delivery_pct": "0"},
			wantApplied:    true,
			wantWriteCount: 1,
			wantDataLen:    0,
		},
		{
			name:           "25% delivers quarter",
			params:         map[string]string{"delivery_pct": "25"},
			wantApplied:    true,
			wantWriteCount: 1,
			wantDataLen:    25,
		},
		{
			name:        "missing delivery_pct returns error",
			params:      map[string]string{},
			wantApplied: false,
			wantErr:     true,
		},
		{
			name:        "empty delivery_pct returns error",
			params:      map[string]string{"delivery_pct": ""},
			wantApplied: false,
			wantErr:     true,
		},
		{
			name:        "invalid delivery_pct returns error",
			params:      map[string]string{"delivery_pct": "abc"},
			wantApplied: false,
			wantErr:     true,
		},
		{
			name:        "delivery_pct over 100 returns error",
			params:      map[string]string{"delivery_pct": "150"},
			wantApplied: false,
			wantErr:     true,
		},
		{
			name:        "negative delivery_pct returns error",
			params:      map[string]string{"delivery_pct": "-10"},
			wantApplied: false,
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transport := newMockTransport()
			obj := types.DataObject{Key: "data/records.jsonl"}
			transport.readData[obj.Key] = inputData

			p := &mutation.PartialMutation{}
			record, err := p.Apply(context.Background(), obj, transport, tt.params)

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
			if record.Mutation != "partial" {
				t.Errorf("Mutation = %q, want %q", record.Mutation, "partial")
			}
			if got := transport.callCount("Write"); got != tt.wantWriteCount {
				t.Errorf("Write call count = %d, want %d", got, tt.wantWriteCount)
			}

			// Verify written data length.
			calls := transport.getCalls()
			for _, c := range calls {
				if c.Method == "Write" {
					if len(c.Data) != tt.wantDataLen {
						t.Errorf("Write data length = %d, want %d", len(c.Data), tt.wantDataLen)
					}
					break
				}
			}
		})
	}
}

func TestPartialMutation_ReadError(t *testing.T) {
	transport := newMockTransport()
	obj := types.DataObject{Key: "data/missing.jsonl"}

	p := &mutation.PartialMutation{}
	record, err := p.Apply(context.Background(), obj, transport, map[string]string{
		"delivery_pct": "50",
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if record.Applied {
		t.Error("expected Applied=false on error")
	}
}
