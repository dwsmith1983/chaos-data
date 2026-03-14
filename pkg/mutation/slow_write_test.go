package mutation_test

import (
	"context"
	"testing"

	"github.com/dwsmith1983/chaos-data/pkg/mutation"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

func TestSlowWriteMutation_Type(t *testing.T) {
	s := &mutation.SlowWriteMutation{}
	if got := s.Type(); got != "slow-write" {
		t.Errorf("Type() = %q, want %q", got, "slow-write")
	}
}

func TestSlowWriteMutation_Apply(t *testing.T) {
	inputData := []byte(`{"id":1,"name":"Alice"}` + "\n")

	tests := []struct {
		name           string
		params         map[string]string
		wantApplied    bool
		wantErr        bool
		wantReadCount  int
		wantWriteCount int
	}{
		{
			name: "basic slow write with minimal latency",
			params: map[string]string{
				"latency": "1ms",
			},
			wantApplied:    true,
			wantReadCount:  1,
			wantWriteCount: 1,
		},
		{
			name: "slow write with latency and jitter",
			params: map[string]string{
				"latency": "1ms",
				"jitter":  "1ms",
			},
			wantApplied:    true,
			wantReadCount:  1,
			wantWriteCount: 1,
		},
		{
			name:        "missing latency returns error",
			params:      map[string]string{},
			wantApplied: false,
			wantErr:     true,
		},
		{
			name: "invalid latency returns error",
			params: map[string]string{
				"latency": "not-a-duration",
			},
			wantApplied: false,
			wantErr:     true,
		},
		{
			name: "invalid jitter returns error",
			params: map[string]string{
				"latency": "1ms",
				"jitter":  "bad",
			},
			wantApplied: false,
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transport := newMockTransport()
			obj := types.DataObject{Key: "data/records.jsonl"}
			transport.readData[obj.Key] = inputData

			s := &mutation.SlowWriteMutation{}
			record, err := s.Apply(context.Background(), obj, transport, tt.params)

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
			if record.Mutation != "slow-write" {
				t.Errorf("Mutation = %q, want %q", record.Mutation, "slow-write")
			}

			// Verify transport calls.
			if got := transport.callCount("Read"); got != tt.wantReadCount {
				t.Errorf("Read call count = %d, want %d", got, tt.wantReadCount)
			}
			if got := transport.callCount("Write"); got != tt.wantWriteCount {
				t.Errorf("Write call count = %d, want %d", got, tt.wantWriteCount)
			}

			// Verify written data matches input.
			if tt.wantWriteCount > 0 {
				calls := transport.getCalls()
				for _, c := range calls {
					if c.Method == "Write" {
						if string(c.Data) != string(inputData) {
							t.Errorf("Write data = %q, want %q", string(c.Data), string(inputData))
						}
						break
					}
				}
			}
		})
	}
}

func TestSlowWriteMutation_ReadError(t *testing.T) {
	transport := newMockTransport()
	obj := types.DataObject{Key: "data/missing.jsonl"}
	// No readData set, so Read will return "key not found" error.

	s := &mutation.SlowWriteMutation{}
	record, err := s.Apply(context.Background(), obj, transport, map[string]string{
		"latency": "1ms",
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if record.Applied {
		t.Error("expected Applied=false on error")
	}
}

func TestSlowWriteMutation_ZeroJitter(t *testing.T) {
	inputData := []byte(`{"id":1}` + "\n")
	transport := newMockTransport()
	obj := types.DataObject{Key: "data/records.jsonl"}
	transport.readData[obj.Key] = inputData

	s := &mutation.SlowWriteMutation{}
	record, err := s.Apply(context.Background(), obj, transport, map[string]string{
		"latency": "1ms",
		"jitter":  "0s",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !record.Applied {
		t.Error("expected Applied=true")
	}
}
