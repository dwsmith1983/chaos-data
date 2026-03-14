package mutation_test

import (
	"context"
	"strings"
	"testing"

	"github.com/dwsmith1983/chaos-data/pkg/mutation"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

func TestStaleReplayMutation_Type(t *testing.T) {
	s := &mutation.StaleReplayMutation{}
	if got := s.Type(); got != "stale-replay" {
		t.Errorf("Type() = %q, want %q", got, "stale-replay")
	}
}

func TestStaleReplayMutation_Apply(t *testing.T) {
	inputData := []byte(`{"id":1,"name":"Alice"}` + "\n")

	tests := []struct {
		name        string
		params      map[string]string
		wantApplied bool
		wantErr     bool
		wantKey     string // expected Write key (empty to skip check)
	}{
		{
			name:        "writes with date-prefixed key",
			params:      map[string]string{"replay_date": "2024-01-15"},
			wantApplied: true,
			wantKey:     "date=2024-01-15/data/records.jsonl",
		},
		{
			name:        "with optional prefix",
			params:      map[string]string{"replay_date": "2024-01-15", "prefix": "archive"},
			wantApplied: true,
			wantKey:     "archive/date=2024-01-15/data/records.jsonl",
		},
		{
			name:        "missing replay_date returns error",
			params:      map[string]string{},
			wantApplied: false,
			wantErr:     true,
		},
		{
			name:        "empty replay_date returns error",
			params:      map[string]string{"replay_date": ""},
			wantApplied: false,
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transport := newMockTransport()
			obj := types.DataObject{Key: "data/records.jsonl"}
			transport.readData[obj.Key] = inputData

			s := &mutation.StaleReplayMutation{}
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
			if record.Mutation != "stale-replay" {
				t.Errorf("Mutation = %q, want %q", record.Mutation, "stale-replay")
			}

			// Verify Write was called with the expected key.
			if tt.wantKey != "" {
				calls := transport.getCalls()
				found := false
				for _, c := range calls {
					if c.Method == "Write" && c.Key == tt.wantKey {
						found = true
						break
					}
				}
				if !found {
					var keys []string
					for _, c := range calls {
						if c.Method == "Write" {
							keys = append(keys, c.Key)
						}
					}
					t.Errorf("expected Write to key %q, got Write keys: %v", tt.wantKey, keys)
				}
			}
		})
	}
}

func TestStaleReplayMutation_ReadError(t *testing.T) {
	transport := newMockTransport()
	obj := types.DataObject{Key: "data/missing.jsonl"}

	s := &mutation.StaleReplayMutation{}
	record, err := s.Apply(context.Background(), obj, transport, map[string]string{
		"replay_date": "2024-01-15",
	})
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
