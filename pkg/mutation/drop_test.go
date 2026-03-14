package mutation_test

import (
	"context"
	"testing"

	"github.com/dwsmith1983/chaos-data/pkg/mutation"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

func TestDropMutation_Type(t *testing.T) {
	d := &mutation.DropMutation{}
	if got := d.Type(); got != "drop" {
		t.Errorf("Type() = %q, want %q", got, "drop")
	}
}

func TestDropMutation_Apply(t *testing.T) {
	tests := []struct {
		name        string
		params      map[string]string
		wantApplied bool
		wantScope   string
	}{
		{
			name:        "drop with default scope returns Applied true",
			params:      map[string]string{},
			wantApplied: true,
			wantScope:   "object",
		},
		{
			name:        "drop with explicit object scope",
			params:      map[string]string{"scope": "object"},
			wantApplied: true,
			wantScope:   "object",
		},
		{
			name:        "drop with partition scope",
			params:      map[string]string{"scope": "partition"},
			wantApplied: true,
			wantScope:   "partition",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transport := newMockTransport()
			d := &mutation.DropMutation{}
			obj := types.DataObject{Key: "test/data.csv"}

			record, err := d.Apply(context.Background(), obj, transport, tt.params)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if record.Applied != tt.wantApplied {
				t.Errorf("Applied = %v, want %v", record.Applied, tt.wantApplied)
			}
			if record.Mutation != "drop" {
				t.Errorf("Mutation = %q, want %q", record.Mutation, "drop")
			}
			if record.ObjectKey != obj.Key {
				t.Errorf("ObjectKey = %q, want %q", record.ObjectKey, obj.Key)
			}

			// Verify scope is recorded correctly.
			if got := record.Params["scope"]; got != tt.wantScope {
				t.Errorf("recorded scope = %q, want %q", got, tt.wantScope)
			}

			// Verify NO transport methods were called.
			calls := transport.getCalls()
			if len(calls) != 0 {
				t.Errorf("expected zero transport calls, got %d: %v", len(calls), calls)
			}
		})
	}
}

func TestDropMutation_EmptyScopeRecordsObject(t *testing.T) {
	transport := newMockTransport()
	d := &mutation.DropMutation{}
	obj := types.DataObject{Key: "test/data.csv"}

	// Pass scope as empty string -- should resolve to "object".
	record, err := d.Apply(context.Background(), obj, transport, map[string]string{
		"scope": "",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !record.Applied {
		t.Error("expected Applied=true")
	}
	if got := record.Params["scope"]; got != "object" {
		t.Errorf("recorded scope = %q, want %q", got, "object")
	}
}
