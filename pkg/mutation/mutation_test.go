package mutation_test

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/mutation"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// stubMutation implements mutation.Mutation for registry tests.
type stubMutation struct {
	name string
}

func (s *stubMutation) Type() string { return s.name }

func (s *stubMutation) Apply(_ context.Context, _ types.DataObject, _ adapter.DataTransport, _ map[string]string) (types.MutationRecord, error) {
	return types.MutationRecord{Applied: true, Mutation: s.name}, nil
}

func TestRegistry_RegisterAndGet(t *testing.T) {
	tests := []struct {
		name       string
		register   []string
		lookup     string
		wantErr    error
		wantFound  bool
		wantType   string
	}{
		{
			name:      "register and lookup single mutation",
			register:  []string{"delay"},
			lookup:    "delay",
			wantErr:   nil,
			wantFound: true,
			wantType:  "delay",
		},
		{
			name:      "register multiple and lookup one",
			register:  []string{"delay", "drop", "corrupt"},
			lookup:    "drop",
			wantErr:   nil,
			wantFound: true,
			wantType:  "drop",
		},
		{
			name:      "lookup unknown returns ErrMutationNotFound",
			register:  []string{"delay"},
			lookup:    "nonexistent",
			wantErr:   mutation.ErrMutationNotFound,
			wantFound: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := mutation.NewRegistry()
			for _, name := range tt.register {
				if err := r.Register(&stubMutation{name: name}); err != nil {
					t.Fatalf("Register(%q) unexpected error: %v", name, err)
				}
			}

			got, err := r.Get(tt.lookup)
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Errorf("Get(%q) error = %v, want %v", tt.lookup, err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("Get(%q) unexpected error: %v", tt.lookup, err)
			}
			if !tt.wantFound {
				t.Fatal("expected mutation not to be found")
			}
			if got.Type() != tt.wantType {
				t.Errorf("Get(%q).Type() = %q, want %q", tt.lookup, got.Type(), tt.wantType)
			}
		})
	}
}

func TestRegistry_RegisterDuplicate(t *testing.T) {
	r := mutation.NewRegistry()
	m := &stubMutation{name: "delay"}

	if err := r.Register(m); err != nil {
		t.Fatalf("first Register() unexpected error: %v", err)
	}

	err := r.Register(m)
	if !errors.Is(err, mutation.ErrDuplicateMutation) {
		t.Errorf("second Register() error = %v, want %v", err, mutation.ErrDuplicateMutation)
	}
}

func TestRegistry_List(t *testing.T) {
	tests := []struct {
		name     string
		register []string
		want     []string
	}{
		{
			name:     "empty registry returns empty slice",
			register: nil,
			want:     []string{},
		},
		{
			name:     "single mutation",
			register: []string{"delay"},
			want:     []string{"delay"},
		},
		{
			name:     "multiple mutations returned sorted",
			register: []string{"drop", "corrupt", "delay"},
			want:     []string{"corrupt", "delay", "drop"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := mutation.NewRegistry()
			for _, name := range tt.register {
				if err := r.Register(&stubMutation{name: name}); err != nil {
					t.Fatalf("Register(%q) unexpected error: %v", name, err)
				}
			}

			got := r.List()
			if len(got) != len(tt.want) {
				t.Fatalf("List() returned %d items, want %d", len(got), len(tt.want))
			}
			if !sort.StringsAreSorted(got) {
				t.Errorf("List() result is not sorted: %v", got)
			}
			for i, name := range got {
				if name != tt.want[i] {
					t.Errorf("List()[%d] = %q, want %q", i, name, tt.want[i])
				}
			}
		})
	}
}

func TestRegistry_ConcurrentRegisterAndGet(t *testing.T) {
	r := mutation.NewRegistry()

	// Pre-register a known mutation so Get has something to find.
	if err := r.Register(&stubMutation{name: "base"}); err != nil {
		t.Fatalf("Register(base) unexpected error: %v", err)
	}

	const goroutines = 50
	var wg sync.WaitGroup
	wg.Add(goroutines * 2)

	// Half the goroutines register unique mutations.
	for i := range goroutines {
		go func(idx int) {
			defer wg.Done()
			_ = r.Register(&stubMutation{name: fmt.Sprintf("mut_%d", idx)})
		}(i)
	}

	// The other half concurrently call Get and List.
	for range goroutines {
		go func() {
			defer wg.Done()
			_, _ = r.Get("base")
			_ = r.List()
		}()
	}

	wg.Wait()

	// Verify "base" is still retrievable.
	m, err := r.Get("base")
	if err != nil {
		t.Fatalf("Get(base) after concurrent access: %v", err)
	}
	if m.Type() != "base" {
		t.Errorf("Type() = %q, want %q", m.Type(), "base")
	}
}
