package mutation

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// Sentinel errors for the mutation registry.
var (
	ErrDuplicateMutation = errors.New("mutation already registered")
	ErrMutationNotFound  = errors.New("mutation not found")
)

// Mutation defines the interface for a chaos mutation that can be applied to data objects.
type Mutation interface {
	// Type returns the unique name of this mutation (e.g., "delay", "corrupt", "drop").
	Type() string

	// Apply executes the mutation against the given object using the provided transport.
	// params contains scenario-specific configuration for this mutation instance.
	// Returns a MutationRecord describing what was done.
	Apply(ctx context.Context, obj types.DataObject, transport adapter.DataTransport, params map[string]string) (types.MutationRecord, error)
}

// Registry holds registered mutation implementations.
type Registry struct {
	mu        sync.RWMutex
	mutations map[string]Mutation
}

// NewRegistry creates a new empty mutation registry.
func NewRegistry() *Registry {
	return &Registry{
		mutations: make(map[string]Mutation),
	}
}

// Register adds a mutation to the registry. It returns ErrDuplicateMutation if
// a mutation with the same Type() is already registered.
func (r *Registry) Register(m Mutation) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	name := m.Type()
	if _, exists := r.mutations[name]; exists {
		return fmt.Errorf("%w: %s", ErrDuplicateMutation, name)
	}
	r.mutations[name] = m
	return nil
}

// Get retrieves a mutation by name. It returns ErrMutationNotFound if no
// mutation with that name is registered.
func (r *Registry) Get(name string) (Mutation, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	m, exists := r.mutations[name]
	if !exists {
		return nil, fmt.Errorf("%w: %s", ErrMutationNotFound, name)
	}
	return m, nil
}

// List returns the names of all registered mutations, sorted alphabetically.
func (r *Registry) List() []string {
	r.mu.RLock()
	names := make([]string, 0, len(r.mutations))
	for name := range r.mutations {
		names = append(names, name)
	}
	r.mu.RUnlock()

	sort.Strings(names)
	return names
}
