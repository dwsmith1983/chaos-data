package testutil

import (
	"context"
	"sync"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// Compile-time interface assertion.
var _ adapter.EventEmitter = (*MockEmitter)(nil)

// MockEmitter is a reusable test double for adapter.EventEmitter.
//
// It records all emitted events and supports both a function callback (EmitFn)
// and a static error (Err). Priority: EmitFn > Err > nil.
type MockEmitter struct {
	mu sync.Mutex

	// EmitFn, when non-nil, is called for every Emit invocation. Its return
	// value is used directly (Err is ignored).
	EmitFn func(ctx context.Context, event types.ChaosEvent) error

	// Err is the error returned by Emit when EmitFn is nil.
	Err error

	events []types.ChaosEvent
}

// Emit implements adapter.EventEmitter.
func (m *MockEmitter) Emit(ctx context.Context, event types.ChaosEvent) error {
	m.mu.Lock()
	m.events = append(m.events, event)
	m.mu.Unlock()
	if m.EmitFn != nil {
		return m.EmitFn(ctx, event)
	}
	return m.Err
}

// GetEvents returns a snapshot of all recorded events.
func (m *MockEmitter) GetEvents() []types.ChaosEvent {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]types.ChaosEvent, len(m.events))
	copy(result, m.events)
	return result
}
