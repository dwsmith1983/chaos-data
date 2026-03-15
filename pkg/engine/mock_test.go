package engine_test

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// Compile-time interface assertions.
var (
	_ adapter.DataTransport    = (*mockTransport)(nil)
	_ adapter.EventEmitter     = (*mockEmitter)(nil)
	_ adapter.SafetyController = (*mockSafety)(nil)
	_ adapter.DependencyResolver = (*mockResolver)(nil)
)

// mockResolver is a test double for adapter.DependencyResolver.
type mockResolver struct {
	mu           sync.Mutex
	downstreamFn func(ctx context.Context, target string) ([]string, error)
	calls        []string
}

func (m *mockResolver) GetDownstream(ctx context.Context, target string) ([]string, error) {
	m.mu.Lock()
	m.calls = append(m.calls, target)
	fn := m.downstreamFn
	m.mu.Unlock()
	if fn != nil {
		return fn(ctx, target)
	}
	return nil, nil
}

func (m *mockResolver) getCalls() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]string, len(m.calls))
	copy(result, m.calls)
	return result
}

// mockTransport is a test double for adapter.DataTransport.
type mockTransport struct {
	mu sync.Mutex

	listFn       func(ctx context.Context, prefix string) ([]types.DataObject, error)
	readFn       func(ctx context.Context, key string) (io.ReadCloser, error)
	writeFn      func(ctx context.Context, key string, data io.Reader) error
	deleteFn     func(ctx context.Context, key string) error
	holdFn       func(ctx context.Context, key string, until time.Time) error
	releaseFn    func(ctx context.Context, key string) error
	releaseAllFn func(ctx context.Context) error
	listHeldFn   func(ctx context.Context) ([]types.DataObject, error)

	holdCalls []holdCall
}

type holdCall struct {
	Key   string
	Until time.Time
}

func (m *mockTransport) List(ctx context.Context, prefix string) ([]types.DataObject, error) {
	if m.listFn != nil {
		return m.listFn(ctx, prefix)
	}
	return nil, nil
}

func (m *mockTransport) Read(ctx context.Context, key string) (io.ReadCloser, error) {
	if m.readFn != nil {
		return m.readFn(ctx, key)
	}
	return nil, nil
}

func (m *mockTransport) Write(ctx context.Context, key string, data io.Reader) error {
	if m.writeFn != nil {
		return m.writeFn(ctx, key, data)
	}
	return nil
}

func (m *mockTransport) Delete(ctx context.Context, key string) error {
	if m.deleteFn != nil {
		return m.deleteFn(ctx, key)
	}
	return nil
}

func (m *mockTransport) Hold(ctx context.Context, key string, until time.Time) error {
	m.mu.Lock()
	m.holdCalls = append(m.holdCalls, holdCall{Key: key, Until: until})
	m.mu.Unlock()
	if m.holdFn != nil {
		return m.holdFn(ctx, key, until)
	}
	return nil
}

func (m *mockTransport) Release(ctx context.Context, key string) error {
	if m.releaseFn != nil {
		return m.releaseFn(ctx, key)
	}
	return nil
}

func (m *mockTransport) ListHeld(ctx context.Context) ([]types.DataObject, error) {
	if m.listHeldFn != nil {
		return m.listHeldFn(ctx)
	}
	return nil, nil
}

func (m *mockTransport) ReleaseAll(ctx context.Context) error {
	if m.releaseAllFn != nil {
		return m.releaseAllFn(ctx)
	}
	return nil
}

// mockEmitter is a test double for adapter.EventEmitter.
type mockEmitter struct {
	mu     sync.Mutex
	events []types.ChaosEvent
	emitFn func(ctx context.Context, event types.ChaosEvent) error
}

func (m *mockEmitter) Emit(ctx context.Context, event types.ChaosEvent) error {
	m.mu.Lock()
	m.events = append(m.events, event)
	m.mu.Unlock()
	if m.emitFn != nil {
		return m.emitFn(ctx, event)
	}
	return nil
}

func (m *mockEmitter) getEvents() []types.ChaosEvent {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]types.ChaosEvent, len(m.events))
	copy(result, m.events)
	return result
}

// mockSafety is a test double for adapter.SafetyController.
type mockSafety struct {
	mu sync.Mutex

	enabled    bool
	enabledErr error
	maxSev     types.Severity
	maxSevErr  error
	blastErr   error
	slaAllowed bool
	slaErr     error

	// blastRadiusFn, when non-nil, overrides blastErr for CheckBlastRadius calls.
	// This allows tests to vary behavior based on the stats argument (e.g.,
	// return an error only after a threshold of affected targets is reached).
	blastRadiusFn func(stats types.ExperimentStats) error

	cooldownErr    error
	recordInjFn    func(ctx context.Context, scenario string) error
	recordInjCalls []string
}

func (m *mockSafety) IsEnabled(_ context.Context) (bool, error) {
	return m.enabled, m.enabledErr
}

func (m *mockSafety) MaxSeverity(_ context.Context) (types.Severity, error) {
	return m.maxSev, m.maxSevErr
}

func (m *mockSafety) CheckBlastRadius(_ context.Context, stats types.ExperimentStats) error {
	m.mu.Lock()
	fn := m.blastRadiusFn
	err := m.blastErr
	m.mu.Unlock()
	if fn != nil {
		return fn(stats)
	}
	return err
}

func (m *mockSafety) CheckSLAWindow(_ context.Context, _ string) (bool, error) {
	return m.slaAllowed, m.slaErr
}

func (m *mockSafety) CheckCooldown(_ context.Context, _ string) error {
	return m.cooldownErr
}

func (m *mockSafety) RecordInjection(_ context.Context, scenario string) error {
	m.mu.Lock()
	m.recordInjCalls = append(m.recordInjCalls, scenario)
	m.mu.Unlock()
	if m.recordInjFn != nil {
		return m.recordInjFn(context.Background(), scenario)
	}
	return nil
}
