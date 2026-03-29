package engine_test

import (
	"context"
	"fmt"
	"sync"

	"github.com/dwsmith1983/chaos-data/internal/testutil"
	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// Compile-time interface assertions.
var (
	_ adapter.DataTransport      = (*mockTransport)(nil)
	_ adapter.EventEmitter       = (*mockEmitter)(nil)
	_ adapter.SafetyController   = (*mockSafety)(nil)
	_ adapter.DependencyResolver = (*mockResolver)(nil)
	_ adapter.Asserter           = (*mockAsserter)(nil)
	_ adapter.Asserter           = (*mockValidatingAsserter)(nil)
	_ adapter.TargetValidator    = (*mockValidatingAsserter)(nil)
)

// mockTransport wraps testutil.MockTransport for the engine test package.
type mockTransport = testutil.MockTransport

// mockEmitter wraps testutil.MockEmitter for the engine test package.
type mockEmitter = testutil.MockEmitter

// mockSafety wraps testutil.MockSafety for the engine test package.
type mockSafety = testutil.MockSafety

// mockAsserter is a test double for adapter.Asserter.
type mockAsserter struct {
	supported map[types.AssertionType]bool
	results   map[string]bool // target → result
	callCount int
	mu        sync.Mutex
}

func (m *mockAsserter) Supports(at types.AssertionType) bool {
	return m.supported[at]
}

func (m *mockAsserter) Evaluate(_ context.Context, a types.Assertion) (bool, error) {
	m.mu.Lock()
	m.callCount++
	result := m.results[a.Target]
	m.mu.Unlock()
	return result, nil
}

// mockValidatingAsserter implements both adapter.Asserter and adapter.TargetValidator.
// It allows tests to control which targets are considered valid by ValidateTarget.
type mockValidatingAsserter struct {
	mockAsserter
	// invalidTargets is the set of targets that ValidateTarget rejects.
	invalidTargets map[string]bool
	validateCalls  []types.Assertion
	mu2            sync.Mutex
}

func (m *mockValidatingAsserter) ValidateTarget(a types.Assertion) error {
	m.mu2.Lock()
	m.validateCalls = append(m.validateCalls, a)
	invalid := m.invalidTargets[a.Target]
	m.mu2.Unlock()
	if invalid {
		return fmt.Errorf("invalid target %q for type %q", a.Target, a.Type)
	}
	return nil
}

func (m *mockValidatingAsserter) getValidateCalls() []types.Assertion {
	m.mu2.Lock()
	defer m.mu2.Unlock()
	result := make([]types.Assertion, len(m.validateCalls))
	copy(result, m.validateCalls)
	return result
}

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
