package testutil

import (
	"context"
	"sync"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// Compile-time interface assertion.
var _ adapter.SafetyController = (*MockSafety)(nil)

// MockSafety is a reusable test double for adapter.SafetyController.
//
// All fields are configurable. Function callbacks take priority over static
// return values when set.
type MockSafety struct {
	mu sync.Mutex

	Enabled    bool
	EnabledErr error
	MaxSev     types.Severity
	MaxSevErr  error
	BlastErr   error
	SLAAllowed bool
	SLAErr     error
	CooldownErr error

	// BlastRadiusFn, when non-nil, overrides BlastErr for CheckBlastRadius.
	BlastRadiusFn func(stats types.ExperimentStats) error

	// RecordInjFn, when non-nil, overrides the default RecordInjection behavior.
	RecordInjFn func(ctx context.Context, scenario string) error

	recordInjCalls []string
}

// IsEnabled implements adapter.SafetyController.
func (m *MockSafety) IsEnabled(_ context.Context) (bool, error) {
	return m.Enabled, m.EnabledErr
}

// MaxSeverity implements adapter.SafetyController.
func (m *MockSafety) MaxSeverity(_ context.Context) (types.Severity, error) {
	return m.MaxSev, m.MaxSevErr
}

// CheckBlastRadius implements adapter.SafetyController.
func (m *MockSafety) CheckBlastRadius(_ context.Context, stats types.ExperimentStats) error {
	m.mu.Lock()
	fn := m.BlastRadiusFn
	err := m.BlastErr
	m.mu.Unlock()
	if fn != nil {
		return fn(stats)
	}
	return err
}

// CheckSLAWindow implements adapter.SafetyController.
func (m *MockSafety) CheckSLAWindow(_ context.Context, _ string) (bool, error) {
	return m.SLAAllowed, m.SLAErr
}

// CheckCooldown implements adapter.SafetyController.
func (m *MockSafety) CheckCooldown(_ context.Context, _ string) error {
	return m.CooldownErr
}

// RecordInjection implements adapter.SafetyController.
func (m *MockSafety) RecordInjection(ctx context.Context, scenario string) error {
	m.mu.Lock()
	m.recordInjCalls = append(m.recordInjCalls, scenario)
	m.mu.Unlock()
	if m.RecordInjFn != nil {
		return m.RecordInjFn(ctx, scenario)
	}
	return nil
}

// GetRecordInjCalls returns a snapshot of all scenario names passed to RecordInjection.
func (m *MockSafety) GetRecordInjCalls() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]string, len(m.recordInjCalls))
	copy(result, m.recordInjCalls)
	return result
}
