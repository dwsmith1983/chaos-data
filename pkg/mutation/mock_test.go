package mutation_test

import (
	"github.com/dwsmith1983/chaos-data/internal/testutil"
	"github.com/dwsmith1983/chaos-data/pkg/adapter"
)

// call is a package-local alias for testutil.TransportCall, preserving
// existing test call-sites that reference the call type.
type call = testutil.TransportCall

// stateCall is a package-local alias for testutil.StateCall.
type stateCall = testutil.StateCall

// mockTransport wraps testutil.MockTransport and adds package-local
// convenience methods (lowercase) so existing test call-sites compile
// without modification.
type mockTransport struct {
	*testutil.MockTransport
}

// mockStateStore wraps testutil.MockStateStore with package-local
// convenience methods. The sensors field aliases the embedded Sensors map so
// that test call-sites using the lowercase name compile without modification.
type mockStateStore struct {
	*testutil.MockStateStore
	// sensors is an alias for MockStateStore.Sensors (same underlying map).
	sensors map[string]adapter.SensorData
}

// Compile-time interface assertions for the wrapped mocks.
var (
	_ adapter.DataTransport = (*mockTransport)(nil)
	_ adapter.StateStore    = (*mockStateStore)(nil)
)

func newMockTransport() *mockTransport {
	return &mockTransport{MockTransport: testutil.NewMockTransport()}
}

func (m *mockTransport) getCalls() []call {
	return m.MockTransport.GetCalls()
}

func (m *mockTransport) callCount(method string) int {
	return m.MockTransport.CallCount(method)
}

func newMockStateStore() *mockStateStore {
	inner := testutil.NewMockStateStore()
	return &mockStateStore{MockStateStore: inner, sensors: inner.Sensors}
}

func (m *mockStateStore) getStateCalls() []stateCall {
	return m.MockStateStore.GetStateCalls()
}

func (m *mockStateStore) stateCallCount(method string) int {
	return m.MockStateStore.StateCallCount(method)
}
