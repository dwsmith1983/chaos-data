package interlock_test

import (
	"context"

	"github.com/dwsmith1983/chaos-data/internal/testutil"
	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// transportCall is a package-local alias for testutil.TransportCall,
// preserving existing test call-sites.
type transportCall = testutil.TransportCall

// stateCall is a package-local alias for testutil.StateCall.
type stateCall = testutil.StateCall

// mockTransport wraps testutil.MockTransport and adds package-local
// convenience methods so existing test call-sites compile without modification.
type mockTransport struct {
	*testutil.MockTransport
}

// mockStateStore wraps testutil.MockStateStore with package-local
// convenience methods and configurable canned responses.
type mockStateStore struct {
	*testutil.MockStateStore

	// jobEvents is returned by ReadJobEvents for test setup.
	jobEvents []adapter.JobEvent
	// reruns is returned by CountReruns for test setup.
	reruns int
}

// Compile-time interface assertions.
var (
	_ adapter.DataTransport = (*mockTransport)(nil)
	_ adapter.StateStore    = (*mockStateStore)(nil)
)

func newMockTransport() *mockTransport {
	return &mockTransport{MockTransport: testutil.NewMockTransport()}
}

func (m *mockTransport) getCalls() []transportCall {
	return m.MockTransport.GetCalls()
}

func newMockStateStore() *mockStateStore {
	return &mockStateStore{MockStateStore: testutil.NewMockStateStore()}
}

func (m *mockStateStore) getStateCalls() []stateCall {
	return m.MockStateStore.GetStateCalls()
}

func (m *mockStateStore) stateCallCount(method string) int {
	return m.MockStateStore.StateCallCount(method)
}

// ReadJobEvents overrides the embedded method to return canned test data.
func (m *mockStateStore) ReadJobEvents(_ context.Context, _, _, _ string) ([]adapter.JobEvent, error) {
	result := make([]adapter.JobEvent, len(m.jobEvents))
	copy(result, m.jobEvents)
	return result, nil
}

// CountReruns overrides the embedded method to return the canned reruns count.
func (m *mockStateStore) CountReruns(_ context.Context, _, _, _ string) (int, error) {
	return m.reruns, nil
}

// mockEventEmitter wraps testutil.MockEmitter with package-local convenience.
type mockEventEmitter struct {
	*testutil.MockEmitter
}

func newMockEventEmitter() *mockEventEmitter {
	return &mockEventEmitter{MockEmitter: &testutil.MockEmitter{}}
}

// mockEventReader implements adapter.EventReader and returns canned events.
// This is unique to interlock and not shared via testutil.
type mockEventReader struct {
	events []types.ChaosEvent
	err    error
}

func newMockEventReader() *mockEventReader {
	return &mockEventReader{}
}

func (m *mockEventReader) Manifest(_ context.Context) ([]types.ChaosEvent, error) {
	if m.err != nil {
		return nil, m.err
	}
	result := make([]types.ChaosEvent, len(m.events))
	copy(result, m.events)
	return result, nil
}
