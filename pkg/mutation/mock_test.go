package mutation_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// call records a single method invocation on the mock transport.
type call struct {
	Method string
	Key    string
	Until  time.Time // only for Hold
	Data   []byte    // only for Write
}

// mockTransport implements adapter.DataTransport and records all method calls.
type mockTransport struct {
	mu    sync.Mutex
	calls []call

	// readData maps keys to data returned by Read.
	readData map[string][]byte

	// readErr maps keys to errors returned by Read.
	readErr map[string]error

	// writeErr maps keys to errors returned by Write.
	writeErr map[string]error

	// holdErr maps keys to errors returned by Hold.
	holdErr map[string]error

	// deleteErr maps keys to errors returned by Delete.
	deleteErr map[string]error
}

func newMockTransport() *mockTransport {
	return &mockTransport{
		readData:  make(map[string][]byte),
		readErr:   make(map[string]error),
		writeErr:  make(map[string]error),
		holdErr:   make(map[string]error),
		deleteErr: make(map[string]error),
	}
}

func (m *mockTransport) record(c call) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, c)
}

func (m *mockTransport) getCalls() []call {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]call, len(m.calls))
	copy(result, m.calls)
	return result
}

func (m *mockTransport) callCount(method string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	count := 0
	for _, c := range m.calls {
		if c.Method == method {
			count++
		}
	}
	return count
}

func (m *mockTransport) List(_ context.Context, prefix string) ([]types.DataObject, error) {
	m.record(call{Method: "List", Key: prefix})
	return nil, nil
}

func (m *mockTransport) Read(_ context.Context, key string) (io.ReadCloser, error) {
	m.record(call{Method: "Read", Key: key})
	if err, ok := m.readErr[key]; ok {
		return nil, err
	}
	data, ok := m.readData[key]
	if !ok {
		return nil, fmt.Errorf("key not found: %s", key)
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (m *mockTransport) Write(_ context.Context, key string, data io.Reader) error {
	content, err := io.ReadAll(data)
	if err != nil {
		return err
	}
	m.record(call{Method: "Write", Key: key, Data: content})
	if err, ok := m.writeErr[key]; ok {
		return err
	}
	return nil
}

func (m *mockTransport) Delete(_ context.Context, key string) error {
	m.record(call{Method: "Delete", Key: key})
	if err, ok := m.deleteErr[key]; ok {
		return err
	}
	return nil
}

func (m *mockTransport) Hold(_ context.Context, key string, until time.Time) error {
	m.record(call{Method: "Hold", Key: key, Until: until})
	if err, ok := m.holdErr[key]; ok {
		return err
	}
	return nil
}

func (m *mockTransport) Release(_ context.Context, key string) error {
	m.record(call{Method: "Release", Key: key})
	return nil
}
