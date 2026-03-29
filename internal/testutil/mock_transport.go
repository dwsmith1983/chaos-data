package testutil

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// Compile-time interface assertion.
var _ adapter.DataTransport = (*MockTransport)(nil)

// TransportCall records a single method invocation on MockTransport.
type TransportCall struct {
	Method string
	Key    string
	Until  time.Time // only for Hold/HoldData
	Data   []byte    // only for Write
}

// MockTransport is a reusable test double for adapter.DataTransport.
//
// It supports two configuration styles that compose freely:
//   - Function callbacks (ListFn, ReadFn, ...) for full control of behavior.
//   - Per-key error/data maps (ReadData, ReadErr, ...) for declarative setup.
//
// Priority: function callback > per-key error map > default (nil/success).
// All method invocations are recorded regardless of which style is used.
type MockTransport struct {
	mu sync.Mutex

	// Function callbacks — when non-nil, the callback is invoked and its
	// return value is used directly (per-key maps are skipped).
	ListFn       func(ctx context.Context, prefix string) ([]types.DataObject, error)
	ReadFn       func(ctx context.Context, key string) (io.ReadCloser, error)
	WriteFn      func(ctx context.Context, key string, data io.Reader) error
	DeleteFn     func(ctx context.Context, key string) error
	HoldFn       func(ctx context.Context, key string, until time.Time) error
	ReleaseFn    func(ctx context.Context, key string) error
	ReleaseAllFn func(ctx context.Context) error
	ListHeldFn   func(ctx context.Context) ([]types.HeldObject, error)
	HoldDataFn   func(ctx context.Context, key string, data io.Reader, until time.Time) error

	// Per-key data/error maps — used when the corresponding function callback
	// is nil. Initialised by NewMockTransport; safe to set directly when the
	// mock is created via a struct literal (the zero map is handled gracefully).
	ReadData  map[string][]byte
	ReadErr   map[string]error
	WriteErr  map[string]error
	HoldErr   map[string]error
	DeleteErr map[string]error

	calls []TransportCall
}

// NewMockTransport returns a MockTransport with initialised maps.
func NewMockTransport() *MockTransport {
	return &MockTransport{
		ReadData:  make(map[string][]byte),
		ReadErr:   make(map[string]error),
		WriteErr:  make(map[string]error),
		HoldErr:   make(map[string]error),
		DeleteErr: make(map[string]error),
	}
}

func (m *MockTransport) record(c TransportCall) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, c)
}

// GetCalls returns a snapshot of all recorded method invocations.
func (m *MockTransport) GetCalls() []TransportCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]TransportCall, len(m.calls))
	copy(result, m.calls)
	return result
}

// CallCount returns the number of recorded invocations for the given method.
func (m *MockTransport) CallCount(method string) int {
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

// HoldCalls returns only the recorded Hold invocations (convenience wrapper).
func (m *MockTransport) HoldCalls() []TransportCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	var result []TransportCall
	for _, c := range m.calls {
		if c.Method == "Hold" {
			result = append(result, c)
		}
	}
	return result
}

// List implements adapter.DataTransport.
func (m *MockTransport) List(ctx context.Context, prefix string) ([]types.DataObject, error) {
	m.record(TransportCall{Method: "List", Key: prefix})
	if m.ListFn != nil {
		return m.ListFn(ctx, prefix)
	}
	return nil, nil
}

// Read implements adapter.DataTransport.
func (m *MockTransport) Read(ctx context.Context, key string) (io.ReadCloser, error) {
	m.record(TransportCall{Method: "Read", Key: key})
	if m.ReadFn != nil {
		return m.ReadFn(ctx, key)
	}
	if m.ReadErr != nil {
		if err, ok := m.ReadErr[key]; ok {
			return nil, err
		}
	}
	if m.ReadData != nil {
		if data, ok := m.ReadData[key]; ok {
			return io.NopCloser(bytes.NewReader(data)), nil
		}
	}
	return nil, fmt.Errorf("key not found: %s", key)
}

// Write implements adapter.DataTransport.
func (m *MockTransport) Write(ctx context.Context, key string, data io.Reader) error {
	content, err := io.ReadAll(data)
	if err != nil {
		return err
	}
	m.record(TransportCall{Method: "Write", Key: key, Data: content})
	if m.WriteFn != nil {
		return m.WriteFn(ctx, key, bytes.NewReader(content))
	}
	if m.WriteErr != nil {
		if err, ok := m.WriteErr[key]; ok {
			return err
		}
	}
	return nil
}

// Delete implements adapter.DataTransport.
func (m *MockTransport) Delete(ctx context.Context, key string) error {
	m.record(TransportCall{Method: "Delete", Key: key})
	if m.DeleteFn != nil {
		return m.DeleteFn(ctx, key)
	}
	if m.DeleteErr != nil {
		if err, ok := m.DeleteErr[key]; ok {
			return err
		}
	}
	return nil
}

// Hold implements adapter.DataTransport.
func (m *MockTransport) Hold(ctx context.Context, key string, until time.Time) error {
	m.record(TransportCall{Method: "Hold", Key: key, Until: until})
	if m.HoldFn != nil {
		return m.HoldFn(ctx, key, until)
	}
	if m.HoldErr != nil {
		if err, ok := m.HoldErr[key]; ok {
			return err
		}
	}
	return nil
}

// Release implements adapter.DataTransport.
func (m *MockTransport) Release(ctx context.Context, key string) error {
	m.record(TransportCall{Method: "Release", Key: key})
	if m.ReleaseFn != nil {
		return m.ReleaseFn(ctx, key)
	}
	return nil
}

// ListHeld implements adapter.DataTransport.
func (m *MockTransport) ListHeld(ctx context.Context) ([]types.HeldObject, error) {
	m.record(TransportCall{Method: "ListHeld"})
	if m.ListHeldFn != nil {
		return m.ListHeldFn(ctx)
	}
	return nil, nil
}

// HoldData implements adapter.DataTransport.
func (m *MockTransport) HoldData(ctx context.Context, key string, data io.Reader, until time.Time) error {
	m.record(TransportCall{Method: "HoldData", Key: key, Until: until})
	if m.HoldDataFn != nil {
		return m.HoldDataFn(ctx, key, data, until)
	}
	return nil
}

// ReleaseAll implements adapter.DataTransport.
func (m *MockTransport) ReleaseAll(ctx context.Context) error {
	m.record(TransportCall{Method: "ReleaseAll"})
	if m.ReleaseAllFn != nil {
		return m.ReleaseAllFn(ctx)
	}
	return nil
}
