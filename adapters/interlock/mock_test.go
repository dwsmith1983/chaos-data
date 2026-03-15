package interlock_test

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

// transportCall records a single method invocation on the mock transport.
type transportCall struct {
	Method string
	Key    string
	Until  time.Time // only for Hold
	Data   []byte    // only for Write
}

// mockTransport implements adapter.DataTransport and records all method calls.
type mockTransport struct {
	mu    sync.Mutex
	calls []transportCall

	readData  map[string][]byte
	readErr   map[string]error
	writeErr  map[string]error
	holdErr   map[string]error
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

func (m *mockTransport) record(c transportCall) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, c)
}

func (m *mockTransport) getCalls() []transportCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]transportCall, len(m.calls))
	copy(result, m.calls)
	return result
}

func (m *mockTransport) List(_ context.Context, prefix string) ([]types.DataObject, error) {
	m.record(transportCall{Method: "List", Key: prefix})
	return nil, nil
}

func (m *mockTransport) Read(_ context.Context, key string) (io.ReadCloser, error) {
	m.record(transportCall{Method: "Read", Key: key})
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
	m.record(transportCall{Method: "Write", Key: key, Data: content})
	if err, ok := m.writeErr[key]; ok {
		return err
	}
	return nil
}

func (m *mockTransport) Delete(_ context.Context, key string) error {
	m.record(transportCall{Method: "Delete", Key: key})
	if err, ok := m.deleteErr[key]; ok {
		return err
	}
	return nil
}

func (m *mockTransport) Hold(_ context.Context, key string, until time.Time) error {
	m.record(transportCall{Method: "Hold", Key: key, Until: until})
	if err, ok := m.holdErr[key]; ok {
		return err
	}
	return nil
}

func (m *mockTransport) Release(_ context.Context, key string) error {
	m.record(transportCall{Method: "Release", Key: key})
	return nil
}

func (m *mockTransport) ListHeld(_ context.Context) ([]types.DataObject, error) {
	m.record(transportCall{Method: "ListHeld"})
	return nil, nil
}

// stateCall records a single method invocation on the mock state store.
type stateCall struct {
	Method   string
	Pipeline string
	Key      string
	Status   string
	Sensor   adapter.SensorData
	Trigger  adapter.TriggerKey
}

// mockStateStore implements adapter.StateStore and records all method calls.
type mockStateStore struct {
	mu    sync.Mutex
	calls []stateCall

	sensors  map[string]adapter.SensorData
	triggers map[string]string
	events   []types.ChaosEvent

	readSensorErr         bool
	writeSensorErr        bool
	writeTriggerStatusErr bool
}

func newMockStateStore() *mockStateStore {
	return &mockStateStore{
		sensors:  make(map[string]adapter.SensorData),
		triggers: make(map[string]string),
	}
}

func (m *mockStateStore) recordCall(c stateCall) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, c)
}

func (m *mockStateStore) getStateCalls() []stateCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]stateCall, len(m.calls))
	copy(result, m.calls)
	return result
}

func (m *mockStateStore) stateCallCount(method string) int {
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

func (m *mockStateStore) ReadSensor(_ context.Context, pipeline, key string) (adapter.SensorData, error) {
	m.recordCall(stateCall{Method: "ReadSensor", Pipeline: pipeline, Key: key})
	if m.readSensorErr {
		return adapter.SensorData{}, fmt.Errorf("read sensor error")
	}
	sKey := pipeline + "/" + key
	m.mu.Lock()
	s, ok := m.sensors[sKey]
	m.mu.Unlock()
	if !ok {
		return adapter.SensorData{}, fmt.Errorf("sensor not found: %s/%s", pipeline, key)
	}
	return s, nil
}

func (m *mockStateStore) WriteSensor(_ context.Context, pipeline, key string, data adapter.SensorData) error {
	m.recordCall(stateCall{Method: "WriteSensor", Pipeline: pipeline, Key: key, Sensor: data})
	if m.writeSensorErr {
		return fmt.Errorf("write sensor error")
	}
	m.mu.Lock()
	m.sensors[pipeline+"/"+key] = data
	m.mu.Unlock()
	return nil
}

func (m *mockStateStore) ReadTriggerStatus(_ context.Context, key adapter.TriggerKey) (string, error) {
	m.recordCall(stateCall{Method: "ReadTriggerStatus", Pipeline: key.Pipeline, Key: key.Schedule, Trigger: key})
	tKey := key.Pipeline + "/" + key.Schedule + "/" + key.Date
	m.mu.Lock()
	status, ok := m.triggers[tKey]
	m.mu.Unlock()
	if !ok {
		return "", fmt.Errorf("trigger not found: %s", tKey)
	}
	return status, nil
}

func (m *mockStateStore) WriteTriggerStatus(_ context.Context, key adapter.TriggerKey, status string) error {
	m.recordCall(stateCall{Method: "WriteTriggerStatus", Pipeline: key.Pipeline, Key: key.Schedule, Status: status, Trigger: key})
	if m.writeTriggerStatusErr {
		return fmt.Errorf("write trigger status error")
	}
	tKey := key.Pipeline + "/" + key.Schedule + "/" + key.Date
	m.mu.Lock()
	m.triggers[tKey] = status
	m.mu.Unlock()
	return nil
}

func (m *mockStateStore) WriteEvent(_ context.Context, event types.ChaosEvent) error {
	m.recordCall(stateCall{Method: "WriteEvent", Pipeline: event.Scenario})
	m.mu.Lock()
	m.events = append(m.events, event)
	m.mu.Unlock()
	return nil
}

func (m *mockStateStore) DeleteSensor(_ context.Context, pipeline, key string) error {
	m.recordCall(stateCall{Method: "DeleteSensor", Pipeline: pipeline, Key: key})
	sKey := pipeline + "/" + key
	m.mu.Lock()
	delete(m.sensors, sKey)
	m.mu.Unlock()
	return nil
}

func (m *mockStateStore) ReadChaosEvents(_ context.Context, _ string) ([]types.ChaosEvent, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]types.ChaosEvent, len(m.events))
	copy(result, m.events)
	return result, nil
}

// mockEventEmitter implements adapter.EventEmitter and records calls.
type mockEventEmitter struct {
	mu    sync.Mutex
	calls []types.ChaosEvent
	err   error
}

func newMockEventEmitter() *mockEventEmitter {
	return &mockEventEmitter{}
}

func (m *mockEventEmitter) Emit(_ context.Context, event types.ChaosEvent) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, event)
	return m.err
}

// mockEventReader implements adapter.EventReader and returns canned events.
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
