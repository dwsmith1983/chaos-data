package testutil

import (
	"context"
	"fmt"
	"sync"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// Compile-time interface assertion.
var _ adapter.StateStore = (*MockStateStore)(nil)

// StateCall records a single method invocation on MockStateStore.
type StateCall struct {
	Method   string
	Pipeline string
	Key      string
	Status   string
	Sensor   adapter.SensorData
	Trigger  adapter.TriggerKey
}

// MockStateStore is a reusable test double for adapter.StateStore.
//
// It provides in-memory sensor and trigger storage, records all method
// invocations, and supports injectable error flags.
type MockStateStore struct {
	mu sync.Mutex

	// Sensors maps "pipeline/key" to SensorData.
	Sensors map[string]adapter.SensorData

	// Triggers maps "pipeline/schedule/date" to status string.
	Triggers map[string]string

	// Events stores written chaos events.
	Events []types.ChaosEvent

	// Error flags — when true, the corresponding method returns an error.
	ReadSensorErr         bool
	WriteSensorErr        bool
	WriteTriggerStatusErr bool

	calls []StateCall
}

// NewMockStateStore returns a MockStateStore with initialised maps.
func NewMockStateStore() *MockStateStore {
	return &MockStateStore{
		Sensors:  make(map[string]adapter.SensorData),
		Triggers: make(map[string]string),
	}
}

func (m *MockStateStore) recordCall(c StateCall) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, c)
}

// GetStateCalls returns a snapshot of all recorded method invocations.
func (m *MockStateStore) GetStateCalls() []StateCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]StateCall, len(m.calls))
	copy(result, m.calls)
	return result
}

// StateCallCount returns the number of recorded invocations for the given method.
func (m *MockStateStore) StateCallCount(method string) int {
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

// ReadSensor implements adapter.StateStore.
func (m *MockStateStore) ReadSensor(_ context.Context, pipeline, key string) (adapter.SensorData, error) {
	m.recordCall(StateCall{Method: "ReadSensor", Pipeline: pipeline, Key: key})
	if m.ReadSensorErr {
		return adapter.SensorData{}, fmt.Errorf("read sensor error")
	}
	sKey := pipeline + "/" + key
	m.mu.Lock()
	s, ok := m.Sensors[sKey]
	m.mu.Unlock()
	if !ok {
		// Return zero-value + nil to match SQLiteState behavior for not-found rows.
		return adapter.SensorData{}, nil
	}
	return s, nil
}

// WriteSensor implements adapter.StateStore.
func (m *MockStateStore) WriteSensor(_ context.Context, pipeline, key string, data adapter.SensorData) error {
	m.recordCall(StateCall{Method: "WriteSensor", Pipeline: pipeline, Key: key, Sensor: data})
	if m.WriteSensorErr {
		return fmt.Errorf("write sensor error")
	}
	m.mu.Lock()
	m.Sensors[pipeline+"/"+key] = data
	m.mu.Unlock()
	return nil
}

// ReadTriggerStatus implements adapter.StateStore.
func (m *MockStateStore) ReadTriggerStatus(_ context.Context, key adapter.TriggerKey) (string, error) {
	m.recordCall(StateCall{Method: "ReadTriggerStatus", Pipeline: key.Pipeline, Key: key.Schedule, Trigger: key})
	tKey := key.Pipeline + "/" + key.Schedule + "/" + key.Date
	m.mu.Lock()
	status, ok := m.Triggers[tKey]
	m.mu.Unlock()
	if !ok {
		return "", fmt.Errorf("trigger not found: %s", tKey)
	}
	return status, nil
}

// WriteTriggerStatus implements adapter.StateStore.
func (m *MockStateStore) WriteTriggerStatus(_ context.Context, key adapter.TriggerKey, status string) error {
	m.recordCall(StateCall{Method: "WriteTriggerStatus", Pipeline: key.Pipeline, Key: key.Schedule, Status: status, Trigger: key})
	if m.WriteTriggerStatusErr {
		return fmt.Errorf("write trigger status error")
	}
	tKey := key.Pipeline + "/" + key.Schedule + "/" + key.Date
	m.mu.Lock()
	m.Triggers[tKey] = status
	m.mu.Unlock()
	return nil
}

// WriteEvent implements adapter.StateStore.
func (m *MockStateStore) WriteEvent(_ context.Context, event types.ChaosEvent) error {
	m.recordCall(StateCall{Method: "WriteEvent", Pipeline: event.Scenario})
	m.mu.Lock()
	m.Events = append(m.Events, event)
	m.mu.Unlock()
	return nil
}

// DeleteSensor implements adapter.StateStore.
func (m *MockStateStore) DeleteSensor(_ context.Context, pipeline, key string) error {
	m.recordCall(StateCall{Method: "DeleteSensor", Pipeline: pipeline, Key: key})
	sKey := pipeline + "/" + key
	m.mu.Lock()
	delete(m.Sensors, sKey)
	m.mu.Unlock()
	return nil
}

// ReadChaosEvents implements adapter.StateStore.
func (m *MockStateStore) ReadChaosEvents(_ context.Context, _ string) ([]types.ChaosEvent, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]types.ChaosEvent, len(m.Events))
	copy(result, m.Events)
	return result, nil
}

// WritePipelineConfig implements adapter.StateStore.
func (m *MockStateStore) WritePipelineConfig(_ context.Context, _ string, _ []byte) error {
	m.recordCall(StateCall{Method: "WritePipelineConfig"})
	return nil
}

// ReadPipelineConfig implements adapter.StateStore.
func (m *MockStateStore) ReadPipelineConfig(_ context.Context, pipeline string) ([]byte, error) {
	m.recordCall(StateCall{Method: "ReadPipelineConfig", Pipeline: pipeline})
	return nil, nil
}

// DeleteByPrefix implements adapter.StateStore.
func (m *MockStateStore) DeleteByPrefix(_ context.Context, prefix string) error {
	m.recordCall(StateCall{Method: "DeleteByPrefix", Key: prefix})
	return nil
}

// CountReruns implements adapter.StateStore.
func (m *MockStateStore) CountReruns(_ context.Context, pipeline, schedule, date string) (int, error) {
	m.recordCall(StateCall{Method: "CountReruns", Pipeline: pipeline, Key: schedule, Status: date})
	return 0, nil
}

// WriteRerun implements adapter.StateStore.
func (m *MockStateStore) WriteRerun(_ context.Context, pipeline, schedule, date, reason string) error {
	m.recordCall(StateCall{Method: "WriteRerun", Pipeline: pipeline, Key: schedule, Status: reason})
	_ = date
	return nil
}

// ReadJobEvents implements adapter.StateStore.
func (m *MockStateStore) ReadJobEvents(_ context.Context, pipeline, schedule, date string) ([]adapter.JobEvent, error) {
	m.recordCall(StateCall{Method: "ReadJobEvents", Pipeline: pipeline, Key: schedule, Status: date})
	return nil, nil
}

// SetSensor safely sets a sensor value in the Sensors map. This is useful
// for goroutines that need to update sensor state concurrently.
func (m *MockStateStore) SetSensor(pipeline, key string, data adapter.SensorData) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Sensors[pipeline+"/"+key] = data
}
