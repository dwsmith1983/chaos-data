package airflow

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// Compile-time assertions: AirflowVariableState implements StateStore and all
// sub-interfaces.
var (
	_ adapter.StateStore   = (*AirflowVariableState)(nil)
	_ adapter.SensorStore  = (*AirflowVariableState)(nil)
	_ adapter.TriggerStore = (*AirflowVariableState)(nil)
	_ adapter.EventStore   = (*AirflowVariableState)(nil)
)

// maxPipelineConfigSize is the maximum size in bytes for pipeline configs stored
// in Airflow Variables. Airflow Variables are stored in the metadata DB and are
// not intended for large blobs.
const maxPipelineConfigSize = 64 * 1024 // 64 KB

// AirflowVariableState implements adapter.StateStore backed by Airflow Variables.
type AirflowVariableState struct {
	api AirflowVariableAPI
}

// NewAirflowVariableState creates a new AirflowVariableState using the given
// AirflowVariableAPI for persistence.
func NewAirflowVariableState(api AirflowVariableAPI) *AirflowVariableState {
	return &AirflowVariableState{api: api}
}

// ---------------------------------------------------------------------------
// SensorStore
// ---------------------------------------------------------------------------

// ReadSensor returns the sensor data for the given pipeline and key.
// If the variable does not exist, a zero-value SensorData is returned without
// error (matching SQLiteState behavior).
func (s *AirflowVariableState) ReadSensor(ctx context.Context, pipeline, key string) (adapter.SensorData, error) {
	varKey := SensorKey(pipeline, key)
	v, err := s.api.GetVariable(ctx, varKey)
	if errors.Is(err, ErrVariableNotFound) {
		return adapter.SensorData{}, nil
	}
	if err != nil {
		return adapter.SensorData{}, fmt.Errorf("airflow: read sensor: %w", err)
	}

	var data adapter.SensorData
	if err := json.Unmarshal([]byte(v.Value), &data); err != nil {
		return adapter.SensorData{}, fmt.Errorf("airflow: read sensor: unmarshal: %w", err)
	}
	return data, nil
}

// WriteSensor stores sensor data as a JSON-encoded Airflow Variable.
func (s *AirflowVariableState) WriteSensor(ctx context.Context, pipeline, key string, data adapter.SensorData) error {
	// Normalize nil metadata to empty map for consistent JSON.
	if data.Metadata == nil {
		data.Metadata = map[string]string{}
	}

	raw, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("airflow: write sensor: marshal: %w", err)
	}

	varKey := SensorKey(pipeline, key)
	return s.api.SetVariable(ctx, Variable{
		Key:         varKey,
		Value:       string(raw),
		Description: fmt.Sprintf("chaos-data sensor: %s/%s", pipeline, key),
	})
}

// DeleteSensor removes the sensor variable.
func (s *AirflowVariableState) DeleteSensor(ctx context.Context, pipeline, key string) error {
	varKey := SensorKey(pipeline, key)
	if err := s.api.DeleteVariable(ctx, varKey); err != nil {
		return fmt.Errorf("airflow: delete sensor: %w", err)
	}
	return nil
}

// ---------------------------------------------------------------------------
// TriggerStore
// ---------------------------------------------------------------------------

// ReadTriggerStatus returns the trigger status string for the given key.
// If the variable does not exist, an empty string is returned without error.
func (s *AirflowVariableState) ReadTriggerStatus(ctx context.Context, key adapter.TriggerKey) (string, error) {
	varKey := TriggerStatusKey(key.Pipeline, key.Schedule, key.Date)
	v, err := s.api.GetVariable(ctx, varKey)
	if errors.Is(err, ErrVariableNotFound) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("airflow: read trigger status: %w", err)
	}
	return v.Value, nil
}

// WriteTriggerStatus stores the trigger status string.
func (s *AirflowVariableState) WriteTriggerStatus(ctx context.Context, key adapter.TriggerKey, status string) error {
	varKey := TriggerStatusKey(key.Pipeline, key.Schedule, key.Date)
	return s.api.SetVariable(ctx, Variable{
		Key:         varKey,
		Value:       status,
		Description: fmt.Sprintf("chaos-data trigger: %s/%s/%s", key.Pipeline, key.Schedule, key.Date),
	})
}

// ---------------------------------------------------------------------------
// EventStore
// ---------------------------------------------------------------------------

// WriteEvent stores a chaos event as a JSON-encoded Airflow Variable.
func (s *AirflowVariableState) WriteEvent(ctx context.Context, event types.ChaosEvent) error {
	raw, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("airflow: write event: marshal: %w", err)
	}

	varKey := EventKey(event.ExperimentID, event.Timestamp, event.ID)
	return s.api.SetVariable(ctx, Variable{
		Key:         varKey,
		Value:       string(raw),
		Description: fmt.Sprintf("chaos-data event: %s", event.ID),
	})
}

// ReadChaosEvents returns all chaos events for the given experiment ID, sorted
// by timestamp ascending. If no events exist, a non-nil empty slice is returned.
func (s *AirflowVariableState) ReadChaosEvents(ctx context.Context, experimentID string) ([]types.ChaosEvent, error) {
	prefix := EventKeyPrefix(experimentID)
	vars, err := s.listByPrefix(ctx, prefix)
	if err != nil {
		return nil, fmt.Errorf("airflow: read chaos events: %w", err)
	}

	events := make([]types.ChaosEvent, 0, len(vars))
	for _, v := range vars {
		var e types.ChaosEvent
		if err := json.Unmarshal([]byte(v.Value), &e); err != nil {
			return nil, fmt.Errorf("airflow: read chaos events: unmarshal: %w", err)
		}
		events = append(events, e)
	}

	sort.Slice(events, func(i, j int) bool {
		return events[i].Timestamp.Before(events[j].Timestamp)
	})

	return events, nil
}

// WritePipelineConfig stores a pipeline configuration blob. The bytes are
// base64-encoded before storage because Airflow Variables are strings.
// Configs larger than 64 KB are rejected.
func (s *AirflowVariableState) WritePipelineConfig(ctx context.Context, pipeline string, config []byte) error {
	if len(config) > maxPipelineConfigSize {
		return fmt.Errorf("airflow: write pipeline config: size %d exceeds maximum %d bytes", len(config), maxPipelineConfigSize)
	}

	encoded := base64.StdEncoding.EncodeToString(config)
	varKey := ConfigKey(pipeline)
	return s.api.SetVariable(ctx, Variable{
		Key:         varKey,
		Value:       encoded,
		Description: fmt.Sprintf("chaos-data config: %s", pipeline),
	})
}

// ReadPipelineConfig retrieves and base64-decodes a pipeline configuration blob.
// If the variable does not exist, nil is returned without error.
func (s *AirflowVariableState) ReadPipelineConfig(ctx context.Context, pipeline string) ([]byte, error) {
	varKey := ConfigKey(pipeline)
	v, err := s.api.GetVariable(ctx, varKey)
	if errors.Is(err, ErrVariableNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("airflow: read pipeline config: %w", err)
	}

	decoded, err := base64.StdEncoding.DecodeString(v.Value)
	if err != nil {
		return nil, fmt.Errorf("airflow: read pipeline config: base64 decode: %w", err)
	}
	return decoded, nil
}

// DeleteByPrefix removes all Airflow Variables whose key starts with
// "chaos:" + prefix. Deletions are best-effort: if one fails, the error is
// returned but preceding deletions are not rolled back.
func (s *AirflowVariableState) DeleteByPrefix(ctx context.Context, prefix string) error {
	fullPrefix := keyPrefix + prefix
	vars, err := s.listByPrefix(ctx, fullPrefix)
	if err != nil {
		return fmt.Errorf("airflow: delete by prefix: %w", err)
	}

	for _, v := range vars {
		if err := s.api.DeleteVariable(ctx, v.Key); err != nil {
			return fmt.Errorf("airflow: delete by prefix: delete %q: %w", v.Key, err)
		}
	}
	return nil
}

// CountReruns returns the number of rerun records for a pipeline/schedule/date.
func (s *AirflowVariableState) CountReruns(ctx context.Context, pipeline, schedule, date string) (int, error) {
	prefix := RerunKeyPrefix(pipeline, schedule, date)
	vars, err := s.listByPrefix(ctx, prefix)
	if err != nil {
		return 0, fmt.Errorf("airflow: count reruns: %w", err)
	}
	return len(vars), nil
}

// WriteRerun records a rerun event.
func (s *AirflowVariableState) WriteRerun(ctx context.Context, pipeline, schedule, date, reason string) error {
	ts := time.Now()
	varKey := RerunKey(pipeline, schedule, date, ts)
	return s.api.SetVariable(ctx, Variable{
		Key:         varKey,
		Value:       reason,
		Description: fmt.Sprintf("chaos-data rerun: %s/%s/%s", pipeline, schedule, date),
	})
}

// ReadJobEvents returns job events for a pipeline/schedule/date, ordered by
// timestamp descending (most recent first). If no events exist, a non-nil
// empty slice is returned.
func (s *AirflowVariableState) ReadJobEvents(ctx context.Context, pipeline, schedule, date string) ([]adapter.JobEvent, error) {
	prefix := JobEventKeyPrefix(pipeline, schedule, date)
	vars, err := s.listByPrefix(ctx, prefix)
	if err != nil {
		return nil, fmt.Errorf("airflow: read job events: %w", err)
	}

	events := make([]adapter.JobEvent, 0, len(vars))
	for _, v := range vars {
		var je adapter.JobEvent
		if err := json.Unmarshal([]byte(v.Value), &je); err != nil {
			return nil, fmt.Errorf("airflow: read job events: unmarshal: %w", err)
		}
		events = append(events, je)
	}

	sort.Slice(events, func(i, j int) bool {
		return events[i].Timestamp.After(events[j].Timestamp)
	})

	return events, nil
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// listByPrefix returns all variables whose key starts with prefix.
// NOTE: This performs a full scan of all Airflow Variables (paginated) because
// the Variables REST API has no server-side prefix filter. The cost is O(N)
// API calls where N = ceil(total_variables / 100). Acceptable for typical
// chaos-data usage where the total chaos:* variable count is small (< 100).
func (s *AirflowVariableState) listByPrefix(ctx context.Context, prefix string) ([]Variable, error) {
	all, err := s.api.ListVariables(ctx)
	if err != nil {
		return nil, err
	}

	var matched []Variable
	for _, v := range all {
		if HasPrefix(v.Key, prefix) {
			matched = append(matched, v)
		}
	}
	return matched, nil
}
