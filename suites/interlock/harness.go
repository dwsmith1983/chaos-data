package interlocksuite

import (
	"context"
	"fmt"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
	"gopkg.in/yaml.v3"
)

// Harness manages state setup and teardown for chaos scenarios.
// Each harness instance operates within an isolated namespace derived from the
// run ID so that concurrent suite executions do not interfere with each other.
type Harness struct {
	store     adapter.StateStore
	namespace string // e.g. "suite-001-"
	clock     adapter.Clock
}

// NewHarness returns a Harness that prefixes all state keys with
// "suite-<runID>-".
func NewHarness(store adapter.StateStore, clock adapter.Clock, runID string) *Harness {
	return &Harness{
		store:     store,
		namespace: "suite-" + runID + "-",
		clock:     clock,
	}
}

// NamespacedPipeline returns the pipeline ID prefixed with the harness
// namespace.
func (h *Harness) NamespacedPipeline(pipeline string) string {
	return h.namespace + pipeline
}

// Setup writes prerequisite state described by spec into the state store.
// All pipeline references are namespaced so that concurrent runs are isolated.
func (h *Harness) Setup(ctx context.Context, spec SetupSpec) error {
	nsPipeline := h.NamespacedPipeline(spec.Pipeline)

	// Write pipeline config if provided.
	if spec.PipelineConfig != nil {
		configBytes, err := yaml.Marshal(spec.PipelineConfig)
		if err != nil {
			return fmt.Errorf("harness setup: marshal pipeline config: %w", err)
		}
		if err := h.store.WritePipelineConfig(ctx, nsPipeline, configBytes); err != nil {
			return fmt.Errorf("harness setup: write pipeline config: %w", err)
		}
	}

	// Write trigger status if provided.
	if spec.TriggerStatus != "" {
		key := adapter.TriggerKey{Pipeline: nsPipeline, Schedule: "default", Date: "default"}
		if err := h.store.WriteTriggerStatus(ctx, key, spec.TriggerStatus); err != nil {
			return fmt.Errorf("harness setup: write trigger status: %w", err)
		}
	}

	// Write sensors if provided.
	for sensorKey, sensorData := range spec.Sensors {
		// status field is optional — use empty string if missing or not a string.
		statusVal, _ := sensorData["status"].(string)
		metadata := toStringMap(sensorData)
		// Preserve baseline sensor_count so PostRunModule can detect drift.
		if sc, ok := metadata["sensor_count"]; ok {
			metadata["__baseline_sensor_count"] = sc
		}
		sd := adapter.SensorData{
			Pipeline:    nsPipeline,
			Key:         sensorKey,
			Status:      types.SensorStatus(statusVal),
			LastUpdated: h.clock.Now(),
			Metadata:    metadata,
		}
		if err := h.store.WriteSensor(ctx, nsPipeline, sensorKey, sd); err != nil {
			return fmt.Errorf("harness setup: write sensor %q: %w", sensorKey, err)
		}
	}

	return nil
}

// Teardown removes all state entries whose pipeline starts with the harness
// namespace prefix.
func (h *Harness) Teardown(ctx context.Context) error {
	if err := h.store.DeleteByPrefix(ctx, h.namespace); err != nil {
		return fmt.Errorf("harness teardown: %w", err)
	}
	return nil
}

// toStringMap converts a map[string]interface{} to map[string]string by
// formatting each value with fmt.Sprintf.
func toStringMap(m map[string]interface{}) map[string]string {
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = fmt.Sprintf("%v", v)
	}
	return out
}
