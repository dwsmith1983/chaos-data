package interlocksuite

import (
	"context"
	"fmt"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"gopkg.in/yaml.v3"
)

// InterlockEvaluator triggers Interlock rule evaluation after chaos injection.
type InterlockEvaluator interface {
	EvaluateAfterInjection(ctx context.Context, pipeline, schedule, date string) error
}

// ---------------------------------------------------------------------------
// AWS implementation (no-op)
// ---------------------------------------------------------------------------

// AWSInterlockEvaluator is a no-op — DynamoDB Streams triggers evaluation
// automatically via the stream-router Lambda.
type AWSInterlockEvaluator struct{}

// NewAWSInterlockEvaluator returns a new AWSInterlockEvaluator.
func NewAWSInterlockEvaluator() *AWSInterlockEvaluator {
	return &AWSInterlockEvaluator{}
}

// EvaluateAfterInjection is a no-op for AWS. DynamoDB Streams → stream-router
// Lambda handles evaluation automatically when state changes.
func (e *AWSInterlockEvaluator) EvaluateAfterInjection(_ context.Context, _, _, _ string) error {
	return nil
}

// ---------------------------------------------------------------------------
// Local implementation
// ---------------------------------------------------------------------------

// LocalInterlockEvaluator dispatches evaluation to a sequence of EvalModule
// implementations. Each module inspects the pipeline config and emits events
// for its domain. Modules execute sequentially and share mutable state (store +
// eventWriter), so events emitted by earlier modules are visible to later ones.
type LocalInterlockEvaluator struct {
	store       adapter.StateStore
	eventWriter *LocalEventReader
	clock       adapter.Clock
	modules     []EvalModule
	sensorKeys  []string // populated per-scenario by SetSensorKeys
}

// NewLocalInterlockEvaluator returns a new LocalInterlockEvaluator with the
// default module chain: Validation → Trigger.
func NewLocalInterlockEvaluator(store adapter.StateStore, eventWriter *LocalEventReader, clock adapter.Clock) *LocalInterlockEvaluator {
	return &LocalInterlockEvaluator{
		store:       store,
		eventWriter: eventWriter,
		clock:       clock,
		modules: []EvalModule{
			NewValidationModule(),
			NewTriggerModule(),
		},
	}
}

// SetSensorKeys sets the sensor keys from the current scenario's setup spec.
// Must be called before EvaluateAfterInjection for each scenario.
func (e *LocalInterlockEvaluator) SetSensorKeys(keys []string) {
	e.sensorKeys = keys
}

// EvaluateAfterInjection reads the pipeline config, parses it to a generic
// map, and dispatches evaluation to each registered module in order.
func (e *LocalInterlockEvaluator) EvaluateAfterInjection(ctx context.Context, pipeline, schedule, date string) error {
	// Read pipeline config.
	configBytes, err := e.store.ReadPipelineConfig(ctx, pipeline)
	if err != nil {
		return fmt.Errorf("read pipeline config: %w", err)
	}
	if configBytes == nil {
		// No config — emit VALIDATION_EXHAUSTED (nothing to evaluate).
		e.eventWriter.Emit(InterlockEventRecord{
			PipelineID: pipeline,
			EventType:  "VALIDATION_EXHAUSTED",
			Timestamp:  e.clock.Now(),
		})
		return nil
	}

	// Parse config to generic map for modules.
	var config map[string]any
	if err := yaml.Unmarshal(configBytes, &config); err != nil {
		return fmt.Errorf("parse pipeline config: %w", err)
	}

	params := EvalParams{
		Pipeline:    pipeline,
		Config:      config,
		Store:       e.store,
		EventWriter: e.eventWriter,
		Clock:       e.clock,
		Schedule:    schedule,
		Date:        date,
		SensorKeys:  e.sensorKeys,
	}

	// Dispatch to modules in order.
	for _, mod := range e.modules {
		if err := mod.Evaluate(ctx, params); err != nil {
			return fmt.Errorf("module %s: %w", mod.Name(), err)
		}
	}

	return nil
}
