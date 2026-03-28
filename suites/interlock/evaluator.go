package interlocksuite

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	interlocktypes "github.com/dwsmith1983/interlock/pkg/types"
	interlockvalidation "github.com/dwsmith1983/interlock/pkg/validation"
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

// LocalInterlockEvaluator embeds Interlock's validation engine for local testing.
// It reads pipeline config and sensor state, evaluates Interlock validation rules,
// and emits JOB_TRIGGERED or VALIDATION_EXHAUSTED events.
type LocalInterlockEvaluator struct {
	store       adapter.StateStore
	eventWriter *LocalEventReader
	clock       adapter.Clock
}

// NewLocalInterlockEvaluator returns a new LocalInterlockEvaluator.
func NewLocalInterlockEvaluator(store adapter.StateStore, eventWriter *LocalEventReader, clock adapter.Clock) *LocalInterlockEvaluator {
	return &LocalInterlockEvaluator{store: store, eventWriter: eventWriter, clock: clock}
}

// EvaluateAfterInjection reads the pipeline config and sensor data from the
// state store, evaluates Interlock validation rules, and emits a
// JOB_TRIGGERED or VALIDATION_EXHAUSTED event based on the result.
func (e *LocalInterlockEvaluator) EvaluateAfterInjection(ctx context.Context, pipeline, _, _ string) error {
	// 1. Read pipeline config.
	configBytes, err := e.store.ReadPipelineConfig(ctx, pipeline)
	if err != nil {
		return fmt.Errorf("read pipeline config: %w", err)
	}
	if configBytes == nil {
		// No config — emit VALIDATION_EXHAUSTED (no rules to evaluate).
		e.eventWriter.Emit(InterlockEventRecord{
			PipelineID: pipeline,
			EventType:  "VALIDATION_EXHAUSTED",
			Timestamp:  e.clock.Now(),
		})
		return nil
	}

	// 2. Parse pipeline config to extract validation rules.
	var config struct {
		Validation struct {
			Trigger string                          `yaml:"trigger" json:"trigger"`
			Rules   []interlocktypes.ValidationRule `yaml:"rules" json:"rules"`
		} `yaml:"validation" json:"validation"`
	}
	if err := yaml.Unmarshal(configBytes, &config); err != nil {
		// Try JSON as fallback.
		if err2 := json.Unmarshal(configBytes, &config); err2 != nil {
			return fmt.Errorf("parse pipeline config: yaml=%v, json=%v", err, err2)
		}
	}

	// 3. Build sensor data map from state store.
	sensors := make(map[string]map[string]interface{})
	for _, rule := range config.Validation.Rules {
		sensorData, err := e.store.ReadSensor(ctx, pipeline, rule.Key)
		if err != nil {
			continue // sensor not found — will be nil in map
		}
		if sensorData.Key != "" {
			data := make(map[string]interface{})
			data["status"] = string(sensorData.Status)
			for k, v := range sensorData.Metadata {
				data[k] = v
			}
			sensors[rule.Key] = data
		}
	}

	// 4. Evaluate rules using Interlock's validation engine.
	result := interlockvalidation.EvaluateRules(
		config.Validation.Trigger,
		config.Validation.Rules,
		sensors,
		e.clock.Now(),
	)

	// 5. Emit appropriate event.
	if result.Passed {
		e.eventWriter.Emit(InterlockEventRecord{
			PipelineID: pipeline,
			EventType:  "JOB_TRIGGERED",
			Timestamp:  e.clock.Now(),
		})
	} else {
		e.eventWriter.Emit(InterlockEventRecord{
			PipelineID: pipeline,
			EventType:  "VALIDATION_EXHAUSTED",
			Timestamp:  e.clock.Now(),
		})
	}

	return nil
}
