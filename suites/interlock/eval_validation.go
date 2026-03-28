package interlocksuite

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	interlocktypes "github.com/dwsmith1983/interlock/pkg/types"
	interlockvalidation "github.com/dwsmith1983/interlock/pkg/validation"
	"gopkg.in/yaml.v3"
)

// ValidationModule evaluates Interlock validation rules against sensor state
// and emits JOB_TRIGGERED or VALIDATION_EXHAUSTED.
type ValidationModule struct{}

// NewValidationModule returns a new ValidationModule.
func NewValidationModule() *ValidationModule { return &ValidationModule{} }

// Name returns the module identifier.
func (m *ValidationModule) Name() string { return "validation" }

// Evaluate reads validation rules from the pipeline config, builds a sensor
// data map from the state store, and delegates to the Interlock validation
// engine. Emits JOB_TRIGGERED if rules pass, VALIDATION_EXHAUSTED otherwise.
// Silently returns nil when the config has no validation section.
func (m *ValidationModule) Evaluate(ctx context.Context, p EvalParams) error {
	// Parse pipeline config to extract validation rules.
	configBytes, err := marshalConfig(p.Config)
	if err != nil {
		return fmt.Errorf("validation module: marshal config: %w", err)
	}

	var vc struct {
		Validation struct {
			Trigger string                          `yaml:"trigger" json:"trigger"`
			Rules   []interlocktypes.ValidationRule `yaml:"rules" json:"rules"`
		} `yaml:"validation" json:"validation"`
	}
	if err := yaml.Unmarshal(configBytes, &vc); err != nil {
		if err2 := json.Unmarshal(configBytes, &vc); err2 != nil {
			return fmt.Errorf("validation module: parse config: yaml=%v, json=%v", err, err2)
		}
	}

	if len(vc.Validation.Rules) == 0 {
		return nil // no validation section — skip
	}

	// Build sensor data map from state store.
	// Rule keys may include a SENSOR# prefix (interlock convention). Strip it
	// before calling ReadSensor since the harness writes bare keys.
	sensors := make(map[string]map[string]any)
	for _, rule := range vc.Validation.Rules {
		bareKey := strings.TrimPrefix(rule.Key, "SENSOR#")
		sensorData, err := p.Store.ReadSensor(ctx, p.Pipeline, bareKey)
		if err != nil {
			continue
		}
		if sensorData.Key != "" {
			data := make(map[string]any)
			data["status"] = string(sensorData.Status)
			for k, v := range sensorData.Metadata {
				data[k] = v
			}
			sensors[rule.Key] = data
		}
	}

	// Evaluate rules using Interlock's validation engine.
	result := interlockvalidation.EvaluateRules(
		vc.Validation.Trigger,
		vc.Validation.Rules,
		sensors,
		p.Clock.Now(),
	)

	if result.Passed {
		p.EventWriter.Emit(InterlockEventRecord{
			PipelineID: p.Pipeline,
			EventType:  "JOB_TRIGGERED",
			Timestamp:  p.Clock.Now(),
		})
	} else {
		p.EventWriter.Emit(InterlockEventRecord{
			PipelineID: p.Pipeline,
			EventType:  "VALIDATION_EXHAUSTED",
			Timestamp:  p.Clock.Now(),
		})
	}

	return nil
}

// marshalConfig converts the parsed map back to bytes for structured
// unmarshalling into the validation config type.
func marshalConfig(config map[string]any) ([]byte, error) {
	return yaml.Marshal(config)
}
