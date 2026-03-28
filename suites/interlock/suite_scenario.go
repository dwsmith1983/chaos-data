package interlocksuite

import (
	"fmt"
	"os"

	"github.com/dwsmith1983/chaos-data/pkg/scenario"
	"gopkg.in/yaml.v3"
)

// SetupSpec defines prerequisite state to write before chaos injection.
type SetupSpec struct {
	Pipeline       string                           `yaml:"pipeline"`
	PipelineConfig map[string]interface{}            `yaml:"pipeline_config,omitempty"`
	TriggerStatus  string                           `yaml:"trigger_status,omitempty"`
	Sensors        map[string]map[string]interface{} `yaml:"sensors,omitempty"`
}

// SuiteScenario wraps a chaos-data scenario with suite-specific metadata.
type SuiteScenario struct {
	scenario.Scenario `yaml:",inline"`
	Setup             *SetupSpec `yaml:"setup,omitempty"`
	Capability        string     `yaml:"capability,omitempty"`
}

// LoadSuiteScenario loads a suite scenario from a YAML file.
func LoadSuiteScenario(path string) (SuiteScenario, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return SuiteScenario{}, fmt.Errorf("load suite scenario %q: %w", path, err)
	}
	var ss SuiteScenario
	if err := yaml.Unmarshal(data, &ss); err != nil {
		return SuiteScenario{}, fmt.Errorf("parse suite scenario %q: %w", path, err)
	}
	return ss, nil
}
