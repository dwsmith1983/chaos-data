// Package scenario defines the Scenario type and its YAML/JSON parsing,
// representing a single chaos injection scenario configuration.
package scenario

import (
	"errors"
	"fmt"

	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// ErrInvalidScenario is returned when a scenario fails validation.
var ErrInvalidScenario = errors.New("invalid scenario")

// validLayer reports whether l is an allowed target layer value.
func validLayer(l string) bool {
	switch l {
	case "data", "state", "orchestrator":
		return true
	default:
		return false
	}
}

// validCategory reports whether c is an allowed scenario category.
func validCategory(c string) bool {
	switch c {
	case "data-arrival", "data-quality", "state-consistency",
		"infrastructure", "orchestrator", "compound":
		return true
	default:
		return false
	}
}

// Scenario represents a single chaos injection scenario loaded from YAML.
type Scenario struct {
	Name        string                      `yaml:"name"                       json:"name"`
	Description string                      `yaml:"description"                json:"description"`
	Category    string                      `yaml:"category"                   json:"category"`
	Severity    types.Severity              `yaml:"severity"                   json:"severity"`
	Version     int                         `yaml:"version"                    json:"version"`
	Target      TargetSpec                  `yaml:"target"                     json:"target"`
	Mutation    MutationSpec                `yaml:"mutation"                   json:"mutation"`
	Probability float64                    `yaml:"probability"                json:"probability"`
	Safety      ScenarioSafety              `yaml:"safety"                     json:"safety"`
	Expected    *ExpectedResponse `yaml:"expected_response,omitempty" json:"expected_response,omitempty"`
}

// TargetSpec identifies which layer, transport, and objects a scenario targets.
type TargetSpec struct {
	Layer     string     `yaml:"layer"     json:"layer"`
	Transport string     `yaml:"transport" json:"transport"`
	Filter    FilterSpec `yaml:"filter"    json:"filter"`
}

// FilterSpec narrows which objects within a target are affected.
type FilterSpec struct {
	Prefix string `yaml:"prefix" json:"prefix"`
	Match  string `yaml:"match"  json:"match"`
}

// MutationSpec describes the type and parameters of a chaos mutation.
type MutationSpec struct {
	Type   string            `yaml:"type"   json:"type"`
	Params map[string]string `yaml:"params" json:"params"`
}

// ScenarioSafety defines safety constraints for a scenario.
type ScenarioSafety struct {
	MaxAffectedPct int            `yaml:"max_affected_pct" json:"max_affected_pct"`
	Cooldown       types.Duration `yaml:"cooldown"         json:"cooldown"`
	SLAAware       bool           `yaml:"sla_aware"        json:"sla_aware"`
}

// ExpectedResponse describes an expected downstream reaction to chaos injection.
type ExpectedResponse struct {
	Within  types.Duration    `yaml:"within"  json:"within"`
	Asserts []types.Assertion `yaml:"asserts" json:"asserts"`
}

// Validate checks that all required fields are present and within valid ranges.
func (s Scenario) Validate() error {
	if s.Name == "" {
		return fmt.Errorf("%w: name must not be empty", ErrInvalidScenario)
	}
	if !validCategory(s.Category) {
		return fmt.Errorf("%w: invalid category %q", ErrInvalidScenario, s.Category)
	}
	if s.Target.Layer != "" && !validLayer(s.Target.Layer) {
		return fmt.Errorf("%w: invalid target layer %q", ErrInvalidScenario, s.Target.Layer)
	}
	if !s.Severity.IsValid() {
		return fmt.Errorf("%w: invalid severity %d", ErrInvalidScenario, int(s.Severity))
	}
	if s.Version < 1 {
		return fmt.Errorf("%w: version must be >= 1, got %d", ErrInvalidScenario, s.Version)
	}
	if s.Mutation.Type == "" {
		return fmt.Errorf("%w: mutation type must not be empty", ErrInvalidScenario)
	}
	if s.Probability < 0 || s.Probability > 1 {
		return fmt.Errorf("%w: probability must be in [0.0, 1.0], got %v", ErrInvalidScenario, s.Probability)
	}
	if s.Safety.MaxAffectedPct < 0 || s.Safety.MaxAffectedPct > 100 {
		return fmt.Errorf("%w: max_affected_pct must be in [0, 100], got %d", ErrInvalidScenario, s.Safety.MaxAffectedPct)
	}
	if s.Expected != nil {
		if s.Expected.Within.Duration <= 0 {
			return fmt.Errorf("%w: expected_response within must be > 0", ErrInvalidScenario)
		}
		if len(s.Expected.Asserts) == 0 {
			return fmt.Errorf("%w: expected_response must have at least one assertion", ErrInvalidScenario)
		}
		for i, a := range s.Expected.Asserts {
			if err := a.Validate(); err != nil {
				return fmt.Errorf("%w: expected_response asserts[%d]: %s", ErrInvalidScenario, i, err)
			}
		}
	}
	return nil
}
