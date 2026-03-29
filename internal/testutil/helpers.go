// Package testutil provides reusable mock implementations and helper functions
// for testing chaos-data components. All mocks are thread-safe and support
// both function-callback and declarative configuration styles.
package testutil

import (
	"time"

	"github.com/dwsmith1983/chaos-data/pkg/scenario"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// NewTestObject creates a standard DataObject for testing with the given key.
func NewTestObject(key string) types.DataObject {
	return types.DataObject{
		Key:          key,
		Size:         100,
		LastModified: time.Now(),
	}
}

// NewTestScenario creates a minimal valid Scenario for testing.
//
// The scenario targets all objects (empty prefix/match), uses the given
// mutation type with no params, and has probability 1.0 for deterministic
// behavior in tests.
func NewTestScenario(name, mutationType string) scenario.Scenario {
	return scenario.Scenario{
		Name:        name,
		Description: "test scenario: " + name,
		Category:    "data-arrival",
		Severity:    types.SeverityLow,
		Version:     1,
		Target: scenario.TargetSpec{
			Layer: "data",
			Filter: scenario.FilterSpec{
				Prefix: "",
				Match:  "",
			},
		},
		Mutation: scenario.MutationSpec{
			Type:   mutationType,
			Params: make(map[string]string),
		},
		Probability: 1.0,
		Safety: scenario.ScenarioSafety{
			MaxAffectedPct: 100,
		},
	}
}
