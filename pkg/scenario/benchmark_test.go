package scenario_test

import (
	"testing"

	"github.com/dwsmith1983/chaos-data/pkg/scenario"
)

// BenchmarkSelectDeterministic measures deterministic scenario selection from a
// catalog-sized set of scenarios.
func BenchmarkSelectDeterministic(b *testing.B) {
	scenarios := make([]scenario.Scenario, 20)
	names := make([]string, 5)
	for i := range scenarios {
		scenarios[i] = validScenario()
		scenarios[i].Name = "scenario-" + string(rune('a'+i))
	}
	// Select 5 scenarios from the 20 available.
	for i := range names {
		names[i] = scenarios[i*4].Name
	}

	b.ResetTimer()
	for range b.N {
		result := scenario.SelectDeterministic(scenarios, names)
		if len(result) != 5 {
			b.Fatalf("SelectDeterministic returned %d, want 5", len(result))
		}
	}
}

// BenchmarkScenarioValidate measures the cost of validating a fully populated
// scenario, including expected_response assertion validation.
func BenchmarkScenarioValidate(b *testing.B) {
	s := validScenario()

	b.ResetTimer()
	for range b.N {
		if err := s.Validate(); err != nil {
			b.Fatalf("Validate: %v", err)
		}
	}
}
