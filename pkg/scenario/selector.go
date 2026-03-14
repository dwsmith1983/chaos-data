package scenario

import (
	"math/rand"

	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// SelectDeterministic returns scenarios whose Name matches one of the provided
// names. The result preserves the order of names. Names that do not match any
// scenario are silently skipped. The input slice is never modified.
//
// If names contains duplicates, the corresponding scenario appears multiple
// times in the result.
func SelectDeterministic(scenarios []Scenario, names []string) []Scenario {
	index := make(map[string]Scenario, len(scenarios))
	for _, s := range scenarios {
		index[s.Name] = s
	}

	result := make([]Scenario, 0, len(names))
	for _, name := range names {
		if s, ok := index[name]; ok {
			result = append(result, s)
		}
	}

	return result
}

// SelectProbabilistic returns scenarios selected by probabilistic sampling.
// For each scenario, it is included if rng.Float64() < scenario.Probability.
// A scenario with Probability 1.0 is always included; 0.0 is never included.
// The input slice is never modified.
//
// The provided rng must not be shared across goroutines without external
// synchronization.
func SelectProbabilistic(scenarios []Scenario, rng *rand.Rand) []Scenario {
	result := make([]Scenario, 0, len(scenarios))
	for _, s := range scenarios {
		switch {
		case s.Probability <= 0.0:
			// never include
		case s.Probability >= 1.0:
			result = append(result, s)
		default:
			if rng.Float64() < s.Probability {
				result = append(result, s)
			}
		}
	}

	return result
}

// FilterBySeverity returns scenarios whose Severity does not exceed
// maxSeverity. It uses Severity.ExceedsThreshold to determine whether a
// scenario should be excluded. The input slice is never modified.
func FilterBySeverity(scenarios []Scenario, maxSeverity types.Severity) []Scenario {
	result := make([]Scenario, 0, len(scenarios))
	for _, s := range scenarios {
		if !s.Severity.ExceedsThreshold(maxSeverity) {
			result = append(result, s)
		}
	}

	return result
}

// FilterByCategory returns scenarios matching the given category.
// The input slice is never modified.
func FilterByCategory(scenarios []Scenario, category string) []Scenario {
	result := make([]Scenario, 0, len(scenarios))
	for _, s := range scenarios {
		if s.Category == category {
			result = append(result, s)
		}
	}

	return result
}
