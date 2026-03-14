package scenario_test

import (
	"math"
	"math/rand"
	"testing"

	"github.com/dwsmith1983/chaos-data/pkg/scenario"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// makeScenario builds a minimal Scenario with the given name, category,
// severity, and probability. Fields not relevant to selector logic use
// sensible defaults.
func makeScenario(name, category string, severity types.Severity, probability float64) scenario.Scenario {
	return scenario.Scenario{
		Name:        name,
		Category:    category,
		Severity:    severity,
		Version:     1,
		Probability: probability,
		Mutation:    scenario.MutationSpec{Type: "delay"},
	}
}

// sampleScenarios returns a reusable set of four scenarios for testing.
func sampleScenarios() []scenario.Scenario {
	return []scenario.Scenario{
		makeScenario("alpha", "data-arrival", types.SeverityLow, 0.5),
		makeScenario("bravo", "data-quality", types.SeverityModerate, 1.0),
		makeScenario("charlie", "data-arrival", types.SeveritySevere, 0.0),
		makeScenario("delta", "infrastructure", types.SeverityCritical, 0.8),
	}
}

func TestSelectDeterministic(t *testing.T) {
	tests := []struct {
		name      string
		scenarios []scenario.Scenario
		names     []string
		wantNames []string
	}{
		{
			name:      "matches by name and preserves order of names",
			scenarios: sampleScenarios(),
			names:     []string{"charlie", "alpha"},
			wantNames: []string{"charlie", "alpha"},
		},
		{
			name:      "skips unknown names",
			scenarios: sampleScenarios(),
			names:     []string{"alpha", "unknown", "bravo"},
			wantNames: []string{"alpha", "bravo"},
		},
		{
			name:      "empty names returns empty",
			scenarios: sampleScenarios(),
			names:     []string{},
			wantNames: []string{},
		},
		{
			name:      "nil names returns empty",
			scenarios: sampleScenarios(),
			names:     nil,
			wantNames: []string{},
		},
		{
			name:      "empty scenarios returns empty",
			scenarios: []scenario.Scenario{},
			names:     []string{"alpha"},
			wantNames: []string{},
		},
		{
			name:      "nil scenarios returns empty",
			scenarios: nil,
			names:     []string{"alpha"},
			wantNames: []string{},
		},
		{
			name:      "all names match",
			scenarios: sampleScenarios(),
			names:     []string{"delta", "bravo", "charlie", "alpha"},
			wantNames: []string{"delta", "bravo", "charlie", "alpha"},
		},
		{
			name:      "no names match",
			scenarios: sampleScenarios(),
			names:     []string{"x", "y", "z"},
			wantNames: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := scenario.SelectDeterministic(tt.scenarios, tt.names)

			if len(got) != len(tt.wantNames) {
				t.Fatalf("SelectDeterministic() returned %d scenarios, want %d", len(got), len(tt.wantNames))
			}
			for i, wantName := range tt.wantNames {
				if got[i].Name != wantName {
					t.Errorf("SelectDeterministic()[%d].Name = %q, want %q", i, got[i].Name, wantName)
				}
			}
		})
	}
}

func TestSelectDeterministic_DoesNotMutateInput(t *testing.T) {
	scenarios := sampleScenarios()
	originalLen := len(scenarios)
	originalFirst := scenarios[0].Name

	_ = scenario.SelectDeterministic(scenarios, []string{"bravo"})

	if len(scenarios) != originalLen {
		t.Errorf("input slice length changed: got %d, want %d", len(scenarios), originalLen)
	}
	if scenarios[0].Name != originalFirst {
		t.Errorf("input slice first element changed: got %q, want %q", scenarios[0].Name, originalFirst)
	}
}

func TestSelectProbabilistic_AlwaysIncluded(t *testing.T) {
	always := makeScenario("always", "data-arrival", types.SeverityLow, 1.0)
	scenarios := []scenario.Scenario{always}

	const iterations = 1000
	for i := 0; i < iterations; i++ {
		rng := rand.New(rand.NewSource(int64(i)))
		got := scenario.SelectProbabilistic(scenarios, rng)
		if len(got) != 1 {
			t.Fatalf("iteration %d: probability 1.0 scenario was not selected", i)
		}
	}
}

func TestSelectProbabilistic_NeverIncluded(t *testing.T) {
	never := makeScenario("never", "data-arrival", types.SeverityLow, 0.0)
	scenarios := []scenario.Scenario{never}

	const iterations = 1000
	for i := 0; i < iterations; i++ {
		rng := rand.New(rand.NewSource(int64(i)))
		got := scenario.SelectProbabilistic(scenarios, rng)
		if len(got) != 0 {
			t.Fatalf("iteration %d: probability 0.0 scenario was selected", i)
		}
	}
}

func TestSelectProbabilistic_StatisticalDistribution(t *testing.T) {
	half := makeScenario("half", "data-arrival", types.SeverityLow, 0.5)
	scenarios := []scenario.Scenario{half}

	const iterations = 10000
	selected := 0
	rng := rand.New(rand.NewSource(42))

	for i := 0; i < iterations; i++ {
		got := scenario.SelectProbabilistic(scenarios, rng)
		if len(got) == 1 {
			selected++
		}
	}

	ratio := float64(selected) / float64(iterations)
	if math.Abs(ratio-0.5) > 0.05 {
		t.Errorf("probability 0.5: selected ratio = %.4f, want ~0.5 (within ±0.05)", ratio)
	}
}

func TestSelectProbabilistic_DoesNotMutateInput(t *testing.T) {
	scenarios := sampleScenarios()
	originalLen := len(scenarios)

	rng := rand.New(rand.NewSource(99))
	_ = scenario.SelectProbabilistic(scenarios, rng)

	if len(scenarios) != originalLen {
		t.Errorf("input slice length changed: got %d, want %d", len(scenarios), originalLen)
	}
}

func TestSelectProbabilistic_EmptyInput(t *testing.T) {
	rng := rand.New(rand.NewSource(1))

	got := scenario.SelectProbabilistic(nil, rng)
	if len(got) != 0 {
		t.Errorf("nil scenarios: got %d, want 0", len(got))
	}

	got = scenario.SelectProbabilistic([]scenario.Scenario{}, rng)
	if len(got) != 0 {
		t.Errorf("empty scenarios: got %d, want 0", len(got))
	}
}

func TestFilterBySeverity(t *testing.T) {
	tests := []struct {
		name        string
		scenarios   []scenario.Scenario
		maxSeverity types.Severity
		wantNames   []string
	}{
		{
			name:        "filters out scenarios above threshold",
			scenarios:   sampleScenarios(),
			maxSeverity: types.SeverityModerate,
			wantNames:   []string{"alpha", "bravo"},
		},
		{
			name:        "keeps all when threshold is critical",
			scenarios:   sampleScenarios(),
			maxSeverity: types.SeverityCritical,
			wantNames:   []string{"alpha", "bravo", "charlie", "delta"},
		},
		{
			name:        "keeps only low when threshold is low",
			scenarios:   sampleScenarios(),
			maxSeverity: types.SeverityLow,
			wantNames:   []string{"alpha"},
		},
		{
			name:        "empty scenarios returns empty",
			scenarios:   []scenario.Scenario{},
			maxSeverity: types.SeverityCritical,
			wantNames:   []string{},
		},
		{
			name:        "nil scenarios returns empty",
			scenarios:   nil,
			maxSeverity: types.SeverityCritical,
			wantNames:   []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := scenario.FilterBySeverity(tt.scenarios, tt.maxSeverity)

			if len(got) != len(tt.wantNames) {
				t.Fatalf("FilterBySeverity() returned %d scenarios, want %d", len(got), len(tt.wantNames))
			}
			for i, wantName := range tt.wantNames {
				if got[i].Name != wantName {
					t.Errorf("FilterBySeverity()[%d].Name = %q, want %q", i, got[i].Name, wantName)
				}
			}
		})
	}
}

func TestFilterBySeverity_DoesNotMutateInput(t *testing.T) {
	scenarios := sampleScenarios()
	originalLen := len(scenarios)

	_ = scenario.FilterBySeverity(scenarios, types.SeverityLow)

	if len(scenarios) != originalLen {
		t.Errorf("input slice length changed: got %d, want %d", len(scenarios), originalLen)
	}
}

func TestFilterByCategory(t *testing.T) {
	tests := []struct {
		name      string
		scenarios []scenario.Scenario
		category  string
		wantNames []string
	}{
		{
			name:      "filters to matching category",
			scenarios: sampleScenarios(),
			category:  "data-arrival",
			wantNames: []string{"alpha", "charlie"},
		},
		{
			name:      "single match",
			scenarios: sampleScenarios(),
			category:  "infrastructure",
			wantNames: []string{"delta"},
		},
		{
			name:      "no matches returns empty",
			scenarios: sampleScenarios(),
			category:  "compound",
			wantNames: []string{},
		},
		{
			name:      "empty scenarios returns empty",
			scenarios: []scenario.Scenario{},
			category:  "data-arrival",
			wantNames: []string{},
		},
		{
			name:      "nil scenarios returns empty",
			scenarios: nil,
			category:  "data-arrival",
			wantNames: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := scenario.FilterByCategory(tt.scenarios, tt.category)

			if len(got) != len(tt.wantNames) {
				t.Fatalf("FilterByCategory() returned %d scenarios, want %d", len(got), len(tt.wantNames))
			}
			for i, wantName := range tt.wantNames {
				if got[i].Name != wantName {
					t.Errorf("FilterByCategory()[%d].Name = %q, want %q", i, got[i].Name, wantName)
				}
			}
		})
	}
}

func TestFilterByCategory_DoesNotMutateInput(t *testing.T) {
	scenarios := sampleScenarios()
	originalLen := len(scenarios)

	_ = scenario.FilterByCategory(scenarios, "data-arrival")

	if len(scenarios) != originalLen {
		t.Errorf("input slice length changed: got %d, want %d", len(scenarios), originalLen)
	}
}
