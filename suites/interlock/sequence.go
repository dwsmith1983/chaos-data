package interlocksuite

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/engine"
	"github.com/dwsmith1983/chaos-data/pkg/types"
	"gopkg.in/yaml.v3"
)

const defaultAssertWithin = 30 * time.Second

// Sequence defines a multi-step chaos test scenario.
type Sequence struct {
	Name         string         `yaml:"name"`
	Description  string         `yaml:"description"`
	Capabilities []string       `yaml:"capabilities"`
	Steps        []SequenceStep `yaml:"steps"`
}

// SequenceStep is a single step in a sequence.
type SequenceStep struct {
	Name              string            `yaml:"name,omitempty"`
	Setup             *SetupSpec        `yaml:"setup,omitempty"`
	Scenario          string            `yaml:"scenario,omitempty"`
	Wait              string            `yaml:"wait,omitempty"`
	AssertWithin      string            `yaml:"assert_within,omitempty"`
	ContinueOnFailure bool              `yaml:"continue_on_failure,omitempty"`
	Assert            []types.Assertion `yaml:"assert,omitempty"`
}

// StepResult holds the outcome of a single sequence step.
type StepResult struct {
	Name     string
	Passed   bool
	Skipped  bool
	Error    string
	Duration time.Duration
}

// SequenceResult holds the outcome of running a sequence.
type SequenceResult struct {
	Sequence     string
	Capabilities []string
	Steps        []StepResult
	Passed       bool
	Duration     time.Duration
}

// LoadSequence loads a sequence from a YAML file.
func LoadSequence(path string) (Sequence, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Sequence{}, fmt.Errorf("load sequence %q: %w", path, err)
	}
	var seq Sequence
	if err := yaml.Unmarshal(data, &seq); err != nil {
		return Sequence{}, fmt.Errorf("parse sequence %q: %w", path, err)
	}
	return seq, nil
}

// RunSequence executes a multi-step sequence.
// Fail-fast by default: if a step fails, subsequent steps are SKIPPED.
// Wait steps respect the clock (TestClock.Advance for testing).
func (r *SuiteRunner) RunSequence(ctx context.Context, seq Sequence) SequenceResult {
	start := r.clock.Now()

	result := SequenceResult{
		Sequence:     seq.Name,
		Capabilities: seq.Capabilities,
		Steps:        make([]StepResult, 0, len(seq.Steps)),
		Passed:       true,
	}

	// Create a single harness for the entire sequence so state persists across steps.
	r.mu.Lock()
	r.runCounter++
	runID := fmt.Sprintf("%03d", r.runCounter)
	r.mu.Unlock()

	h := NewHarness(r.store, r.clock, runID)
	defer func() { _ = h.Teardown(ctx) }()

	failed := false
	for i, step := range seq.Steps {
		if err := ctx.Err(); err != nil {
			// Context cancelled: mark remaining steps as skipped.
			for j := i; j < len(seq.Steps); j++ {
				result.Steps = append(result.Steps, StepResult{
					Name:    seq.Steps[j].Name,
					Skipped: true,
				})
			}
			result.Passed = false
			break
		}

		if failed {
			result.Steps = append(result.Steps, StepResult{
				Name:    step.Name,
				Skipped: true,
			})
			continue
		}

		sr := r.runSequenceStep(ctx, h, step)
		result.Steps = append(result.Steps, sr)

		if !sr.Passed && !sr.Skipped {
			result.Passed = false
			if !step.ContinueOnFailure {
				failed = true
			}
		}
	}

	result.Duration = r.clock.Now().Sub(start)
	return result
}

// runSequenceStep executes one step within a sequence.
func (r *SuiteRunner) runSequenceStep(ctx context.Context, h *Harness, step SequenceStep) StepResult {
	stepStart := r.clock.Now()

	sr := StepResult{
		Name:   step.Name,
		Passed: true,
	}

	// Setup: write prerequisite state.
	if step.Setup != nil {
		if err := h.Setup(ctx, *step.Setup); err != nil {
			sr.Passed = false
			sr.Error = fmt.Sprintf("setup failed: %v", err)
			sr.Duration = r.clock.Now().Sub(stepStart)
			return sr
		}
	}

	// Scenario: load and run via RunScenario.
	if step.Scenario != "" {
		scenarioPath := step.Scenario
		// Resolve relative paths against the suite directory.
		if !filepath.IsAbs(scenarioPath) {
			scenarioPath = filepath.Join("suites", "interlock", scenarioPath)
		}

		ss, err := LoadSuiteScenario(scenarioPath)
		if err != nil {
			sr.Passed = false
			sr.Error = fmt.Sprintf("load scenario: %v", err)
			sr.Duration = r.clock.Now().Sub(stepStart)
			return sr
		}

		scenarioResult := r.RunScenarioInHarness(ctx, ss, h)
		if !scenarioResult.Passed {
			sr.Passed = false
			sr.Error = scenarioResult.Error
			sr.Duration = r.clock.Now().Sub(stepStart)
			return sr
		}
	}

	// Wait: parse duration and advance clock or sleep.
	if step.Wait != "" {
		dur, err := time.ParseDuration(step.Wait)
		if err != nil {
			sr.Passed = false
			sr.Error = fmt.Sprintf("parse wait duration %q: %v", step.Wait, err)
			sr.Duration = r.clock.Now().Sub(stepStart)
			return sr
		}

		if tc, ok := r.clock.(*adapter.TestClock); ok {
			tc.Advance(dur)
		} else {
			time.Sleep(dur)
		}
	}

	// Assert: evaluate assertions via engine.EvaluateAssertionSet when a
	// CompositeAsserter is available; fall back to the legacy hardcoded
	// exists/not_exists logic for backward compatibility with tests that
	// don't configure an asserter.
	if len(step.Assert) > 0 {
		if r.asserter != nil {
			within := defaultAssertWithin
			if step.AssertWithin != "" {
				parsed, err := time.ParseDuration(step.AssertWithin)
				if err != nil {
					sr.Passed = false
					sr.Error = fmt.Sprintf("parse assert_within %q: %v", step.AssertWithin, err)
					sr.Duration = r.clock.Now().Sub(stepStart)
					return sr
				}
				within = parsed
			}

			const pollInterval = 100 * time.Millisecond
			results := engine.EvaluateAssertionSet(
				ctx, step.Assert, within, r.asserter, r.clock, nil, pollInterval,
			)
			for _, ar := range results {
				if ar.Error != "" {
					sr.Passed = false
					sr.Error = fmt.Sprintf("assertion error: %s", ar.Error)
					sr.Duration = r.clock.Now().Sub(stepStart)
					return sr
				}
				if !ar.Satisfied {
					sr.Passed = false
					sr.Error = fmt.Sprintf(
						"assertion failed: %s %s on %q not satisfied",
						ar.Assertion.Type, ar.Assertion.Condition, ar.Assertion.Target,
					)
					sr.Duration = r.clock.Now().Sub(stepStart)
					return sr
				}
			}
		} else {
			// Legacy fallback: only exists/not_exists via eventReader.
			for _, a := range step.Assert {
				if err := a.Validate(); err != nil {
					sr.Passed = false
					sr.Error = fmt.Sprintf("assertion validation: %v", err)
					sr.Duration = r.clock.Now().Sub(stepStart)
					return sr
				}
				events, err := r.eventReader.ReadEvents(ctx, "", string(a.Target))
				if err != nil {
					sr.Passed = false
					sr.Error = fmt.Sprintf("read events for assertion: %v", err)
					sr.Duration = r.clock.Now().Sub(stepStart)
					return sr
				}

				switch a.Condition {
				case types.CondExists:
					if len(events) == 0 {
						sr.Passed = false
						sr.Error = fmt.Sprintf("assertion failed: expected %q event to exist, found none", a.Target)
					}
				case types.CondNotExists:
					if len(events) > 0 {
						sr.Passed = false
						sr.Error = fmt.Sprintf("assertion failed: expected %q event not to exist, found %d", a.Target, len(events))
					}
				default:
					sr.Passed = false
					sr.Error = fmt.Sprintf("unsupported assertion condition %q for sequence step", a.Condition)
				}

				if !sr.Passed {
					sr.Duration = r.clock.Now().Sub(stepStart)
					return sr
				}
			}
		}
	}

	sr.Duration = r.clock.Now().Sub(stepStart)
	return sr
}
