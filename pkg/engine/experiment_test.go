package engine_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/dwsmith1983/chaos-data/pkg/engine"
	"github.com/dwsmith1983/chaos-data/pkg/mutation"
	"github.com/dwsmith1983/chaos-data/pkg/scenario"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// --- Step 10.1: Experiment lifecycle tests ---

func TestStartExperiment_CreatesRunningExperiment(t *testing.T) {
	t.Parallel()

	transport := &mockTransport{
		listFn: func(_ context.Context, _ string) ([]types.DataObject, error) {
			return []types.DataObject{newTestObject("a.csv")}, nil
		},
	}
	emitter := &mockEmitter{}

	reg := mutation.NewRegistry()
	if err := reg.Register(&mutation.DelayMutation{}); err != nil {
		t.Fatalf("register: %v", err)
	}

	scenarios := []scenario.Scenario{newDelayScenario("test-delay", types.SeverityLow)}

	eng := engine.New(
		defaultConfig(),
		transport,
		reg,
		scenarios,
		engine.WithEmitter(emitter),
	)

	config := types.ExperimentConfig{
		Scenarios: []string{"test-delay"},
		Duration:  types.Duration{Duration: 5 * time.Second},
		Mode:      "deterministic",
	}

	exp, err := eng.StartExperiment(context.Background(), config)
	if err != nil {
		t.Fatalf("StartExperiment() error = %v", err)
	}
	defer exp.Stop()

	if exp.ID == "" {
		t.Error("experiment ID should not be empty")
	}
	if exp.State() != types.ExperimentRunning {
		t.Errorf("experiment state = %q, want %q", exp.State(), types.ExperimentRunning)
	}
	if exp.StartTime().IsZero() {
		t.Error("experiment StartTime should not be zero")
	}
}

func TestExperiment_CompletesAfterDuration(t *testing.T) {
	t.Parallel()

	transport := &mockTransport{
		listFn: func(_ context.Context, _ string) ([]types.DataObject, error) {
			return []types.DataObject{newTestObject("a.csv")}, nil
		},
	}
	emitter := &mockEmitter{}

	reg := mutation.NewRegistry()
	if err := reg.Register(&mutation.DelayMutation{}); err != nil {
		t.Fatalf("register: %v", err)
	}

	scenarios := []scenario.Scenario{newDelayScenario("test-delay", types.SeverityLow)}

	eng := engine.New(
		defaultConfig(),
		transport,
		reg,
		scenarios,
		engine.WithEmitter(emitter),
	)

	config := types.ExperimentConfig{
		Scenarios: []string{"test-delay"},
		Duration:  types.Duration{Duration: 50 * time.Millisecond},
		Mode:      "deterministic",
	}

	exp, err := eng.StartExperiment(context.Background(), config)
	if err != nil {
		t.Fatalf("StartExperiment() error = %v", err)
	}

	exp.Wait()

	if exp.State() != types.ExperimentCompleted {
		t.Errorf("experiment state = %q, want %q", exp.State(), types.ExperimentCompleted)
	}
	if exp.EndTime().IsZero() {
		t.Error("experiment EndTime should not be zero after completion")
	}
	if exp.Err() != nil {
		t.Errorf("Err() = %v, want nil after successful completion", exp.Err())
	}
}

func TestExperiment_StopAbortsExperiment(t *testing.T) {
	t.Parallel()

	transport := &mockTransport{
		listFn: func(ctx context.Context, _ string) ([]types.DataObject, error) {
			// Block until context is canceled, keeping the engine running.
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}

	reg := mutation.NewRegistry()
	if err := reg.Register(&mutation.DelayMutation{}); err != nil {
		t.Fatalf("register: %v", err)
	}

	scenarios := []scenario.Scenario{newDelayScenario("test-delay", types.SeverityLow)}

	eng := engine.New(
		defaultConfig(),
		transport,
		reg,
		scenarios,
	)

	config := types.ExperimentConfig{
		Scenarios: []string{"test-delay"},
		Duration:  types.Duration{Duration: 10 * time.Second},
		Mode:      "deterministic",
	}

	exp, err := eng.StartExperiment(context.Background(), config)
	if err != nil {
		t.Fatalf("StartExperiment() error = %v", err)
	}

	// Give the goroutine time to start.
	time.Sleep(20 * time.Millisecond)

	exp.Stop()
	exp.Wait()

	if exp.State() != types.ExperimentAborted {
		t.Errorf("experiment state = %q, want %q", exp.State(), types.ExperimentAborted)
	}
}

func TestExperiment_ManifestReturnsValidJSONL(t *testing.T) {
	t.Parallel()

	transport := &mockTransport{
		listFn: func(_ context.Context, _ string) ([]types.DataObject, error) {
			return []types.DataObject{
				newTestObject("a.csv"),
				newTestObject("b.csv"),
			}, nil
		},
	}
	emitter := &mockEmitter{}

	reg := mutation.NewRegistry()
	if err := reg.Register(&mutation.DelayMutation{}); err != nil {
		t.Fatalf("register: %v", err)
	}

	scenarios := []scenario.Scenario{newDelayScenario("test-delay", types.SeverityLow)}

	eng := engine.New(
		defaultConfig(),
		transport,
		reg,
		scenarios,
		engine.WithEmitter(emitter),
	)

	config := types.ExperimentConfig{
		Scenarios: []string{"test-delay"},
		Duration:  types.Duration{Duration: 100 * time.Millisecond},
		Mode:      "deterministic",
	}

	exp, err := eng.StartExperiment(context.Background(), config)
	if err != nil {
		t.Fatalf("StartExperiment() error = %v", err)
	}

	exp.Wait()

	manifest, err := exp.Manifest()
	if err != nil {
		t.Fatalf("Manifest() error = %v", err)
	}

	// Each line should be valid JSON representing a ChaosEvent.
	lines := strings.Split(strings.TrimSpace(string(manifest)), "\n")
	if len(lines) == 0 {
		t.Fatal("Manifest() returned no lines")
	}

	for i, line := range lines {
		var event types.ChaosEvent
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			t.Errorf("line %d: invalid JSON: %v\n  line: %s", i, err, line)
		}
		if event.Scenario == "" {
			t.Errorf("line %d: scenario should not be empty", i)
		}
		if event.Mutation == "" {
			t.Errorf("line %d: mutation should not be empty", i)
		}
	}
}

func TestExperiment_StatsReturnsCorrectCounts(t *testing.T) {
	t.Parallel()

	transport := &mockTransport{
		listFn: func(_ context.Context, _ string) ([]types.DataObject, error) {
			return []types.DataObject{
				newTestObject("a.csv"),
				newTestObject("b.csv"),
				newTestObject("c.csv"),
			}, nil
		},
	}
	emitter := &mockEmitter{}

	reg := mutation.NewRegistry()
	if err := reg.Register(&mutation.DelayMutation{}); err != nil {
		t.Fatalf("register: %v", err)
	}

	scenarios := []scenario.Scenario{newDelayScenario("test-delay", types.SeverityLow)}

	eng := engine.New(
		defaultConfig(),
		transport,
		reg,
		scenarios,
		engine.WithEmitter(emitter),
	)

	config := types.ExperimentConfig{
		Scenarios: []string{"test-delay"},
		Duration:  types.Duration{Duration: 100 * time.Millisecond},
		Mode:      "deterministic",
	}

	exp, err := eng.StartExperiment(context.Background(), config)
	if err != nil {
		t.Fatalf("StartExperiment() error = %v", err)
	}

	exp.Wait()

	stats := exp.Stats()

	if stats.ExperimentID != exp.ID {
		t.Errorf("stats ExperimentID = %q, want %q", stats.ExperimentID, exp.ID)
	}
	if stats.TotalEvents < 1 {
		t.Errorf("stats TotalEvents = %d, want >= 1", stats.TotalEvents)
	}
	if stats.AffectedTargets < 1 {
		t.Errorf("stats AffectedTargets = %d, want >= 1", stats.AffectedTargets)
	}
	if stats.AffectedPipelines < 1 {
		t.Errorf("stats AffectedPipelines = %d, want >= 1", stats.AffectedPipelines)
	}
	if stats.StartTime.IsZero() {
		t.Error("stats StartTime should not be zero")
	}
}

func TestExperiment_WaitBlocksUntilCompletion(t *testing.T) {
	t.Parallel()

	transport := &mockTransport{
		listFn: func(_ context.Context, _ string) ([]types.DataObject, error) {
			return []types.DataObject{newTestObject("a.csv")}, nil
		},
	}

	reg := mutation.NewRegistry()
	if err := reg.Register(&mutation.DelayMutation{}); err != nil {
		t.Fatalf("register: %v", err)
	}

	scenarios := []scenario.Scenario{newDelayScenario("test-delay", types.SeverityLow)}

	eng := engine.New(
		defaultConfig(),
		transport,
		reg,
		scenarios,
	)

	config := types.ExperimentConfig{
		Scenarios: []string{"test-delay"},
		Duration:  types.Duration{Duration: 50 * time.Millisecond},
		Mode:      "deterministic",
	}

	exp, err := eng.StartExperiment(context.Background(), config)
	if err != nil {
		t.Fatalf("StartExperiment() error = %v", err)
	}

	done := make(chan struct{})
	go func() {
		exp.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Wait returned -- expected.
	case <-time.After(5 * time.Second):
		t.Fatal("Wait() did not return within timeout")
	}

	state := exp.State()
	if state != types.ExperimentCompleted {
		t.Errorf("state after Wait = %q, want %q", state, types.ExperimentCompleted)
	}
	if exp.Err() != nil {
		t.Errorf("Err() = %v, want nil after successful completion", exp.Err())
	}
}

func TestStartExperiment_InvalidConfigReturnsError(t *testing.T) {
	t.Parallel()

	transport := &mockTransport{}
	reg := mutation.NewRegistry()
	scenarios := []scenario.Scenario{newDelayScenario("test-delay", types.SeverityLow)}

	eng := engine.New(
		defaultConfig(),
		transport,
		reg,
		scenarios,
	)

	config := types.ExperimentConfig{
		Scenarios: []string{"test-delay"},
		Duration:  types.Duration{Duration: 5 * time.Second},
		Mode:      "invalid-mode",
	}

	exp, err := eng.StartExperiment(context.Background(), config)
	if err == nil {
		t.Fatal("StartExperiment() error = nil, want error for invalid config")
	}
	if exp != nil {
		t.Errorf("StartExperiment() returned non-nil experiment on error")
	}
}
