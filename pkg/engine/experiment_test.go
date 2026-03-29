package engine_test

import (
	"context"
	"encoding/json"
	"errors"
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
		ListFn: func(_ context.Context, _ string) ([]types.DataObject, error) {
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
		ListFn: func(_ context.Context, _ string) ([]types.DataObject, error) {
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
		ListFn: func(ctx context.Context, _ string) ([]types.DataObject, error) {
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
		ListFn: func(_ context.Context, _ string) ([]types.DataObject, error) {
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
		ListFn: func(_ context.Context, _ string) ([]types.DataObject, error) {
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
		ListFn: func(_ context.Context, _ string) ([]types.DataObject, error) {
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

// --- Phase 3: Blast radius computation tests ---

func TestExperiment_BlastRadius_WithResolver(t *testing.T) {
	t.Parallel()

	transport := &mockTransport{
		ListFn: func(_ context.Context, _ string) ([]types.DataObject, error) {
			return []types.DataObject{
				newTestObject("events-001.jsonl"),
				newTestObject("events-002.jsonl"),
			}, nil
		},
	}

	resolver := &mockResolver{
		downstreamFn: func(_ context.Context, target string) ([]string, error) {
			return []string{"analytics.user_events", "reporting.daily_summary"}, nil
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
		engine.WithDependencyResolver(resolver),
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

	entries := exp.BlastRadius(context.Background())

	// Two objects were processed, each producing one applied record.
	if len(entries) != 2 {
		t.Fatalf("BlastRadius() len = %d, want 2; entries=%v", len(entries), entries)
	}

	for i, e := range entries {
		if e.MutatedObject == "" {
			t.Errorf("entry[%d].MutatedObject is empty", i)
		}
		if e.MutationType == "" {
			t.Errorf("entry[%d].MutationType is empty", i)
		}
		if len(e.Downstream) != 2 {
			t.Errorf("entry[%d].Downstream len = %d, want 2; got %v", i, len(e.Downstream), e.Downstream)
		}
	}
}

func TestExperiment_BlastRadius_NilResolver(t *testing.T) {
	t.Parallel()

	transport := &mockTransport{
		ListFn: func(_ context.Context, _ string) ([]types.DataObject, error) {
			return []types.DataObject{newTestObject("events-001.jsonl")}, nil
		},
	}

	reg := mutation.NewRegistry()
	if err := reg.Register(&mutation.DelayMutation{}); err != nil {
		t.Fatalf("register: %v", err)
	}

	scenarios := []scenario.Scenario{newDelayScenario("test-delay", types.SeverityLow)}

	// No WithDependencyResolver option — resolver stays nil.
	eng := engine.New(
		defaultConfig(),
		transport,
		reg,
		scenarios,
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

	entries := exp.BlastRadius(context.Background())
	if entries != nil {
		t.Errorf("BlastRadius() = %v, want nil when no resolver configured", entries)
	}
}

func TestExperiment_BlastRadius_OnlyAppliedRecords(t *testing.T) {
	t.Parallel()

	// Two objects: only one is in "dry-run" so Applied=false; resolver must only
	// be called for Applied=true records.
	transport := &mockTransport{
		ListFn: func(_ context.Context, _ string) ([]types.DataObject, error) {
			return []types.DataObject{
				newTestObject("events-001.jsonl"),
			}, nil
		},
	}

	resolver := &mockResolver{
		downstreamFn: func(_ context.Context, target string) ([]string, error) {
			return []string{"analytics.user_events"}, nil
		},
	}

	reg := mutation.NewRegistry()
	if err := reg.Register(&mutation.DelayMutation{}); err != nil {
		t.Fatalf("register: %v", err)
	}

	scenarios := []scenario.Scenario{newDelayScenario("test-delay", types.SeverityLow)}

	cfg := defaultConfig()
	cfg.DryRun = true // mutations are not applied

	eng := engine.New(
		cfg,
		transport,
		reg,
		scenarios,
		engine.WithDependencyResolver(resolver),
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

	entries := exp.BlastRadius(context.Background())
	// Dry-run means Applied=false on all records, so no downstream lookups.
	if len(entries) != 0 {
		t.Errorf("BlastRadius() len = %d, want 0 for dry-run (no applied records); got %v", len(entries), entries)
	}

	// Resolver must not have been called.
	calls := resolver.getCalls()
	if len(calls) != 0 {
		t.Errorf("resolver GetDownstream called %d times, want 0 for dry-run", len(calls))
	}
}

func TestExperiment_BlastRadius_ResolverErrorSkipped(t *testing.T) {
	t.Parallel()

	// Two objects processed; resolver returns error for the first, success for
	// the second. BlastRadius must return 1 entry (the successful one).
	transport := &mockTransport{
		ListFn: func(_ context.Context, _ string) ([]types.DataObject, error) {
			return []types.DataObject{
				newTestObject("events-001.jsonl"),
				newTestObject("events-002.jsonl"),
			}, nil
		},
	}

	resolver := &mockResolver{
		downstreamFn: func(_ context.Context, target string) ([]string, error) {
			if target == "events-001.jsonl" {
				return nil, errors.New("resolver unavailable for first record")
			}
			return []string{"analytics.user_events"}, nil
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
		engine.WithDependencyResolver(resolver),
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

	entries := exp.BlastRadius(context.Background())

	// Only the second record should produce an entry (first had resolver error).
	if len(entries) != 1 {
		t.Fatalf("BlastRadius() len = %d, want 1; entries=%v", len(entries), entries)
	}

	if entries[0].MutatedObject != "events-002.jsonl" {
		t.Errorf("entry[0].MutatedObject = %q, want %q", entries[0].MutatedObject, "events-002.jsonl")
	}
	if len(entries[0].Downstream) != 1 {
		t.Errorf("entry[0].Downstream len = %d, want 1; got %v", len(entries[0].Downstream), entries[0].Downstream)
	}
}
