package engine_test

import (
	"context"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/engine"
	"github.com/dwsmith1983/chaos-data/pkg/mutation"
	"github.com/dwsmith1983/chaos-data/pkg/scenario"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// --- Step 14.1: Probabilistic engine loop tests ---

func TestRunProbabilistic_AppliesMutationsBasedOnProbability(t *testing.T) {
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

	// Scenario with probability 1.0 should always be selected.
	sc := newDelayScenario("always-delay", types.SeverityLow)
	sc.Probability = 1.0

	eng := engine.New(
		defaultConfig(),
		transport,
		reg,
		[]scenario.Scenario{sc},
		engine.WithEmitter(emitter),
	)

	// Use a deterministic seed for reproducibility.
	rng := rand.New(rand.NewSource(42)) //nolint:gosec

	// Run with a short duration so only a few iterations happen.
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	records, err := eng.RunProbabilistic(ctx, 50*time.Millisecond, rng)
	if err != nil {
		t.Fatalf("RunProbabilistic() error = %v", err)
	}

	// With probability 1.0, every tick should produce records for all objects.
	// We expect at least 2 records (1 iteration x 2 objects minimum).
	if len(records) < 2 {
		t.Errorf("expected at least 2 records, got %d", len(records))
	}

	// All records should be for delay mutations.
	for i, rec := range records {
		if rec.Mutation != "delay" {
			t.Errorf("records[%d].Mutation = %q, want %q", i, rec.Mutation, "delay")
		}
		if !rec.Applied {
			t.Errorf("records[%d].Applied = false, want true", i)
		}
	}
}

func TestRunProbabilistic_ContextCancellationStopsLoop(t *testing.T) {
	t.Parallel()

	var listCalls atomic.Int32

	transport := &mockTransport{
		ListFn: func(_ context.Context, _ string) ([]types.DataObject, error) {
			listCalls.Add(1)
			return []types.DataObject{newTestObject("a.csv")}, nil
		},
	}

	reg := mutation.NewRegistry()
	if err := reg.Register(&mutation.DelayMutation{}); err != nil {
		t.Fatalf("register: %v", err)
	}

	sc := newDelayScenario("always-delay", types.SeverityLow)
	sc.Probability = 1.0

	eng := engine.New(
		defaultConfig(),
		transport,
		reg,
		[]scenario.Scenario{sc},
	)

	rng := rand.New(rand.NewSource(42)) //nolint:gosec

	ctx, cancel := context.WithCancel(context.Background())

	// Cancel after a short delay so the loop runs at most a couple of iterations.
	go func() {
		time.Sleep(80 * time.Millisecond)
		cancel()
	}()

	records, err := eng.RunProbabilistic(ctx, 30*time.Millisecond, rng)
	if err != nil {
		t.Fatalf("RunProbabilistic() error = %v", err)
	}

	// Should have gotten some records but the loop should have stopped.
	calls := listCalls.Load()
	if calls > 5 {
		t.Errorf("expected loop to stop, but listFn was called %d times", calls)
	}

	// Should have at least 1 record from the first iteration.
	if len(records) < 1 {
		t.Errorf("expected at least 1 record, got %d", len(records))
	}
	_ = records
}

func TestRunProbabilistic_ZeroProbabilityNeverSelected(t *testing.T) {
	t.Parallel()

	transport := &mockTransport{
		ListFn: func(_ context.Context, _ string) ([]types.DataObject, error) {
			return []types.DataObject{
				newTestObject("a.csv"),
			}, nil
		},
	}
	emitter := &mockEmitter{}

	reg := mutation.NewRegistry()
	if err := reg.Register(&mutation.DelayMutation{}); err != nil {
		t.Fatalf("register: %v", err)
	}

	// Scenario with probability 0.0 should never be selected.
	sc := newDelayScenario("never-delay", types.SeverityLow)
	sc.Probability = 0.0

	eng := engine.New(
		defaultConfig(),
		transport,
		reg,
		[]scenario.Scenario{sc},
		engine.WithEmitter(emitter),
	)

	rng := rand.New(rand.NewSource(42)) //nolint:gosec

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	records, err := eng.RunProbabilistic(ctx, 50*time.Millisecond, rng)
	if err != nil {
		t.Fatalf("RunProbabilistic() error = %v", err)
	}

	if len(records) != 0 {
		t.Errorf("expected 0 records for zero-probability scenario, got %d", len(records))
	}

	events := emitter.GetEvents()
	if len(events) != 0 {
		t.Errorf("expected 0 events, got %d", len(events))
	}
}

func TestRunProbabilistic_MultipleIterationsAccumulateRecords(t *testing.T) {
	t.Parallel()

	transport := &mockTransport{
		ListFn: func(_ context.Context, _ string) ([]types.DataObject, error) {
			return []types.DataObject{
				newTestObject("a.csv"),
			}, nil
		},
	}
	emitter := &mockEmitter{}

	reg := mutation.NewRegistry()
	if err := reg.Register(&mutation.DelayMutation{}); err != nil {
		t.Fatalf("register: %v", err)
	}

	sc := newDelayScenario("always-delay", types.SeverityLow)
	sc.Probability = 1.0

	eng := engine.New(
		defaultConfig(),
		transport,
		reg,
		[]scenario.Scenario{sc},
		engine.WithEmitter(emitter),
	)

	rng := rand.New(rand.NewSource(42)) //nolint:gosec

	// Run for 250ms with 50ms interval: should get ~4-5 iterations.
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()

	records, err := eng.RunProbabilistic(ctx, 50*time.Millisecond, rng)
	if err != nil {
		t.Fatalf("RunProbabilistic() error = %v", err)
	}

	// With 1 object and probability 1.0, we should get at least 3 records
	// from multiple iterations (allowing for timing variance).
	if len(records) < 3 {
		t.Errorf("expected at least 3 accumulated records, got %d", len(records))
	}

	events := emitter.GetEvents()
	if len(events) != len(records) {
		t.Errorf("events count %d != records count %d", len(events), len(records))
	}
}

func TestProbabilisticIteration_SkipsOnCooldown(t *testing.T) {
	t.Parallel()

	transport := &mockTransport{
		ListFn: func(_ context.Context, _ string) ([]types.DataObject, error) {
			return []types.DataObject{
				newTestObject("a.csv"),
			}, nil
		},
	}
	emitter := &mockEmitter{}
	safety := &mockSafety{
		Enabled:     true,
		MaxSev:      types.SeverityCritical,
		CooldownErr: adapter.ErrCooldownActive,
	}

	reg := mutation.NewRegistry()
	if err := reg.Register(&mutation.DelayMutation{}); err != nil {
		t.Fatalf("register: %v", err)
	}

	sc := newDelayScenario("always-delay", types.SeverityLow)
	sc.Probability = 1.0

	eng := engine.New(
		defaultConfig(),
		transport,
		reg,
		[]scenario.Scenario{sc},
		engine.WithEmitter(emitter),
		engine.WithSafety(safety),
	)

	rng := rand.New(rand.NewSource(42)) //nolint:gosec

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	records, err := eng.RunProbabilistic(ctx, 50*time.Millisecond, rng)
	if err != nil {
		t.Fatalf("RunProbabilistic() error = %v", err)
	}

	// All scenarios should be skipped due to cooldown.
	if len(records) != 0 {
		t.Errorf("expected 0 records (cooldown active), got %d", len(records))
	}

	events := emitter.GetEvents()
	if len(events) != 0 {
		t.Errorf("expected 0 events, got %d", len(events))
	}
}
