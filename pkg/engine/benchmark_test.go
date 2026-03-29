package engine_test

import (
	"context"
	"testing"
	"time"

	"github.com/dwsmith1983/chaos-data/pkg/engine"
	"github.com/dwsmith1983/chaos-data/pkg/mutation"
	"github.com/dwsmith1983/chaos-data/pkg/scenario"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// BenchmarkProcessObject measures the core ProcessObject pipeline with a delay
// mutation and mock transport. The benchmark exercises scenario filtering,
// severity checking, mutation lookup, and mutation application.
func BenchmarkProcessObject(b *testing.B) {
	transport := &mockTransport{}
	emitter := &mockEmitter{}
	safety := &mockSafety{
		Enabled:    true,
		MaxSev:     types.SeverityCritical,
		SLAAllowed: true,
	}

	reg := mutation.NewRegistry()
	if err := reg.Register(&mutation.DelayMutation{}); err != nil {
		b.Fatalf("register delay: %v", err)
	}

	scenarios := []scenario.Scenario{
		newDelayScenario("bench-delay", types.SeverityLow),
	}

	eng := engine.New(
		defaultConfig(),
		transport,
		reg,
		scenarios,
		engine.WithEmitter(emitter),
		engine.WithSafety(safety),
	)

	obj := newTestObject("data/bench.csv")
	ctx := context.Background()

	b.ResetTimer()
	for range b.N {
		_, err := eng.ProcessObject(ctx, obj)
		if err != nil {
			b.Fatalf("ProcessObject: %v", err)
		}
	}
}

// BenchmarkEvaluateAssertions measures assertion evaluation with a single
// data_state assertion backed by a mock transport that immediately satisfies
// the condition.
func BenchmarkEvaluateAssertions(b *testing.B) {
	transport := &mockTransport{
		ListHeldFn: func(_ context.Context) ([]types.HeldObject, error) {
			return []types.HeldObject{
				{
					DataObject: types.DataObject{Key: "data/bench.csv"},
					HeldUntil:  time.Now().Add(time.Hour),
				},
			}, nil
		},
	}

	cfg := defaultConfig()
	cfg.AssertWait = true
	cfg.AssertPollInterval = types.Duration{Duration: time.Millisecond}

	reg := mutation.NewRegistry()
	scenarios := []scenario.Scenario{
		{
			Name:        "bench-assert",
			Description: "benchmark assertion scenario",
			Category:    "data-arrival",
			Severity:    types.SeverityLow,
			Version:     1,
			Target: scenario.TargetSpec{
				Layer: "data",
			},
			Mutation: scenario.MutationSpec{
				Type: "delay",
			},
			Probability: 1.0,
			Expected: &scenario.ExpectedResponse{
				Within: types.Duration{Duration: time.Second},
				Asserts: []types.Assertion{
					{
						Type:      types.AssertDataState,
						Target:    "data/bench.csv",
						Condition: types.CondIsHeld,
					},
				},
			},
		},
	}

	eng := engine.New(cfg, transport, reg, scenarios)
	ctx := context.Background()

	b.ResetTimer()
	for range b.N {
		results := eng.EvaluateAssertions(ctx, scenarios)
		if len(results) == 0 {
			b.Fatal("expected at least one assertion result")
		}
	}
}
