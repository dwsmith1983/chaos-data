package engine_test

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/engine"
	"github.com/dwsmith1983/chaos-data/pkg/mutation"
	"github.com/dwsmith1983/chaos-data/pkg/scenario"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// --- helpers ---

// newDelayScenario creates a scenario targeting all objects with the delay mutation.
func newDelayScenario(name string, severity types.Severity) scenario.Scenario {
	return scenario.Scenario{
		Name:        name,
		Description: "test delay scenario",
		Category:    "data-arrival",
		Severity:    severity,
		Version:     1,
		Target: scenario.TargetSpec{
			Layer: "data",
			Filter: scenario.FilterSpec{
				Prefix: "",
				Match:  "",
			},
		},
		Mutation: scenario.MutationSpec{
			Type:   "delay",
			Params: map[string]string{"duration": "10m", "release": "true"},
		},
		Probability: 1.0,
		Safety: scenario.ScenarioSafety{
			MaxAffectedPct: 100,
		},
	}
}

// newPrefixScenario creates a scenario that only matches objects with a specific prefix.
func newPrefixScenario(name, prefix string) scenario.Scenario {
	s := newDelayScenario(name, types.SeverityLow)
	s.Target.Filter.Prefix = prefix
	return s
}

// newTestObject creates a DataObject for testing.
func newTestObject(key string) types.DataObject {
	return types.DataObject{
		Key:          key,
		Size:         100,
		LastModified: time.Now(),
	}
}

func defaultConfig() types.EngineConfig {
	return types.EngineConfig{
		Mode: "deterministic",
		Safety: types.SafetyConfig{
			MaxSeverity:    types.SeverityModerate,
			MaxAffectedPct: 100,
			MaxPipelines:   10,
		},
	}
}

// --- Step 7.1: ProcessObject tests ---

func TestProcessObject_PassthroughWhenKillSwitchDisabled(t *testing.T) {
	t.Parallel()

	transport := &mockTransport{}
	emitter := &mockEmitter{}
	safety := &mockSafety{Enabled: false, MaxSev: types.SeverityCritical}

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
		engine.WithSafety(safety),
	)

	records, err := eng.ProcessObject(context.Background(), newTestObject("data.csv"))
	if err != nil {
		t.Fatalf("ProcessObject() error = %v", err)
	}
	if len(records) != 0 {
		t.Errorf("expected 0 records (passthrough), got %d", len(records))
	}

	events := emitter.GetEvents()
	if len(events) != 0 {
		t.Errorf("expected 0 events, got %d", len(events))
	}
}

func TestProcessObject_PassthroughWhenNoScenariosMatch(t *testing.T) {
	t.Parallel()

	transport := &mockTransport{}
	emitter := &mockEmitter{}

	reg := mutation.NewRegistry()
	if err := reg.Register(&mutation.DelayMutation{}); err != nil {
		t.Fatalf("register: %v", err)
	}

	// Scenario only matches "logs/" prefix, but object is "data.csv".
	scenarios := []scenario.Scenario{newPrefixScenario("log-delay", "logs/")}

	eng := engine.New(
		defaultConfig(),
		transport,
		reg,
		scenarios,
		engine.WithEmitter(emitter),
	)

	records, err := eng.ProcessObject(context.Background(), newTestObject("data.csv"))
	if err != nil {
		t.Fatalf("ProcessObject() error = %v", err)
	}
	if len(records) != 0 {
		t.Errorf("expected 0 records, got %d", len(records))
	}
}

func TestProcessObject_AppliesMutationWhenScenarioMatches(t *testing.T) {
	t.Parallel()

	transport := &mockTransport{}

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

	obj := newTestObject("data.csv")
	records, err := eng.ProcessObject(context.Background(), obj)
	if err != nil {
		t.Fatalf("ProcessObject() error = %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}

	rec := records[0]
	if rec.ObjectKey != "data.csv" {
		t.Errorf("record ObjectKey = %q, want %q", rec.ObjectKey, "data.csv")
	}
	if rec.Mutation != "delay" {
		t.Errorf("record Mutation = %q, want %q", rec.Mutation, "delay")
	}
	if !rec.Applied {
		t.Error("record Applied = false, want true")
	}

	// Verify transport.Hold was called.
	if calls := len(transport.HoldCalls()); calls != 1 {
		t.Errorf("transport.Hold called %d times, want 1", calls)
	}
}

func TestProcessObject_EmitsChaosEvent(t *testing.T) {
	t.Parallel()

	transport := &mockTransport{}
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

	_, err := eng.ProcessObject(context.Background(), newTestObject("data.csv"))
	if err != nil {
		t.Fatalf("ProcessObject() error = %v", err)
	}

	events := emitter.GetEvents()
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}

	ev := events[0]
	if ev.Scenario != "test-delay" {
		t.Errorf("event Scenario = %q, want %q", ev.Scenario, "test-delay")
	}
	if ev.Mutation != "delay" {
		t.Errorf("event Mutation = %q, want %q", ev.Mutation, "delay")
	}
	if ev.Target != "data.csv" {
		t.Errorf("event Target = %q, want %q", ev.Target, "data.csv")
	}
	if ev.Severity != types.SeverityLow {
		t.Errorf("event Severity = %v, want %v", ev.Severity, types.SeverityLow)
	}
}

func TestProcessObject_SkipsScenarioWhenSeverityTooHigh(t *testing.T) {
	t.Parallel()

	transport := &mockTransport{}
	emitter := &mockEmitter{}
	safety := &mockSafety{
		Enabled: true,
		MaxSev:  types.SeverityLow, // Only allow low severity.
	}

	reg := mutation.NewRegistry()
	if err := reg.Register(&mutation.DelayMutation{}); err != nil {
		t.Fatalf("register: %v", err)
	}

	// Scenario has moderate severity, which exceeds the low max.
	scenarios := []scenario.Scenario{newDelayScenario("severe-delay", types.SeverityModerate)}

	eng := engine.New(
		defaultConfig(),
		transport,
		reg,
		scenarios,
		engine.WithEmitter(emitter),
		engine.WithSafety(safety),
	)

	records, err := eng.ProcessObject(context.Background(), newTestObject("data.csv"))
	if err != nil {
		t.Fatalf("ProcessObject() error = %v", err)
	}
	if len(records) != 0 {
		t.Errorf("expected 0 records (skipped), got %d", len(records))
	}

	events := emitter.GetEvents()
	if len(events) != 0 {
		t.Errorf("expected 0 events, got %d", len(events))
	}
}

func TestProcessObject_MultipleScenariosAllApplied(t *testing.T) {
	t.Parallel()

	transport := &mockTransport{}
	emitter := &mockEmitter{}

	reg := mutation.NewRegistry()
	if err := reg.Register(&mutation.DelayMutation{}); err != nil {
		t.Fatalf("register delay: %v", err)
	}
	if err := reg.Register(&mutation.DropMutation{}); err != nil {
		t.Fatalf("register drop: %v", err)
	}

	s1 := newDelayScenario("delay-scenario", types.SeverityLow)
	s2 := scenario.Scenario{
		Name:        "drop-scenario",
		Description: "test drop scenario",
		Category:    "data-arrival",
		Severity:    types.SeverityLow,
		Version:     1,
		Target: scenario.TargetSpec{
			Layer:  "data",
			Filter: scenario.FilterSpec{},
		},
		Mutation: scenario.MutationSpec{
			Type:   "drop",
			Params: map[string]string{"scope": "object"},
		},
		Probability: 1.0,
		Safety:      scenario.ScenarioSafety{MaxAffectedPct: 100},
	}

	eng := engine.New(
		defaultConfig(),
		transport,
		reg,
		[]scenario.Scenario{s1, s2},
		engine.WithEmitter(emitter),
	)

	records, err := eng.ProcessObject(context.Background(), newTestObject("data.csv"))
	if err != nil {
		t.Fatalf("ProcessObject() error = %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(records))
	}

	// Verify both mutation types were recorded.
	mutTypes := map[string]bool{}
	for _, r := range records {
		mutTypes[r.Mutation] = true
	}
	if !mutTypes["delay"] {
		t.Error("missing delay mutation record")
	}
	if !mutTypes["drop"] {
		t.Error("missing drop mutation record")
	}

	events := emitter.GetEvents()
	if len(events) != 2 {
		t.Errorf("expected 2 events, got %d", len(events))
	}
}

func TestProcessObject_MutationNotFoundReturnsError(t *testing.T) {
	t.Parallel()

	transport := &mockTransport{}

	// Empty registry -- "delay" mutation is NOT registered.
	reg := mutation.NewRegistry()

	scenarios := []scenario.Scenario{newDelayScenario("test-delay", types.SeverityLow)}

	eng := engine.New(
		defaultConfig(),
		transport,
		reg,
		scenarios,
	)

	_, err := eng.ProcessObject(context.Background(), newTestObject("data.csv"))
	if err == nil {
		t.Fatal("ProcessObject() error = nil, want error for missing mutation")
	}
	if !errors.Is(err, mutation.ErrMutationNotFound) {
		t.Errorf("error = %v, want %v", err, mutation.ErrMutationNotFound)
	}
}

func TestProcessObject_IsEnabledErrorReturnsError(t *testing.T) {
	t.Parallel()

	transport := &mockTransport{}
	safety := &mockSafety{
		Enabled:    false,
		EnabledErr: errors.New("safety controller unavailable"),
		MaxSev:     types.SeverityCritical,
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
		engine.WithSafety(safety),
	)

	_, err := eng.ProcessObject(context.Background(), newTestObject("data.csv"))
	if err == nil {
		t.Fatal("ProcessObject() error = nil, want error when IsEnabled fails")
	}
	if !errors.Is(err, safety.EnabledErr) {
		t.Errorf("error = %v, want wrapped %v", err, safety.EnabledErr)
	}
}

func TestProcessObject_MaxSeverityErrorReturnsError(t *testing.T) {
	t.Parallel()

	transport := &mockTransport{}
	safety := &mockSafety{
		Enabled:   true,
		MaxSev:    types.SeverityCritical,
		MaxSevErr: errors.New("max severity lookup failed"),
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
		engine.WithSafety(safety),
	)

	_, err := eng.ProcessObject(context.Background(), newTestObject("data.csv"))
	if err == nil {
		t.Fatal("ProcessObject() error = nil, want error when MaxSeverity fails")
	}
	if !errors.Is(err, safety.MaxSevErr) {
		t.Errorf("error = %v, want wrapped %v", err, safety.MaxSevErr)
	}
}

// --- Step 7.2: Run tests ---

func TestRun_ProcessesAllListedObjects(t *testing.T) {
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

	records, err := eng.Run(context.Background())
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if len(records) != 3 {
		t.Fatalf("Run() returned %d records, want 3", len(records))
	}

	// Verify each object was processed.
	keys := map[string]bool{}
	for _, r := range records {
		keys[r.ObjectKey] = true
	}
	for _, k := range []string{"a.csv", "b.csv", "c.csv"} {
		if !keys[k] {
			t.Errorf("missing record for object %q", k)
		}
	}

	events := emitter.GetEvents()
	if len(events) != 3 {
		t.Errorf("expected 3 events, got %d", len(events))
	}
}

func TestRun_EmptyStagingReturnsEmptyRecords(t *testing.T) {
	t.Parallel()

	transport := &mockTransport{
		ListFn: func(_ context.Context, _ string) ([]types.DataObject, error) {
			return nil, nil
		},
	}

	reg := mutation.NewRegistry()
	scenarios := []scenario.Scenario{newDelayScenario("test-delay", types.SeverityLow)}

	eng := engine.New(
		defaultConfig(),
		transport,
		reg,
		scenarios,
	)

	records, err := eng.Run(context.Background())
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if len(records) != 0 {
		t.Errorf("Run() returned %d records, want 0", len(records))
	}
}

func TestRun_StopsOnContextCancellation(t *testing.T) {
	t.Parallel()

	var callCount atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())

	transport := &mockTransport{
		ListFn: func(_ context.Context, _ string) ([]types.DataObject, error) {
			return []types.DataObject{
				newTestObject("a.csv"),
				newTestObject("b.csv"),
				newTestObject("c.csv"),
			}, nil
		},
		HoldFn: func(_ context.Context, _ string, _ time.Time) error {
			n := callCount.Add(1)
			if n >= 2 {
				cancel()
				return context.Canceled
			}
			return nil
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

	_, err := eng.Run(ctx)
	if err == nil {
		t.Fatal("Run() error = nil, want context cancellation error")
	}
}

// --- Step 8: Dry-run mode tests ---

func TestProcessObject_DryRunSkipsApply(t *testing.T) {
	t.Parallel()

	transport := &mockTransport{}
	emitter := &mockEmitter{}

	reg := mutation.NewRegistry()
	if err := reg.Register(&mutation.DelayMutation{}); err != nil {
		t.Fatalf("register: %v", err)
	}

	scenarios := []scenario.Scenario{newDelayScenario("test-delay", types.SeverityLow)}

	cfg := defaultConfig()
	cfg.DryRun = true

	eng := engine.New(
		cfg,
		transport,
		reg,
		scenarios,
		engine.WithEmitter(emitter),
	)

	obj := newTestObject("data.csv")
	records, err := eng.ProcessObject(context.Background(), obj)
	if err != nil {
		t.Fatalf("ProcessObject() error = %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}

	rec := records[0]
	if rec.ObjectKey != "data.csv" {
		t.Errorf("record ObjectKey = %q, want %q", rec.ObjectKey, "data.csv")
	}
	if rec.Mutation != "delay" {
		t.Errorf("record Mutation = %q, want %q", rec.Mutation, "delay")
	}
	if rec.Applied {
		t.Error("record Applied = true, want false in dry-run mode")
	}
	if rec.Error != "dry-run" {
		t.Errorf("record Error = %q, want %q", rec.Error, "dry-run")
	}

	// Verify transport.Hold was NOT called (mutation not actually applied).
	if holdCount := len(transport.HoldCalls()); holdCount != 0 {
		t.Errorf("transport.Hold called %d times, want 0 in dry-run mode", holdCount)
	}

	// Verify emitter.Emit WAS called (we still observe what would happen).
	events := emitter.GetEvents()
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if events[0].Scenario != "test-delay" {
		t.Errorf("event Scenario = %q, want %q", events[0].Scenario, "test-delay")
	}
}

func TestProcessObject_DryRunStillChecksSafety(t *testing.T) {
	t.Parallel()

	transport := &mockTransport{}
	emitter := &mockEmitter{}
	safety := &mockSafety{Enabled: false, MaxSev: types.SeverityCritical}

	reg := mutation.NewRegistry()
	if err := reg.Register(&mutation.DelayMutation{}); err != nil {
		t.Fatalf("register: %v", err)
	}

	scenarios := []scenario.Scenario{newDelayScenario("test-delay", types.SeverityLow)}

	cfg := defaultConfig()
	cfg.DryRun = true

	eng := engine.New(
		cfg,
		transport,
		reg,
		scenarios,
		engine.WithEmitter(emitter),
		engine.WithSafety(safety),
	)

	records, err := eng.ProcessObject(context.Background(), newTestObject("data.csv"))
	if err != nil {
		t.Fatalf("ProcessObject() error = %v", err)
	}
	if len(records) != 0 {
		t.Errorf("expected 0 records (kill switch disabled), got %d", len(records))
	}

	events := emitter.GetEvents()
	if len(events) != 0 {
		t.Errorf("expected 0 events, got %d", len(events))
	}
}

// --- Cooldown integration tests ---

func TestProcessObject_SkipsOnCooldown(t *testing.T) {
	t.Parallel()

	transport := &mockTransport{}
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

	scenarios := []scenario.Scenario{newDelayScenario("test-delay", types.SeverityLow)}

	eng := engine.New(
		defaultConfig(),
		transport,
		reg,
		scenarios,
		engine.WithEmitter(emitter),
		engine.WithSafety(safety),
	)

	records, err := eng.ProcessObject(context.Background(), newTestObject("data.csv"))
	if err != nil {
		t.Fatalf("ProcessObject() error = %v", err)
	}
	if len(records) != 0 {
		t.Errorf("expected 0 records (cooldown active), got %d", len(records))
	}

	events := emitter.GetEvents()
	if len(events) != 0 {
		t.Errorf("expected 0 events, got %d", len(events))
	}
}

func TestProcessObject_RecordsInjectionAfterApply(t *testing.T) {
	t.Parallel()

	transport := &mockTransport{}
	emitter := &mockEmitter{}
	safety := &mockSafety{
		Enabled:    true,
		MaxSev:     types.SeverityCritical,
		SLAAllowed: true, // SLA window is open; mutation must proceed so RecordInjection is called
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
		engine.WithEmitter(emitter),
		engine.WithSafety(safety),
	)

	_, err := eng.ProcessObject(context.Background(), newTestObject("data.csv"))
	if err != nil {
		t.Fatalf("ProcessObject() error = %v", err)
	}

	calls := safety.GetRecordInjCalls()
	if len(calls) != 1 {
		t.Fatalf("expected 1 RecordInjection call, got %d", len(calls))
	}
	if calls[0] != "test-delay" {
		t.Errorf("RecordInjection called with %q, want %q", calls[0], "test-delay")
	}
}

func TestProcessObject_CooldownErrorPropagatesNonSentinel(t *testing.T) {
	t.Parallel()

	transport := &mockTransport{}
	dbErr := errors.New("db down")
	safety := &mockSafety{
		Enabled:     true,
		MaxSev:      types.SeverityCritical,
		CooldownErr: dbErr,
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
		engine.WithSafety(safety),
	)

	_, err := eng.ProcessObject(context.Background(), newTestObject("data.csv"))
	if err == nil {
		t.Fatal("ProcessObject() error = nil, want error for non-sentinel cooldown error")
	}
	if !errors.Is(err, dbErr) {
		t.Errorf("error = %v, want wrapped %v", err, dbErr)
	}
}

// --- Phase 1: SLA window and blast radius wiring tests ---

func TestProcessObject_SkipsScenarioInSLAWindow(t *testing.T) {
	t.Parallel()

	transport := &mockTransport{}
	emitter := &mockEmitter{}
	// slaAllowed=false means the pipeline IS within its SLA window —
	// chaos injection must be skipped.
	safety := &mockSafety{
		Enabled:    true,
		MaxSev:     types.SeverityCritical,
		SLAAllowed: false,
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
		engine.WithEmitter(emitter),
		engine.WithSafety(safety),
	)

	records, err := eng.ProcessObject(context.Background(), newTestObject("data.csv"))
	if err != nil {
		t.Fatalf("ProcessObject() error = %v, want nil (SLA skip is not an error)", err)
	}
	if len(records) != 0 {
		t.Errorf("expected 0 records (SLA window active), got %d", len(records))
	}

	// No mutation was applied, so no event should have been emitted.
	events := emitter.GetEvents()
	if len(events) != 0 {
		t.Errorf("expected 0 events (SLA window active), got %d", len(events))
	}
}

func TestProcessObject_SLAWindowError(t *testing.T) {
	t.Parallel()

	transport := &mockTransport{}
	slaErr := errors.New("SLA schedule unavailable")
	safety := &mockSafety{
		Enabled: true,
		MaxSev:  types.SeverityCritical,
		SLAErr:  slaErr,
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
		engine.WithSafety(safety),
	)

	_, err := eng.ProcessObject(context.Background(), newTestObject("data.csv"))
	if err == nil {
		t.Fatal("ProcessObject() error = nil, want error when CheckSLAWindow fails")
	}
	if !errors.Is(err, slaErr) {
		t.Errorf("error = %v, want wrapped %v", err, slaErr)
	}
}

func TestRun_StopsInjectingWhenBlastRadiusExceeded(t *testing.T) {
	t.Parallel()

	// Three objects; blast radius check returns an error after the first
	// affected target, so only objects before that threshold should be mutated.
	// Crucially, Run must NOT return an error itself (fail-open passthrough).
	transport := &mockTransport{
		ListFn: func(_ context.Context, _ string) ([]types.DataObject, error) {
			return []types.DataObject{
				newTestObject("a.csv"),
				newTestObject("b.csv"),
				newTestObject("c.csv"),
			}, nil
		},
	}

	blastExceeded := errors.New("blast radius exceeded")
	safety := &mockSafety{
		Enabled:    true,
		MaxSev:     types.SeverityCritical,
		SLAAllowed: true, // SLA window is open; mutations may proceed
		// Return error on the first CheckBlastRadius call — after the first
		// object is processed and its target added to the affected set.
		BlastRadiusFn: func(stats types.ExperimentStats) error {
			if stats.AffectedTargets >= 1 {
				return blastExceeded
			}
			return nil
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
		engine.WithSafety(safety),
	)

	records, err := eng.Run(context.Background())

	// Fail-open: blast radius breach must NOT surface as an error to the caller.
	if err != nil {
		t.Fatalf("Run() error = %v, want nil (blast radius breach is fail-open)", err)
	}

	// Only the first object should have been mutated before the breach stopped
	// further injection.
	if len(records) != 1 {
		t.Errorf("Run() returned %d records, want 1 (injection stopped after blast radius exceeded)", len(records))
	}
	if len(records) > 0 && records[0].ObjectKey != "a.csv" {
		t.Errorf("first record ObjectKey = %q, want %q", records[0].ObjectKey, "a.csv")
	}
}

func TestRun_TracksAffectedPctAndCallsBlastRadius(t *testing.T) {
	t.Parallel()

	// 4 objects: only 2 match the scenario (prefix "hit/"), leaving 2 untouched.
	// After Run, blast radius must have been called with accurate stats reflecting
	// only the matched targets.
	transport := &mockTransport{
		ListFn: func(_ context.Context, _ string) ([]types.DataObject, error) {
			return []types.DataObject{
				newTestObject("hit/a.csv"),
				newTestObject("miss/b.csv"),
				newTestObject("hit/c.csv"),
				newTestObject("miss/d.csv"),
			}, nil
		},
	}

	var capturedStats []types.ExperimentStats
	safety := &mockSafety{
		Enabled:    true,
		MaxSev:     types.SeverityCritical,
		SLAAllowed: true, // SLA window is open; mutations may proceed
		BlastRadiusFn: func(stats types.ExperimentStats) error {
			capturedStats = append(capturedStats, stats)
			return nil
		},
	}

	reg := mutation.NewRegistry()
	if err := reg.Register(&mutation.DelayMutation{}); err != nil {
		t.Fatalf("register: %v", err)
	}

	// Scenario only matches "hit/" prefix.
	scenarios := []scenario.Scenario{newPrefixScenario("hit-delay", "hit/")}

	eng := engine.New(
		defaultConfig(),
		transport,
		reg,
		scenarios,
		engine.WithSafety(safety),
	)

	records, err := eng.Run(context.Background())
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	// Only the 2 "hit/" objects should produce applied records.
	if len(records) != 2 {
		t.Fatalf("Run() returned %d records, want 2", len(records))
	}

	// CheckBlastRadius must have been called after every object (4 calls).
	if len(capturedStats) != 4 {
		t.Fatalf("CheckBlastRadius called %d times, want 4", len(capturedStats))
	}

	// After the last object, TotalObjects must be 4.
	lastStats := capturedStats[len(capturedStats)-1]
	if lastStats.TotalObjects != 4 {
		t.Errorf("final stats.TotalObjects = %d, want 4", lastStats.TotalObjects)
	}
	// 2 of 4 objects were affected.
	if lastStats.AffectedTargets != 2 {
		t.Errorf("final stats.AffectedTargets = %d, want 2", lastStats.AffectedTargets)
	}
	// AffectedPct must be 50%.
	const wantPct = 50.0
	if lastStats.AffectedPct != wantPct {
		t.Errorf("final stats.AffectedPct = %.2f, want %.2f", lastStats.AffectedPct, wantPct)
	}
	// 1 unique scenario was involved.
	if lastStats.AffectedPipelines != 1 {
		t.Errorf("final stats.AffectedPipelines = %d, want 1", lastStats.AffectedPipelines)
	}
}

// --- Phase 2: HeldBytes and MutationsApplied tracking ---

func TestRun_TracksHeldBytesAndMutationsApplied(t *testing.T) {
	t.Parallel()

	// Three objects; all match the scenario and get mutated (Applied=true).
	// ListHeld returns two objects with sizes 400 and 600 = 1000 total bytes.
	heldObjects := []types.HeldObject{
		{DataObject: types.DataObject{Key: "held/a.csv", Size: 400}},
		{DataObject: types.DataObject{Key: "held/b.csv", Size: 600}},
	}

	transport := &mockTransport{
		ListFn: func(_ context.Context, _ string) ([]types.DataObject, error) {
			return []types.DataObject{
				newTestObject("a.csv"),
				newTestObject("b.csv"),
				newTestObject("c.csv"),
			}, nil
		},
		ListHeldFn: func(_ context.Context) ([]types.HeldObject, error) {
			return heldObjects, nil
		},
	}

	var capturedStats []types.ExperimentStats
	safety := &mockSafety{
		Enabled:    true,
		MaxSev:     types.SeverityCritical,
		SLAAllowed: true,
		BlastRadiusFn: func(stats types.ExperimentStats) error {
			capturedStats = append(capturedStats, stats)
			return nil
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
		engine.WithSafety(safety),
	)

	records, err := eng.Run(context.Background())
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if len(records) != 3 {
		t.Fatalf("Run() returned %d records, want 3", len(records))
	}

	// CheckBlastRadius is called after each object — 3 calls.
	if len(capturedStats) != 3 {
		t.Fatalf("CheckBlastRadius called %d times, want 3", len(capturedStats))
	}

	last := capturedStats[len(capturedStats)-1]

	// HeldBytes must equal the sum of sizes from ListHeld (400 + 600 = 1000).
	const wantHeldBytes int64 = 1000
	if last.HeldBytes != wantHeldBytes {
		t.Errorf("final stats.HeldBytes = %d, want %d", last.HeldBytes, wantHeldBytes)
	}

	// MutationsApplied must reflect the 3 applied records.
	if last.MutationsApplied != 3 {
		t.Errorf("final stats.MutationsApplied = %d, want 3", last.MutationsApplied)
	}
}

func TestRun_ListHeldErrorIsIgnored(t *testing.T) {
	t.Parallel()

	// ListHeld returns an error, but Run() should still succeed (fail-open)
	// and mutations should still be applied.
	transport := &mockTransport{
		ListFn: func(_ context.Context, _ string) ([]types.DataObject, error) {
			return []types.DataObject{
				newTestObject("a.csv"),
				newTestObject("b.csv"),
			}, nil
		},
		ListHeldFn: func(_ context.Context) ([]types.HeldObject, error) {
			return nil, errors.New("storage unavailable")
		},
	}

	safety := &mockSafety{
		Enabled:    true,
		MaxSev:     types.SeverityCritical,
		SLAAllowed: true,
		BlastRadiusFn: func(stats types.ExperimentStats) error {
			return nil
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
		engine.WithSafety(safety),
	)

	records, err := eng.Run(context.Background())

	// Fail-open: ListHeld error must NOT surface as an error.
	if err != nil {
		t.Fatalf("Run() error = %v, want nil (ListHeld error is fail-open)", err)
	}

	// Both objects should still have been mutated.
	if len(records) != 2 {
		t.Errorf("Run() returned %d records, want 2", len(records))
	}

	// Verify mutations were actually applied.
	for _, r := range records {
		if !r.Applied {
			t.Errorf("record for %q has Applied=false, want true", r.ObjectKey)
		}
	}
}

func TestRun_StopsWhenMaxMutationsExceeded(t *testing.T) {
	t.Parallel()

	// Five objects; blast radius check returns error when MutationsApplied > 2.
	// Run must stop injecting and return fail-open (no error to caller).
	transport := &mockTransport{
		ListFn: func(_ context.Context, _ string) ([]types.DataObject, error) {
			return []types.DataObject{
				newTestObject("a.csv"),
				newTestObject("b.csv"),
				newTestObject("c.csv"),
				newTestObject("d.csv"),
				newTestObject("e.csv"),
			}, nil
		},
	}

	mutationsLimit := errors.New("blast radius exceeded: mutations limit")
	safety := &mockSafety{
		Enabled:    true,
		MaxSev:     types.SeverityCritical,
		SLAAllowed: true,
		BlastRadiusFn: func(stats types.ExperimentStats) error {
			if stats.MutationsApplied > 2 {
				return mutationsLimit
			}
			return nil
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
		engine.WithSafety(safety),
	)

	records, err := eng.Run(context.Background())

	// Fail-open: mutation limit breach must NOT surface as an error.
	if err != nil {
		t.Fatalf("Run() error = %v, want nil (mutations limit is fail-open)", err)
	}

	// After the 3rd object is processed, MutationsApplied becomes 3 which
	// exceeds the limit of 2; Run stops. So exactly 3 records are returned.
	if len(records) != 3 {
		t.Errorf("Run() returned %d records, want 3 (stopped after limit exceeded on 3rd check)", len(records))
	}
}

// --- Assertion evaluation tests ---

// newAssertScenario creates a scenario with an Expected response block.
func newAssertScenario(name string, asserts []types.Assertion, within time.Duration) scenario.Scenario {
	s := newDelayScenario(name, types.SeverityLow)
	s.Expected = &scenario.ExpectedResponse{
		Within:  types.Duration{Duration: within},
		Asserts: asserts,
	}
	return s
}

func TestEvaluateAssertions_AllSatisfied(t *testing.T) {
	t.Parallel()
	transport := &mockTransport{}
	asserter := &mockAsserter{
		supported: map[types.AssertionType]bool{types.AssertSensorState: true},
		results:   map[string]bool{"pipeline/key": true},
	}
	cfg := defaultConfig()
	cfg.AssertWait = true
	cfg.AssertPollInterval = types.Duration{Duration: 10 * time.Millisecond}

	sc := newAssertScenario("test", []types.Assertion{
		{Type: types.AssertSensorState, Target: "pipeline/key", Condition: types.CondIsStale},
	}, 5*time.Second)

	eng := engine.New(cfg, transport, mutation.NewRegistry(), []scenario.Scenario{sc}, engine.WithAsserter(asserter))
	results := eng.EvaluateAssertions(context.Background(), []scenario.Scenario{sc})

	if len(results) != 1 {
		t.Fatalf("got %d results, want 1", len(results))
	}
	if !results[0].Satisfied {
		t.Error("expected Satisfied=true")
	}
	if results[0].EvalAt.IsZero() {
		t.Error("expected EvalAt to be set")
	}
}

func TestEvaluateAssertions_Timeout(t *testing.T) {
	t.Parallel()
	transport := &mockTransport{}
	asserter := &mockAsserter{
		supported: map[types.AssertionType]bool{types.AssertSensorState: true},
		results:   map[string]bool{"pipeline/key": false}, // never satisfied
	}
	cfg := defaultConfig()
	cfg.AssertWait = true
	cfg.AssertPollInterval = types.Duration{Duration: 10 * time.Millisecond}

	sc := newAssertScenario("test", []types.Assertion{
		{Type: types.AssertSensorState, Target: "pipeline/key", Condition: types.CondIsStale},
	}, 100*time.Millisecond) // short timeout

	eng := engine.New(cfg, transport, mutation.NewRegistry(), []scenario.Scenario{sc}, engine.WithAsserter(asserter))
	results := eng.EvaluateAssertions(context.Background(), []scenario.Scenario{sc})

	if len(results) != 1 {
		t.Fatalf("got %d results, want 1", len(results))
	}
	if results[0].Satisfied {
		t.Error("expected Satisfied=false (timed out)")
	}
}

// --- TargetValidator integration tests ---

// TestEvaluateAssertions_TargetValidator_InvalidTargetSkipsPoll verifies that
// when the asserter implements TargetValidator and ValidateTarget returns an
// error, the assertion result records that error immediately without entering
// the poll loop (Evaluate is never called).
func TestEvaluateAssertions_TargetValidator_InvalidTargetSkipsPoll(t *testing.T) {
	t.Parallel()
	transport := &mockTransport{}
	asserter := &mockValidatingAsserter{
		mockAsserter: mockAsserter{
			supported: map[types.AssertionType]bool{types.AssertSensorState: true},
			results:   map[string]bool{},
		},
		invalidTargets: map[string]bool{"bad-target": true},
	}
	cfg := defaultConfig()
	cfg.AssertWait = true
	cfg.AssertPollInterval = types.Duration{Duration: 10 * time.Millisecond}

	sc := newAssertScenario("test", []types.Assertion{
		{Type: types.AssertSensorState, Target: "bad-target", Condition: types.CondIsStale},
	}, 5*time.Second)

	eng := engine.New(cfg, transport, mutation.NewRegistry(), []scenario.Scenario{sc}, engine.WithAsserter(asserter))
	results := eng.EvaluateAssertions(context.Background(), []scenario.Scenario{sc})

	if len(results) != 1 {
		t.Fatalf("got %d results, want 1", len(results))
	}
	// The result must not be satisfied and must carry the validation error.
	if results[0].Satisfied {
		t.Error("expected Satisfied=false for invalid target")
	}
	if results[0].Error == "" {
		t.Error("expected non-empty Error for invalid target")
	}

	// ValidateTarget must have been called once.
	calls := asserter.getValidateCalls()
	if len(calls) != 1 {
		t.Errorf("ValidateTarget called %d times, want 1", len(calls))
	}

	// Evaluate must NOT have been called (invalid targets skip the poll loop).
	asserter.mu.Lock()
	evalCount := asserter.callCount
	asserter.mu.Unlock()
	if evalCount != 0 {
		t.Errorf("Evaluate called %d times, want 0 (invalid target should not poll)", evalCount)
	}
}

// TestEvaluateAssertions_TargetValidator_ValidTargetEntersPollLoop verifies that
// when the asserter implements TargetValidator and ValidateTarget returns nil,
// the assertion enters the poll loop normally (Evaluate is called).
func TestEvaluateAssertions_TargetValidator_ValidTargetEntersPollLoop(t *testing.T) {
	t.Parallel()
	transport := &mockTransport{}
	asserter := &mockValidatingAsserter{
		mockAsserter: mockAsserter{
			supported: map[types.AssertionType]bool{types.AssertSensorState: true},
			results:   map[string]bool{"pipeline/key": true},
		},
		invalidTargets: map[string]bool{}, // nothing invalid
	}
	cfg := defaultConfig()
	cfg.AssertWait = true
	cfg.AssertPollInterval = types.Duration{Duration: 10 * time.Millisecond}

	sc := newAssertScenario("test", []types.Assertion{
		{Type: types.AssertSensorState, Target: "pipeline/key", Condition: types.CondIsStale},
	}, 5*time.Second)

	eng := engine.New(cfg, transport, mutation.NewRegistry(), []scenario.Scenario{sc}, engine.WithAsserter(asserter))
	results := eng.EvaluateAssertions(context.Background(), []scenario.Scenario{sc})

	if len(results) != 1 {
		t.Fatalf("got %d results, want 1", len(results))
	}
	if !results[0].Satisfied {
		t.Error("expected Satisfied=true for valid target")
	}

	// ValidateTarget must have been called.
	calls := asserter.getValidateCalls()
	if len(calls) != 1 {
		t.Errorf("ValidateTarget called %d times, want 1", len(calls))
	}

	// Evaluate must have been called at least once.
	asserter.mu.Lock()
	evalCount := asserter.callCount
	asserter.mu.Unlock()
	if evalCount == 0 {
		t.Error("Evaluate was not called for valid target")
	}
}

// TestEvaluateAssertions_NoTargetValidator_AllTargetsEnterPollLoop verifies
// backward-compat: when the asserter does NOT implement TargetValidator, all
// assertion targets enter the poll loop regardless of format.
func TestEvaluateAssertions_NoTargetValidator_AllTargetsEnterPollLoop(t *testing.T) {
	t.Parallel()
	transport := &mockTransport{}
	// Use the plain mockAsserter (does not implement TargetValidator).
	asserter := &mockAsserter{
		supported: map[types.AssertionType]bool{types.AssertSensorState: true},
		results:   map[string]bool{"unusual-target": true},
	}
	cfg := defaultConfig()
	cfg.AssertWait = true
	cfg.AssertPollInterval = types.Duration{Duration: 10 * time.Millisecond}

	sc := newAssertScenario("test", []types.Assertion{
		{Type: types.AssertSensorState, Target: "unusual-target", Condition: types.CondIsStale},
	}, 5*time.Second)

	eng := engine.New(cfg, transport, mutation.NewRegistry(), []scenario.Scenario{sc}, engine.WithAsserter(asserter))
	results := eng.EvaluateAssertions(context.Background(), []scenario.Scenario{sc})

	if len(results) != 1 {
		t.Fatalf("got %d results, want 1", len(results))
	}
	if !results[0].Satisfied {
		t.Error("expected Satisfied=true (no TargetValidator, unusual target accepted)")
	}

	// Evaluate must have been called.
	asserter.mu.Lock()
	evalCount := asserter.callCount
	asserter.mu.Unlock()
	if evalCount == 0 {
		t.Error("Evaluate was not called — plain asserter should not run ValidateTarget")
	}
}

func TestEvaluateAssertions_DataStateNative(t *testing.T) {
	t.Parallel()
	transport := &mockTransport{
		ReadFn: func(_ context.Context, _ string) (io.ReadCloser, error) {
			return io.NopCloser(strings.NewReader("data")), nil
		},
	}
	cfg := defaultConfig()
	cfg.AssertWait = true
	cfg.AssertPollInterval = types.Duration{Duration: 10 * time.Millisecond}

	sc := newAssertScenario("test", []types.Assertion{
		{Type: types.AssertDataState, Target: "file.jsonl", Condition: types.CondExists},
	}, 5*time.Second)

	// No external asserter — data_state should be handled natively.
	eng := engine.New(cfg, transport, mutation.NewRegistry(), []scenario.Scenario{sc})
	results := eng.EvaluateAssertions(context.Background(), []scenario.Scenario{sc})

	if len(results) != 1 {
		t.Fatalf("got %d results, want 1", len(results))
	}
	if !results[0].Satisfied {
		t.Error("expected Satisfied=true (file exists)")
	}
}

func TestRun_AssertWaitTrue_EmitsAssertionEvent(t *testing.T) {
	t.Parallel()
	transport := &mockTransport{
		ListFn: func(_ context.Context, _ string) ([]types.DataObject, error) {
			return []types.DataObject{newTestObject("a.csv")}, nil
		},
	}
	emitter := &mockEmitter{}
	asserter := &mockAsserter{
		supported: map[types.AssertionType]bool{types.AssertSensorState: true},
		results:   map[string]bool{"pipeline/key": true},
	}

	reg := mutation.NewRegistry()
	if err := reg.Register(&mutation.DelayMutation{}); err != nil {
		t.Fatalf("register: %v", err)
	}

	cfg := defaultConfig()
	cfg.AssertWait = true
	cfg.AssertPollInterval = types.Duration{Duration: 10 * time.Millisecond}

	sc := newAssertScenario("test-delay", []types.Assertion{
		{Type: types.AssertSensorState, Target: "pipeline/key", Condition: types.CondIsStale},
	}, 5*time.Second)

	eng := engine.New(cfg, transport, reg, []scenario.Scenario{sc},
		engine.WithEmitter(emitter),
		engine.WithAsserter(asserter),
	)

	_, err := eng.Run(context.Background())
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	events := emitter.GetEvents()
	// Should have at least 2 events: the mutation event + the assertion event.
	var assertEvent *types.ChaosEvent
	for i := range events {
		if events[i].Mutation == "assertion_evaluation" {
			assertEvent = &events[i]
			break
		}
	}
	if assertEvent == nil {
		t.Fatal("no assertion_evaluation event found")
	}
	if len(assertEvent.Assertions) != 1 {
		t.Fatalf("assertion event has %d assertions, want 1", len(assertEvent.Assertions))
	}
	if !assertEvent.Assertions[0].Satisfied {
		t.Error("expected assertion Satisfied=true")
	}
}

func TestRun_AssertWaitFalse_WritesUnevaluated(t *testing.T) {
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

	cfg := defaultConfig()
	cfg.AssertWait = false // do not wait for assertions

	sc := newAssertScenario("test-delay", []types.Assertion{
		{Type: types.AssertSensorState, Target: "pipeline/key", Condition: types.CondIsStale},
	}, 5*time.Second)

	eng := engine.New(cfg, transport, reg, []scenario.Scenario{sc},
		engine.WithEmitter(emitter),
	)

	_, err := eng.Run(context.Background())
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	events := emitter.GetEvents()
	var assertEvent *types.ChaosEvent
	for i := range events {
		if events[i].Mutation == "assertion_evaluation" {
			assertEvent = &events[i]
			break
		}
	}
	if assertEvent == nil {
		t.Fatal("no assertion_evaluation event found")
	}
	if len(assertEvent.Assertions) != 1 {
		t.Fatalf("assertion event has %d assertions, want 1", len(assertEvent.Assertions))
	}
	if assertEvent.Assertions[0].Satisfied {
		t.Error("expected Satisfied=false (not evaluated)")
	}
	if !assertEvent.Assertions[0].EvalAt.IsZero() {
		t.Error("expected EvalAt to be zero (not evaluated)")
	}
}

// --- WithClock option tests ---

func TestEngine_UsesInjectedClock(t *testing.T) {
	t.Parallel()
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clk := adapter.NewTestClock(start)

	reg := mutation.NewRegistry()
	eng := engine.New(
		types.EngineConfig{Mode: "deterministic"},
		&mockTransport{},
		reg,
		nil,
		engine.WithClock(clk),
	)
	if eng.Clock() == nil {
		t.Fatal("expected clock to be set")
	}
	if !eng.Clock().Now().Equal(start) {
		t.Errorf("Clock().Now() = %v, want %v", eng.Clock().Now(), start)
	}
}

func TestEngine_ProcessObject_UsesClockForTimestamps(t *testing.T) {
	t.Parallel()
	frozen := time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)
	clk := adapter.NewTestClock(frozen)

	reg := mutation.NewRegistry()
	if err := reg.Register(&mutation.DelayMutation{}); err != nil {
		t.Fatalf("register: %v", err)
	}

	sc := scenario.Scenario{
		Name: "test-sc", Category: "data-arrival", Severity: types.SeverityLow,
		Version: 1, Target: scenario.TargetSpec{Layer: "data"},
		Mutation: scenario.MutationSpec{Type: "delay", Params: map[string]string{"duration": "10m", "release": "true"}},
		Probability: 1.0, Safety: scenario.ScenarioSafety{MaxAffectedPct: 100},
	}

	emitter := &mockEmitter{}
	eng := engine.New(
		types.EngineConfig{Mode: "deterministic", DryRun: true},
		&mockTransport{},
		reg,
		[]scenario.Scenario{sc},
		engine.WithClock(clk),
		engine.WithEmitter(emitter),
	)

	records, err := eng.ProcessObject(context.Background(), types.DataObject{Key: "test.jsonl"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(records) == 0 {
		t.Fatal("expected at least one record")
	}
	if !records[0].Timestamp.Equal(frozen) {
		t.Errorf("record timestamp = %v, want %v", records[0].Timestamp, frozen)
	}
	events := emitter.GetEvents()
	if len(events) > 0 && !events[0].Timestamp.Equal(frozen) {
		t.Errorf("event timestamp = %v, want %v", events[0].Timestamp, frozen)
	}
}

func TestProbabilistic_UsesInjectedClock(t *testing.T) {
	t.Parallel()
	frozen := time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)
	clk := adapter.NewTestClock(frozen)

	eng := engine.New(
		types.EngineConfig{Mode: "probabilistic"},
		&mockTransport{},
		mutation.NewRegistry(),
		nil,
		engine.WithClock(clk),
	)

	if !eng.Clock().Now().Equal(frozen) {
		t.Errorf("engine clock = %v, want %v", eng.Clock().Now(), frozen)
	}
}

func TestEngine_DefaultClockIsWallClock(t *testing.T) {
	t.Parallel()

	reg := mutation.NewRegistry()
	eng := engine.New(
		types.EngineConfig{Mode: "deterministic"},
		&mockTransport{},
		reg,
		nil,
	)
	if eng.Clock() == nil {
		t.Fatal("expected default clock to be set")
	}
	// Verify the default clock returns a time close to now (within 1 second).
	now := time.Now()
	clockNow := eng.Clock().Now()
	if clockNow.Sub(now).Abs() > time.Second {
		t.Errorf("default clock time %v is not close to wall time %v", clockNow, now)
	}
}

// --- not_exists (negative polarity) tests ---

func TestEvaluateAssertions_NotExists_PassesOnTimeout(t *testing.T) {
	t.Parallel()
	asserter := &mockAsserter{
		supported: map[types.AssertionType]bool{types.AssertEventEmitted: true},
		results:   map[string]bool{}, // Event never found (missing key returns false)
	}

	eng := engine.New(
		types.EngineConfig{
			Mode: "deterministic", AssertWait: true,
			AssertPollInterval: types.Duration{Duration: 10 * time.Millisecond},
		},
		&mockTransport{},
		mutation.NewRegistry(),
		nil,
		engine.WithAsserter(asserter),
	)

	sc := newAssertScenario("test", []types.Assertion{
		{Type: types.AssertEventEmitted, Target: "sc/mut", Condition: types.CondNotExists},
	}, 50*time.Millisecond)

	results := eng.EvaluateAssertions(context.Background(), []scenario.Scenario{sc})
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if !results[0].Satisfied {
		t.Error("not_exists assertion should be satisfied when event is never found")
	}
}

func TestEvaluateAssertions_NotExists_FailsWhenFound(t *testing.T) {
	t.Parallel()
	asserter := &mockAsserter{
		supported: map[types.AssertionType]bool{types.AssertEventEmitted: true},
		results:   map[string]bool{"sc/mut": true}, // Event IS found
	}

	eng := engine.New(
		types.EngineConfig{
			Mode: "deterministic", AssertWait: true,
			AssertPollInterval: types.Duration{Duration: 10 * time.Millisecond},
		},
		&mockTransport{},
		mutation.NewRegistry(),
		nil,
		engine.WithAsserter(asserter),
	)

	sc := newAssertScenario("test", []types.Assertion{
		{Type: types.AssertEventEmitted, Target: "sc/mut", Condition: types.CondNotExists},
	}, 200*time.Millisecond)

	start := time.Now()
	results := eng.EvaluateAssertions(context.Background(), []scenario.Scenario{sc})
	elapsed := time.Since(start)

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].Satisfied {
		t.Error("not_exists assertion should NOT be satisfied when event is found")
	}
	// Should fail fast, not wait the full 200ms.
	if elapsed > 100*time.Millisecond {
		t.Errorf("expected fast failure, but took %v", elapsed)
	}
}

func TestEvaluateAssertions_MixedPositiveAndNegative(t *testing.T) {
	t.Parallel()
	asserter := &mockAsserter{
		supported: map[types.AssertionType]bool{types.AssertEventEmitted: true},
		results:   map[string]bool{"found/event": true}, // only found/event is found
	}

	eng := engine.New(
		types.EngineConfig{
			Mode: "deterministic", AssertWait: true,
			AssertPollInterval: types.Duration{Duration: 10 * time.Millisecond},
		},
		&mockTransport{},
		mutation.NewRegistry(),
		nil,
		engine.WithAsserter(asserter),
	)

	sc := newAssertScenario("test", []types.Assertion{
		{Type: types.AssertEventEmitted, Target: "found/event", Condition: types.CondExists},      // positive: should pass
		{Type: types.AssertEventEmitted, Target: "missing/event", Condition: types.CondNotExists}, // negative: should pass (not found)
	}, 50*time.Millisecond)

	results := eng.EvaluateAssertions(context.Background(), []scenario.Scenario{sc})
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	if !results[0].Satisfied {
		t.Error("positive exists assertion should be satisfied")
	}
	if !results[1].Satisfied {
		t.Error("negative not_exists assertion should be satisfied")
	}
}
