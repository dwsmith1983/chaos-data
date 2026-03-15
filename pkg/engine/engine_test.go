package engine_test

import (
	"context"
	"errors"
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
	safety := &mockSafety{enabled: false, maxSev: types.SeverityCritical}

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

	events := emitter.getEvents()
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
	transport.mu.Lock()
	calls := len(transport.holdCalls)
	transport.mu.Unlock()
	if calls != 1 {
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

	events := emitter.getEvents()
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
		enabled: true,
		maxSev:  types.SeverityLow, // Only allow low severity.
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

	events := emitter.getEvents()
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

	events := emitter.getEvents()
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
		enabled:    false,
		enabledErr: errors.New("safety controller unavailable"),
		maxSev:     types.SeverityCritical,
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
	if !errors.Is(err, safety.enabledErr) {
		t.Errorf("error = %v, want wrapped %v", err, safety.enabledErr)
	}
}

func TestProcessObject_MaxSeverityErrorReturnsError(t *testing.T) {
	t.Parallel()

	transport := &mockTransport{}
	safety := &mockSafety{
		enabled:   true,
		maxSev:    types.SeverityCritical,
		maxSevErr: errors.New("max severity lookup failed"),
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
	if !errors.Is(err, safety.maxSevErr) {
		t.Errorf("error = %v, want wrapped %v", err, safety.maxSevErr)
	}
}

// --- Step 7.2: Run tests ---

func TestRun_ProcessesAllListedObjects(t *testing.T) {
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

	events := emitter.getEvents()
	if len(events) != 3 {
		t.Errorf("expected 3 events, got %d", len(events))
	}
}

func TestRun_EmptyStagingReturnsEmptyRecords(t *testing.T) {
	t.Parallel()

	transport := &mockTransport{
		listFn: func(_ context.Context, _ string) ([]types.DataObject, error) {
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
		listFn: func(_ context.Context, _ string) ([]types.DataObject, error) {
			return []types.DataObject{
				newTestObject("a.csv"),
				newTestObject("b.csv"),
				newTestObject("c.csv"),
			}, nil
		},
		holdFn: func(_ context.Context, _ string, _ time.Time) error {
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
	transport.mu.Lock()
	holdCount := len(transport.holdCalls)
	transport.mu.Unlock()
	if holdCount != 0 {
		t.Errorf("transport.Hold called %d times, want 0 in dry-run mode", holdCount)
	}

	// Verify emitter.Emit WAS called (we still observe what would happen).
	events := emitter.getEvents()
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
	safety := &mockSafety{enabled: false, maxSev: types.SeverityCritical}

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

	events := emitter.getEvents()
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
		enabled:     true,
		maxSev:      types.SeverityCritical,
		cooldownErr: adapter.ErrCooldownActive,
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

	events := emitter.getEvents()
	if len(events) != 0 {
		t.Errorf("expected 0 events, got %d", len(events))
	}
}

func TestProcessObject_RecordsInjectionAfterApply(t *testing.T) {
	t.Parallel()

	transport := &mockTransport{}
	emitter := &mockEmitter{}
	safety := &mockSafety{
		enabled: true,
		maxSev:  types.SeverityCritical,
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

	safety.mu.Lock()
	calls := make([]string, len(safety.recordInjCalls))
	copy(calls, safety.recordInjCalls)
	safety.mu.Unlock()

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
		enabled:     true,
		maxSev:      types.SeverityCritical,
		cooldownErr: dbErr,
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
