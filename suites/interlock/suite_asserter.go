package interlocksuite

import (
	"context"
	"fmt"
	"sync"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// PipelineSettable is implemented by asserters that need the current
// namespaced pipeline set before each scenario evaluation.
type PipelineSettable interface {
	SetPipeline(pipeline string)
}

// ---------------------------------------------------------------------------
// SuiteAsserter — handles interlock_event assertions
// ---------------------------------------------------------------------------

// SuiteAsserter evaluates interlock_event assertions by reading from an
// InterlockEventReader. It bridges the gap between the engine's adapter.Asserter
// interface and the suite's InterlockEventReader which stores Interlock-emitted
// events (JOB_TRIGGERED, VALIDATION_EXHAUSTED, etc.).
type SuiteAsserter struct {
	mu          sync.Mutex
	eventReader InterlockEventReader
	pipeline    string // namespaced pipeline, set per-scenario
}

// NewSuiteAsserter creates a SuiteAsserter backed by the given event reader.
func NewSuiteAsserter(reader InterlockEventReader) *SuiteAsserter {
	return &SuiteAsserter{eventReader: reader}
}

// SetPipeline sets the namespaced pipeline ID for the current scenario.
// Must be called before each scenario's engine creation.
func (a *SuiteAsserter) SetPipeline(pipeline string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.pipeline = pipeline
}

// Supports reports that this asserter handles interlock_event assertions.
func (a *SuiteAsserter) Supports(at types.AssertionType) bool {
	return at == types.AssertInterlockEvent
}

// Evaluate checks whether an interlock event matching the assertion's target
// (event type) exists for the current pipeline.
func (a *SuiteAsserter) Evaluate(ctx context.Context, assertion types.Assertion) (bool, error) {
	a.mu.Lock()
	pipeline := a.pipeline
	a.mu.Unlock()

	events, err := a.eventReader.ReadEvents(ctx, pipeline, assertion.Target)
	if err != nil {
		return false, fmt.Errorf("suite asserter: read events: %w", err)
	}
	found := len(events) > 0

	switch assertion.Condition {
	case types.CondExists:
		return found, nil
	case types.CondNotExists:
		return !found, nil
	default:
		return false, fmt.Errorf("suite asserter: unsupported condition %q for interlock_event", assertion.Condition)
	}
}

// ValidateTarget checks that the assertion target is non-empty.
func (a *SuiteAsserter) ValidateTarget(assertion types.Assertion) error {
	if assertion.Target == "" {
		return fmt.Errorf("interlock_event target must not be empty")
	}
	return nil
}

// ---------------------------------------------------------------------------
// TriggerStateAsserter — handles trigger_state assertions
// ---------------------------------------------------------------------------

// TriggerStateAsserter evaluates trigger_state assertions by reading trigger
// status from the state store. Used in suite context where trigger state is
// written by the harness with schedule="default" and date="default".
type TriggerStateAsserter struct {
	mu       sync.Mutex
	store    adapter.StateStore
	pipeline string // namespaced pipeline, set per-scenario
}

// NewTriggerStateAsserter creates a TriggerStateAsserter backed by the store.
func NewTriggerStateAsserter(store adapter.StateStore) *TriggerStateAsserter {
	return &TriggerStateAsserter{store: store}
}

// SetPipeline sets the namespaced pipeline ID for the current scenario.
func (a *TriggerStateAsserter) SetPipeline(pipeline string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.pipeline = pipeline
}

// Supports reports that this asserter handles trigger_state assertions.
func (a *TriggerStateAsserter) Supports(at types.AssertionType) bool {
	return at == types.AssertTriggerState
}

// Evaluate checks whether the trigger's current status matches the condition.
// The assertion target is the bare pipeline name (the asserter uses the
// namespaced pipeline set via SetPipeline).
func (a *TriggerStateAsserter) Evaluate(ctx context.Context, assertion types.Assertion) (bool, error) {
	a.mu.Lock()
	pipeline := a.pipeline
	a.mu.Unlock()

	key := adapter.TriggerKey{
		Pipeline: pipeline,
		Schedule: "default",
		Date:     "default",
	}
	status, err := a.store.ReadTriggerStatus(ctx, key)
	if err != nil {
		return false, fmt.Errorf("trigger asserter: read status: %w", err)
	}

	switch assertion.Condition {
	case types.CondWasTriggered:
		return status == "triggered", nil
	case types.CondStatusRunning:
		return status == "running", nil
	case types.CondStatusFailed:
		return status == "failed", nil
	case types.CondStatusSuccess:
		return status == "succeeded" || status == "completed", nil
	case types.CondStatusKilled:
		return status == "killed", nil
	case types.CondStatusTimeout:
		return status == "timeout", nil
	case types.CondStatusStopped:
		return status == "stopped", nil
	case types.CondIsStale:
		return false, fmt.Errorf("trigger asserter: is_stale requires watchdog evaluator module (not yet implemented)")
	default:
		return false, fmt.Errorf("trigger asserter: unsupported condition %q", assertion.Condition)
	}
}

// ValidateTarget checks that the assertion target is non-empty.
func (a *TriggerStateAsserter) ValidateTarget(assertion types.Assertion) error {
	if assertion.Target == "" {
		return fmt.Errorf("trigger_state target must not be empty")
	}
	return nil
}

// ---------------------------------------------------------------------------
// RerunStateAsserter — handles rerun_state assertions
// ---------------------------------------------------------------------------

// RerunStateAsserter evaluates rerun_state assertions by counting reruns in the
// state store. Used in suite context where reruns are recorded with
// schedule="default" and date="default".
type RerunStateAsserter struct {
	mu       sync.Mutex
	store    adapter.StateStore
	pipeline string
}

// NewRerunStateAsserter creates a RerunStateAsserter backed by the given store.
func NewRerunStateAsserter(store adapter.StateStore) *RerunStateAsserter {
	return &RerunStateAsserter{store: store}
}

// SetPipeline sets the namespaced pipeline ID for the current scenario.
func (a *RerunStateAsserter) SetPipeline(pipeline string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.pipeline = pipeline
}

// Supports reports that this asserter handles rerun_state assertions.
func (a *RerunStateAsserter) Supports(at types.AssertionType) bool {
	return at == types.AssertRerunState
}

// Evaluate checks whether reruns exist for the current pipeline using the
// state store's CountReruns method with schedule="default" and date="default".
func (a *RerunStateAsserter) Evaluate(ctx context.Context, assertion types.Assertion) (bool, error) {
	a.mu.Lock()
	pipeline := a.pipeline
	a.mu.Unlock()

	count, err := a.store.CountReruns(ctx, pipeline, "default", "default")
	if err != nil {
		return false, fmt.Errorf("rerun asserter: count reruns: %w", err)
	}

	switch assertion.Condition {
	case types.CondExists:
		return count > 0, nil
	case types.CondNotExists:
		return count == 0, nil
	default:
		return false, fmt.Errorf("rerun asserter: unsupported condition %q for rerun_state", assertion.Condition)
	}
}

// ValidateTarget checks that the assertion target is non-empty.
func (a *RerunStateAsserter) ValidateTarget(assertion types.Assertion) error {
	if assertion.Target == "" {
		return fmt.Errorf("rerun_state target must not be empty")
	}
	return nil
}

// ---------------------------------------------------------------------------
// CompositeAsserter — delegates to multiple child asserters
// ---------------------------------------------------------------------------

// CompositeAsserter delegates assertion evaluation to the first child asserter
// that supports the given assertion type. Follows the same pattern as
// CompositeEmitter in pkg/adapter/observer.go.
type CompositeAsserter struct {
	asserters []adapter.Asserter
}

// NewCompositeAsserter creates a CompositeAsserter that delegates to the given
// asserters in order.
func NewCompositeAsserter(asserters ...adapter.Asserter) *CompositeAsserter {
	return &CompositeAsserter{asserters: asserters}
}

// Supports returns true if any child asserter supports the given type.
func (c *CompositeAsserter) Supports(at types.AssertionType) bool {
	for _, a := range c.asserters {
		if a.Supports(at) {
			return true
		}
	}
	return false
}

// Evaluate delegates to the first child asserter that supports the type.
func (c *CompositeAsserter) Evaluate(ctx context.Context, assertion types.Assertion) (bool, error) {
	for _, a := range c.asserters {
		if a.Supports(assertion.Type) {
			return a.Evaluate(ctx, assertion)
		}
	}
	return false, fmt.Errorf("no asserter supports assertion type %q", assertion.Type)
}

// ValidateTarget delegates to the first child asserter that supports the type,
// if it implements adapter.TargetValidator.
func (c *CompositeAsserter) ValidateTarget(assertion types.Assertion) error {
	for _, a := range c.asserters {
		if a.Supports(assertion.Type) {
			if tv, ok := a.(adapter.TargetValidator); ok {
				return tv.ValidateTarget(assertion)
			}
			return nil
		}
	}
	return nil
}
