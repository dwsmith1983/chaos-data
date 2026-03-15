package types

import (
	"fmt"
	"strings"
	"time"
)

// AssertionType identifies the kind of system state being asserted.
type AssertionType string

const (
	AssertSensorState  AssertionType = "sensor_state"
	AssertTriggerState AssertionType = "trigger_state"
	AssertEventEmitted AssertionType = "event_emitted"
	AssertJobState     AssertionType = "job_state"
	AssertDataState    AssertionType = "data_state"
)

// IsValid reports whether the assertion type is known.
func (at AssertionType) IsValid() bool {
	switch at {
	case AssertSensorState, AssertTriggerState, AssertEventEmitted, AssertJobState, AssertDataState:
		return true
	default:
		return false
	}
}

// targetSegments returns the expected number of "/" separated segments for
// this type, or -1 if the type is unregistered.
func (at AssertionType) targetSegments() int {
	switch at {
	case AssertSensorState, AssertEventEmitted:
		return 2
	case AssertTriggerState:
		return 3
	case AssertJobState, AssertDataState:
		return 1
	default:
		return -1
	}
}

// Condition is a named predicate from a fixed vocabulary.
type Condition string

const (
	CondIsStale       Condition = "is_stale"
	CondIsReady       Condition = "is_ready"
	CondIsPending     Condition = "is_pending"
	CondExists        Condition = "exists"
	CondNotExists     Condition = "not_exists"
	CondIsHeld        Condition = "is_held"
	CondStatusFailed  Condition = "status:failed"
	CondStatusRunning Condition = "status:running"
	CondStatusSuccess Condition = "status:success"
	CondStatusKilled  Condition = "status:killed"
	CondStatusTimeout Condition = "status:timeout"
	CondWasTriggered  Condition = "was_triggered"
)

// IsValid reports whether the condition is a known predicate.
func (c Condition) IsValid() bool {
	switch c {
	case CondIsStale, CondIsReady, CondIsPending, CondExists, CondNotExists,
		CondIsHeld, CondStatusFailed, CondStatusRunning, CondStatusSuccess,
		CondStatusKilled, CondStatusTimeout, CondWasTriggered:
		return true
	default:
		return false
	}
}

// validConditions maps each assertion type to its allowed conditions.
var validConditions = map[AssertionType]map[Condition]bool{
	AssertSensorState:  {CondIsStale: true, CondIsReady: true, CondIsPending: true, CondExists: true},
	AssertTriggerState: {CondStatusFailed: true, CondStatusSuccess: true, CondStatusKilled: true, CondStatusTimeout: true, CondWasTriggered: true},
	AssertEventEmitted: {CondExists: true},
	AssertJobState:     {CondStatusFailed: true, CondStatusRunning: true, CondStatusSuccess: true},
	AssertDataState:    {CondExists: true, CondNotExists: true, CondIsHeld: true},
}

// ValidFor reports whether condition c is valid for assertion type at.
func (c Condition) ValidFor(at AssertionType) bool {
	m, ok := validConditions[at]
	if !ok {
		return false
	}
	return m[c]
}

// Assertion is a single declarative assertion within an expected_response block.
type Assertion struct {
	Type      AssertionType `yaml:"type"      json:"type"`
	Target    string        `yaml:"target"    json:"target"`
	Condition Condition     `yaml:"condition" json:"condition"`
}

// Validate checks that Type, Condition, and Target are valid and consistent.
func (a Assertion) Validate() error {
	if !a.Type.IsValid() {
		return fmt.Errorf("unknown assertion type %q", a.Type)
	}
	if !a.Condition.IsValid() {
		return fmt.Errorf("unknown condition %q", a.Condition)
	}
	if !a.Condition.ValidFor(a.Type) {
		return fmt.Errorf("condition %q not valid for assertion type %q", a.Condition, a.Type)
	}
	if a.Target == "" {
		return fmt.Errorf("assertion target must not be empty")
	}
	expected := a.Type.targetSegments()
	if expected < 0 {
		return fmt.Errorf("no target segment rule for assertion type %q", a.Type)
	}
	got := strings.Count(a.Target, "/") + 1
	if got != expected {
		return fmt.Errorf("assertion type %q expects %d target segment(s), got %d in %q",
			a.Type, expected, got, a.Target)
	}
	return nil
}

// AssertionResult records the outcome of evaluating a single assertion.
type AssertionResult struct {
	Assertion Assertion `json:"assertion"`
	Satisfied bool      `json:"satisfied"`
	Error     string    `json:"error,omitempty"`
	EvalAt    time.Time `json:"evaluated_at,omitempty"`
}
