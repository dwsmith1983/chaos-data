package dagster

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// Compile-time interface compliance checks.
var (
	_ adapter.Asserter        = (*DagsterAsserter)(nil)
	_ adapter.TargetValidator = (*DagsterAsserter)(nil)
)

// DagsterAsserter evaluates chaos-data assertions against Dagster state by
// querying the dagit GraphQL API through a DagsterAPI client.
type DagsterAsserter struct {
	api DagsterAPI
}

// NewDagsterAsserter constructs a DagsterAsserter backed by the given DagsterAPI.
func NewDagsterAsserter(api DagsterAPI) *DagsterAsserter {
	return &DagsterAsserter{api: api}
}

// Supports reports whether this asserter handles the given assertion type.
// Returns true for sensor_state, trigger_state, and job_state.
func (d *DagsterAsserter) Supports(assertionType types.AssertionType) bool {
	switch assertionType {
	case types.AssertSensorState, types.AssertTriggerState, types.AssertJobState:
		return true
	default:
		return false
	}
}

// ValidateTarget checks that the assertion target is a single-segment name
// (no "/" separators). Dagster entity names are flat identifiers, not paths.
func (d *DagsterAsserter) ValidateTarget(assertion types.Assertion) error {
	if strings.Contains(assertion.Target, "/") {
		return fmt.Errorf("%s target %q: dagster targets are single-segment names, not paths",
			assertion.Type, assertion.Target)
	}
	return nil
}

// Evaluate checks whether assertion.Condition holds for assertion.Target.
// Returns (true, nil) when satisfied, (false, nil) when not yet satisfied,
// and (false, err) when the evaluation itself failed.
func (d *DagsterAsserter) Evaluate(ctx context.Context, assertion types.Assertion) (bool, error) {
	switch assertion.Type {
	case types.AssertSensorState:
		return d.evalSensor(ctx, assertion)
	case types.AssertTriggerState:
		return d.evalSchedule(ctx, assertion)
	case types.AssertJobState:
		return d.evalJob(ctx, assertion)
	default:
		return false, fmt.Errorf("dagster: unsupported assertion type %q", assertion.Type)
	}
}

// evalSensor evaluates sensor_state assertions.
func (d *DagsterAsserter) evalSensor(ctx context.Context, assertion types.Assertion) (bool, error) {
	state, err := d.api.GetSensor(ctx, assertion.Target)
	if err != nil {
		if errors.Is(err, ErrDagsterNotFound) {
			if assertion.Condition == types.CondExists {
				return false, nil
			}
			return false, err
		}
		return false, err
	}

	switch assertion.Condition {
	case types.CondIsStale:
		return state.LatestTick != nil &&
			(*state.LatestTick == TickSkipped || *state.LatestTick == TickFailure), nil
	case types.CondIsReady:
		return state.LatestTick != nil && *state.LatestTick == TickSuccess, nil
	case types.CondIsPending:
		return state.LatestTick == nil, nil
	case types.CondExists:
		return true, nil
	case types.CondStatusRunning:
		return state.InstigationStatus == InstigationRunning, nil
	case types.CondStatusStopped:
		return state.InstigationStatus == InstigationStopped, nil
	default:
		return false, fmt.Errorf("dagster: unsupported condition %q for sensor_state", assertion.Condition)
	}
}

// evalSchedule evaluates trigger_state assertions against Dagster schedules.
func (d *DagsterAsserter) evalSchedule(ctx context.Context, assertion types.Assertion) (bool, error) {
	// Unsupported conditions are checked before making any API call.
	switch assertion.Condition {
	case types.CondStatusKilled:
		return false, fmt.Errorf("dagster: condition %q is not supported for trigger_state in V1", assertion.Condition)
	case types.CondStatusTimeout:
		return false, fmt.Errorf("dagster: condition %q is not supported for trigger_state in V1", assertion.Condition)
	}

	state, err := d.api.GetSchedule(ctx, assertion.Target)
	if err != nil {
		return false, err
	}

	switch assertion.Condition {
	case types.CondStatusFailed:
		return state.LatestTick != nil && *state.LatestTick == TickFailure, nil
	case types.CondStatusSuccess:
		return state.LatestTick != nil && *state.LatestTick == TickSuccess, nil
	case types.CondWasTriggered:
		return state.LatestTick != nil && *state.LatestTick == TickSuccess, nil
	case types.CondStatusRunning:
		return state.InstigationStatus == InstigationRunning, nil
	case types.CondStatusStopped:
		return state.InstigationStatus == InstigationStopped, nil
	default:
		return false, fmt.Errorf("dagster: unsupported condition %q for trigger_state", assertion.Condition)
	}
}

// evalJob evaluates job_state assertions against Dagster pipeline runs.
func (d *DagsterAsserter) evalJob(ctx context.Context, assertion types.Assertion) (bool, error) {
	state, err := d.api.GetRun(ctx, assertion.Target)
	if err != nil {
		return false, err
	}

	switch assertion.Condition {
	case types.CondStatusFailed:
		return state.Status == RunFailure, nil
	case types.CondStatusRunning:
		return state.Status == RunStarted || state.Status == RunStarting, nil
	case types.CondStatusSuccess:
		return state.Status == RunSuccess, nil
	case types.CondStatusKilled:
		return state.Status == RunCanceled, nil
	case types.CondIsPending:
		return state.Status == RunQueued || state.Status == RunNotStarted, nil
	default:
		return false, fmt.Errorf("dagster: unsupported condition %q for job_state", assertion.Condition)
	}
}
