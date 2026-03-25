package airflow

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
	_ adapter.Asserter        = (*AirflowAsserter)(nil)
	_ adapter.TargetValidator = (*AirflowAsserter)(nil)
)

// AirflowAsserter evaluates chaos-data assertions against Airflow state by
// querying the Airflow REST API v1 through an AirflowAPI client.
type AirflowAsserter struct {
	api AirflowAPI
}

// NewAirflowAsserter constructs an AirflowAsserter backed by the given AirflowAPI.
func NewAirflowAsserter(api AirflowAPI) *AirflowAsserter {
	return &AirflowAsserter{api: api}
}

// Supports reports whether this asserter handles the given assertion type.
// Returns true for sensor_state, trigger_state, and job_state.
func (a *AirflowAsserter) Supports(assertionType types.AssertionType) bool {
	switch assertionType {
	case types.AssertSensorState, types.AssertTriggerState, types.AssertJobState:
		return true
	default:
		return false
	}
}

// ValidateTarget checks that the assertion target is well-formed for the given
// assertion type:
//   - job_state and trigger_state: single-segment DAG ID (no "/" separators)
//   - sensor_state: exactly two segments "dag_id/task_id"
func (a *AirflowAsserter) ValidateTarget(assertion types.Assertion) error {
	switch assertion.Type {
	case types.AssertJobState, types.AssertTriggerState:
		if strings.Contains(assertion.Target, "/") {
			return fmt.Errorf("%s target %q: airflow %s targets are single-segment DAG IDs, not paths",
				assertion.Type, assertion.Target, assertion.Type)
		}
	case types.AssertSensorState:
		parts := strings.Split(assertion.Target, "/")
		if len(parts) != 2 {
			return fmt.Errorf("%s target %q: airflow sensor targets must be \"dag_id/task_id\" (exactly 2 segments)",
				assertion.Type, assertion.Target)
		}
	}
	return nil
}

// Evaluate checks whether assertion.Condition holds for assertion.Target.
// Returns (true, nil) when satisfied, (false, nil) when not yet satisfied,
// and (false, err) when the evaluation itself failed.
func (a *AirflowAsserter) Evaluate(ctx context.Context, assertion types.Assertion) (bool, error) {
	switch assertion.Type {
	case types.AssertJobState:
		return a.evalJob(ctx, assertion)
	case types.AssertTriggerState:
		return a.evalTrigger(ctx, assertion)
	case types.AssertSensorState:
		return a.evalSensor(ctx, assertion)
	default:
		return false, fmt.Errorf("airflow: unsupported assertion type %q", assertion.Type)
	}
}

// evalJob evaluates job_state assertions against the latest DAG run.
func (a *AirflowAsserter) evalJob(ctx context.Context, assertion types.Assertion) (bool, error) {
	// Unsupported conditions checked before any API call.
	if assertion.Condition == types.CondStatusKilled {
		return false, fmt.Errorf("airflow: condition %q is not supported for job_state in V1", assertion.Condition)
	}

	run, err := a.api.GetDAGRun(ctx, assertion.Target)
	if err != nil {
		return false, err
	}

	switch assertion.Condition {
	case types.CondStatusRunning:
		return run.Status == DAGRunRunning, nil
	case types.CondStatusSuccess:
		return run.Status == DAGRunSuccess, nil
	case types.CondStatusFailed:
		return run.Status == DAGRunFailed, nil
	case types.CondIsPending:
		return run.Status == DAGRunQueued, nil
	default:
		return false, fmt.Errorf("airflow: unsupported condition %q for job_state", assertion.Condition)
	}
}

// evalTrigger evaluates trigger_state assertions against DAG metadata and
// optionally the latest DAG run.
func (a *AirflowAsserter) evalTrigger(ctx context.Context, assertion types.Assertion) (bool, error) {
	// Unsupported conditions checked before any API call.
	switch assertion.Condition {
	case types.CondStatusKilled:
		return false, fmt.Errorf("airflow: condition %q is not supported for trigger_state in V1", assertion.Condition)
	case types.CondStatusTimeout:
		return false, fmt.Errorf("airflow: condition %q is not supported for trigger_state in V1", assertion.Condition)
	}

	dag, err := a.api.GetDAG(ctx, assertion.Target)
	if err != nil {
		return false, err
	}

	switch assertion.Condition {
	case types.CondStatusRunning:
		return !dag.IsPaused && dag.IsActive, nil
	case types.CondStatusStopped:
		return dag.IsPaused, nil
	case types.CondStatusFailed, types.CondStatusSuccess, types.CondWasTriggered:
		return a.evalTriggerRun(ctx, assertion, dag)
	default:
		return false, fmt.Errorf("airflow: unsupported condition %q for trigger_state", assertion.Condition)
	}
}

// evalTriggerRun evaluates trigger_state conditions that require the latest
// DAG run in addition to the DAG metadata.
func (a *AirflowAsserter) evalTriggerRun(ctx context.Context, assertion types.Assertion, _ DAGState) (bool, error) {
	run, err := a.api.GetDAGRun(ctx, assertion.Target)
	if err != nil {
		if errors.Is(err, ErrAirflowNotFound) {
			return false, nil
		}
		return false, err
	}

	switch assertion.Condition {
	case types.CondStatusFailed:
		return run.Status == DAGRunFailed, nil
	case types.CondStatusSuccess:
		return run.Status == DAGRunSuccess, nil
	case types.CondWasTriggered:
		return true, nil
	default:
		return false, fmt.Errorf("airflow: unsupported condition %q for trigger_state run check", assertion.Condition)
	}
}

// evalSensor evaluates sensor_state assertions against a task instance in a
// DAG run. The target is expected to be "dag_id/task_id".
func (a *AirflowAsserter) evalSensor(ctx context.Context, assertion types.Assertion) (bool, error) {
	// Unsupported conditions checked before any API call.
	if assertion.Condition == types.CondStatusStopped {
		return false, fmt.Errorf("airflow: condition %q is not supported for sensor_state in V1", assertion.Condition)
	}

	parts := strings.SplitN(assertion.Target, "/", 2)
	dagID := parts[0]
	taskID := parts[1]

	task, err := a.api.GetTaskInstance(ctx, dagID, taskID)
	if err != nil {
		if errors.Is(err, ErrAirflowNotFound) {
			if assertion.Condition == types.CondExists {
				return false, nil
			}
			return false, err
		}
		return false, err
	}

	switch assertion.Condition {
	case types.CondIsReady:
		return task.Status == TaskSuccess, nil
	case types.CondIsStale:
		return task.Status == TaskFailed || task.Status == TaskUpstreamFailed, nil
	case types.CondIsPending:
		return task.Status == TaskScheduled || task.Status == TaskQueued || task.Status == "", nil
	case types.CondStatusRunning:
		return task.Status == TaskRunning || task.Status == TaskUpForReschedule || task.Status == TaskDeferred, nil
	case types.CondExists:
		return true, nil
	default:
		return false, fmt.Errorf("airflow: unsupported condition %q for sensor_state", assertion.Condition)
	}
}
