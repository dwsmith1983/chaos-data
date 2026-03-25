package airflow_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/dwsmith1983/chaos-data/adapters/airflow"
	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// Compile-time interface compliance checks.
var (
	_ adapter.Asserter        = (*airflow.AirflowAsserter)(nil)
	_ adapter.TargetValidator = (*airflow.AirflowAsserter)(nil)
)

// ---- Supports ---------------------------------------------------------------

func TestAirflowAsserter_Supports(t *testing.T) {
	t.Parallel()
	a := airflow.NewAirflowAsserter(&mockAirflowAPI{})

	tests := []struct {
		assertionType types.AssertionType
		want          bool
	}{
		{types.AssertSensorState, true},
		{types.AssertTriggerState, true},
		{types.AssertJobState, true},
		{types.AssertEventEmitted, false},
		{types.AssertDataState, false},
	}

	for _, tc := range tests {
		t.Run(string(tc.assertionType), func(t *testing.T) {
			t.Parallel()
			if got := a.Supports(tc.assertionType); got != tc.want {
				t.Errorf("Supports(%q) = %v, want %v", tc.assertionType, got, tc.want)
			}
		})
	}
}

// ---- ValidateTarget ---------------------------------------------------------

func TestAirflowAsserter_ValidateTarget(t *testing.T) {
	t.Parallel()
	a := airflow.NewAirflowAsserter(&mockAirflowAPI{})

	tests := []struct {
		name      string
		assertion types.Assertion
		wantErr   bool
	}{
		{
			name:      "job_state single segment valid",
			assertion: types.Assertion{Type: types.AssertJobState, Target: "my_dag", Condition: types.CondStatusRunning},
			wantErr:   false,
		},
		{
			name:      "job_state multi-segment invalid",
			assertion: types.Assertion{Type: types.AssertJobState, Target: "my_dag/extra", Condition: types.CondStatusRunning},
			wantErr:   true,
		},
		{
			name:      "trigger_state single segment valid",
			assertion: types.Assertion{Type: types.AssertTriggerState, Target: "my_dag", Condition: types.CondStatusRunning},
			wantErr:   false,
		},
		{
			name:      "sensor_state two segments valid",
			assertion: types.Assertion{Type: types.AssertSensorState, Target: "my_dag/my_sensor", Condition: types.CondIsReady},
			wantErr:   false,
		},
		{
			name:      "sensor_state single segment invalid",
			assertion: types.Assertion{Type: types.AssertSensorState, Target: "my_dag", Condition: types.CondIsReady},
			wantErr:   true,
		},
		{
			name:      "sensor_state three segments invalid",
			assertion: types.Assertion{Type: types.AssertSensorState, Target: "a/b/c", Condition: types.CondIsReady},
			wantErr:   true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := a.ValidateTarget(tc.assertion)
			if (err != nil) != tc.wantErr {
				t.Errorf("ValidateTarget(%+v) error = %v, wantErr %v", tc.assertion, err, tc.wantErr)
			}
		})
	}
}

// ---- job_state --------------------------------------------------------------

func TestAirflowAsserter_Evaluate_JobState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		condition types.Condition
		state     airflow.DAGRunState
		apiErr    error
		wantOK    bool
		wantErr   bool
	}{
		{
			name:      "status:running run running",
			condition: types.CondStatusRunning,
			state:     airflow.DAGRunState{Status: airflow.DAGRunRunning},
			wantOK:    true,
		},
		{
			name:      "status:running run success",
			condition: types.CondStatusRunning,
			state:     airflow.DAGRunState{Status: airflow.DAGRunSuccess},
			wantOK:    false,
		},
		{
			name:      "status:success run success",
			condition: types.CondStatusSuccess,
			state:     airflow.DAGRunState{Status: airflow.DAGRunSuccess},
			wantOK:    true,
		},
		{
			name:      "status:failed run failed",
			condition: types.CondStatusFailed,
			state:     airflow.DAGRunState{Status: airflow.DAGRunFailed},
			wantOK:    true,
		},
		{
			name:      "is_pending run queued",
			condition: types.CondIsPending,
			state:     airflow.DAGRunState{Status: airflow.DAGRunQueued},
			wantOK:    true,
		},
		{
			name:      "is_pending run running",
			condition: types.CondIsPending,
			state:     airflow.DAGRunState{Status: airflow.DAGRunRunning},
			wantOK:    false,
		},
		{
			name:      "status:killed unsupported",
			condition: types.CondStatusKilled,
			wantOK:    false,
			wantErr:   true,
		},
		{
			name:      "ErrAirflowNotFound",
			condition: types.CondStatusRunning,
			apiErr:    airflow.ErrAirflowNotFound,
			wantOK:    false,
			wantErr:   true,
		},
		{
			name:      "API error",
			condition: types.CondStatusRunning,
			apiErr:    errors.New("connection refused"),
			wantOK:    false,
			wantErr:   true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			mock := &mockAirflowAPI{
				getDAGRunFn: func(_ context.Context, _ string) (airflow.DAGRunState, error) {
					return tc.state, tc.apiErr
				},
			}
			a := airflow.NewAirflowAsserter(mock)

			assertion := types.Assertion{
				Type:      types.AssertJobState,
				Target:    "my_dag",
				Condition: tc.condition,
			}

			ok, err := a.Evaluate(context.Background(), assertion)
			if ok != tc.wantOK {
				t.Errorf("Evaluate() ok = %v, want %v", ok, tc.wantOK)
			}
			if (err != nil) != tc.wantErr {
				t.Errorf("Evaluate() err = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

// ---- trigger_state ----------------------------------------------------------

func TestAirflowAsserter_Evaluate_TriggerState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		condition types.Condition
		dagState  airflow.DAGState
		dagErr    error
		runState  airflow.DAGRunState
		runErr    error
		wantOK    bool
		wantErr   bool
	}{
		// status:running — only GetDAG is called
		{
			name:      "status:running not paused and active",
			condition: types.CondStatusRunning,
			dagState:  airflow.DAGState{IsPaused: false, IsActive: true},
			wantOK:    true,
		},
		{
			name:      "status:running paused",
			condition: types.CondStatusRunning,
			dagState:  airflow.DAGState{IsPaused: true, IsActive: true},
			wantOK:    false,
		},
		{
			name:      "status:running not paused but not active",
			condition: types.CondStatusRunning,
			dagState:  airflow.DAGState{IsPaused: false, IsActive: false},
			wantOK:    false,
		},
		// status:stopped — only GetDAG is called
		{
			name:      "status:stopped paused",
			condition: types.CondStatusStopped,
			dagState:  airflow.DAGState{IsPaused: true},
			wantOK:    true,
		},
		{
			name:      "status:stopped not paused",
			condition: types.CondStatusStopped,
			dagState:  airflow.DAGState{IsPaused: false},
			wantOK:    false,
		},
		// status:failed — GetDAG + GetDAGRun
		{
			name:      "status:failed latest run failed",
			condition: types.CondStatusFailed,
			dagState:  airflow.DAGState{DagID: "my_dag", IsActive: true},
			runState:  airflow.DAGRunState{Status: airflow.DAGRunFailed},
			wantOK:    true,
		},
		{
			name:      "status:failed latest run success",
			condition: types.CondStatusFailed,
			dagState:  airflow.DAGState{DagID: "my_dag", IsActive: true},
			runState:  airflow.DAGRunState{Status: airflow.DAGRunSuccess},
			wantOK:    false,
		},
		{
			name:      "status:failed no runs",
			condition: types.CondStatusFailed,
			dagState:  airflow.DAGState{DagID: "my_dag", IsActive: true},
			runErr:    airflow.ErrAirflowNotFound,
			wantOK:    false,
		},
		// status:success — GetDAG + GetDAGRun
		{
			name:      "status:success latest run success",
			condition: types.CondStatusSuccess,
			dagState:  airflow.DAGState{DagID: "my_dag", IsActive: true},
			runState:  airflow.DAGRunState{Status: airflow.DAGRunSuccess},
			wantOK:    true,
		},
		{
			name:      "status:success latest run failed",
			condition: types.CondStatusSuccess,
			dagState:  airflow.DAGState{DagID: "my_dag", IsActive: true},
			runState:  airflow.DAGRunState{Status: airflow.DAGRunFailed},
			wantOK:    false,
		},
		{
			name:      "status:success no runs",
			condition: types.CondStatusSuccess,
			dagState:  airflow.DAGState{DagID: "my_dag", IsActive: true},
			runErr:    airflow.ErrAirflowNotFound,
			wantOK:    false,
		},
		// was_triggered — GetDAG + GetDAGRun
		{
			name:      "was_triggered run exists running",
			condition: types.CondWasTriggered,
			dagState:  airflow.DAGState{DagID: "my_dag", IsActive: true},
			runState:  airflow.DAGRunState{Status: airflow.DAGRunRunning},
			wantOK:    true,
		},
		{
			name:      "was_triggered no runs",
			condition: types.CondWasTriggered,
			dagState:  airflow.DAGState{DagID: "my_dag", IsActive: true},
			runErr:    airflow.ErrAirflowNotFound,
			wantOK:    false,
		},
		// unsupported conditions
		{
			name:      "status:killed unsupported",
			condition: types.CondStatusKilled,
			wantOK:    false,
			wantErr:   true,
		},
		{
			name:      "status:timeout unsupported",
			condition: types.CondStatusTimeout,
			wantOK:    false,
			wantErr:   true,
		},
		// GetDAG errors
		{
			name:      "GetDAG ErrAirflowNotFound",
			condition: types.CondStatusRunning,
			dagErr:    airflow.ErrAirflowNotFound,
			wantOK:    false,
			wantErr:   true,
		},
		{
			name:      "GetDAG API error",
			condition: types.CondStatusRunning,
			dagErr:    errors.New("connection refused"),
			wantOK:    false,
			wantErr:   true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			mock := &mockAirflowAPI{
				getDAGFn: func(_ context.Context, _ string) (airflow.DAGState, error) {
					return tc.dagState, tc.dagErr
				},
				getDAGRunFn: func(_ context.Context, _ string) (airflow.DAGRunState, error) {
					return tc.runState, tc.runErr
				},
			}
			a := airflow.NewAirflowAsserter(mock)

			assertion := types.Assertion{
				Type:      types.AssertTriggerState,
				Target:    "my_dag",
				Condition: tc.condition,
			}

			ok, err := a.Evaluate(context.Background(), assertion)
			if ok != tc.wantOK {
				t.Errorf("Evaluate() ok = %v, want %v", ok, tc.wantOK)
			}
			if (err != nil) != tc.wantErr {
				t.Errorf("Evaluate() err = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

// ---- sensor_state -----------------------------------------------------------

func TestAirflowAsserter_Evaluate_SensorState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		condition types.Condition
		state     airflow.TaskInstanceState
		apiErr    error
		wantOK    bool
		wantErr   bool
	}{
		// is_ready
		{
			name:      "is_ready task success",
			condition: types.CondIsReady,
			state:     airflow.TaskInstanceState{Status: airflow.TaskSuccess},
			wantOK:    true,
		},
		{
			name:      "is_ready task running",
			condition: types.CondIsReady,
			state:     airflow.TaskInstanceState{Status: airflow.TaskRunning},
			wantOK:    false,
		},
		// is_stale
		{
			name:      "is_stale task failed",
			condition: types.CondIsStale,
			state:     airflow.TaskInstanceState{Status: airflow.TaskFailed},
			wantOK:    true,
		},
		{
			name:      "is_stale task upstream_failed",
			condition: types.CondIsStale,
			state:     airflow.TaskInstanceState{Status: airflow.TaskUpstreamFailed},
			wantOK:    true,
		},
		{
			name:      "is_stale task success",
			condition: types.CondIsStale,
			state:     airflow.TaskInstanceState{Status: airflow.TaskSuccess},
			wantOK:    false,
		},
		// is_pending
		{
			name:      "is_pending task scheduled",
			condition: types.CondIsPending,
			state:     airflow.TaskInstanceState{Status: airflow.TaskScheduled},
			wantOK:    true,
		},
		{
			name:      "is_pending task queued",
			condition: types.CondIsPending,
			state:     airflow.TaskInstanceState{Status: airflow.TaskQueued},
			wantOK:    true,
		},
		{
			name:      "is_pending task empty status",
			condition: types.CondIsPending,
			state:     airflow.TaskInstanceState{Status: ""},
			wantOK:    true,
		},
		{
			name:      "is_pending task running",
			condition: types.CondIsPending,
			state:     airflow.TaskInstanceState{Status: airflow.TaskRunning},
			wantOK:    false,
		},
		// status:running
		{
			name:      "status:running task running",
			condition: types.CondStatusRunning,
			state:     airflow.TaskInstanceState{Status: airflow.TaskRunning},
			wantOK:    true,
		},
		{
			name:      "status:running task up_for_reschedule",
			condition: types.CondStatusRunning,
			state:     airflow.TaskInstanceState{Status: airflow.TaskUpForReschedule},
			wantOK:    true,
		},
		{
			name:      "status:running task deferred",
			condition: types.CondStatusRunning,
			state:     airflow.TaskInstanceState{Status: airflow.TaskDeferred},
			wantOK:    true,
		},
		{
			name:      "status:running task success",
			condition: types.CondStatusRunning,
			state:     airflow.TaskInstanceState{Status: airflow.TaskSuccess},
			wantOK:    false,
		},
		// status:stopped — unsupported
		{
			name:      "status:stopped unsupported",
			condition: types.CondStatusStopped,
			wantOK:    false,
			wantErr:   true,
		},
		// exists
		{
			name:      "exists task found",
			condition: types.CondExists,
			state:     airflow.TaskInstanceState{TaskID: "my_task"},
			wantOK:    true,
		},
		{
			name:      "exists ErrAirflowNotFound",
			condition: types.CondExists,
			apiErr:    airflow.ErrAirflowNotFound,
			wantOK:    false,
			wantErr:   false,
		},
		// non-exists condition + ErrAirflowNotFound
		{
			name:      "non-exists ErrAirflowNotFound",
			condition: types.CondIsReady,
			apiErr:    airflow.ErrAirflowNotFound,
			wantOK:    false,
			wantErr:   true,
		},
		// API error
		{
			name:      "API error",
			condition: types.CondIsReady,
			apiErr:    errors.New("network failure"),
			wantOK:    false,
			wantErr:   true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			mock := &mockAirflowAPI{
				getTaskInstanceFn: func(_ context.Context, _, _ string) (airflow.TaskInstanceState, error) {
					return tc.state, tc.apiErr
				},
			}
			a := airflow.NewAirflowAsserter(mock)

			assertion := types.Assertion{
				Type:      types.AssertSensorState,
				Target:    "my_dag/my_task",
				Condition: tc.condition,
			}

			ok, err := a.Evaluate(context.Background(), assertion)
			if ok != tc.wantOK {
				t.Errorf("Evaluate() ok = %v, want %v", ok, tc.wantOK)
			}
			if (err != nil) != tc.wantErr {
				t.Errorf("Evaluate() err = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

// ---- unsupported type -------------------------------------------------------

func TestAirflowAsserter_Evaluate_UnsupportedType(t *testing.T) {
	t.Parallel()
	api := &mockAirflowAPI{}
	a := airflow.NewAirflowAsserter(api)

	ok, err := a.Evaluate(context.Background(), types.Assertion{
		Type: types.AssertEventEmitted, Target: "sc/mut", Condition: types.CondExists,
	})
	if ok {
		t.Error("expected false for unsupported type")
	}
	if err == nil {
		t.Error("expected error for unsupported type")
	}
}

// Verify unused import suppression.
var _ = fmt.Sprintf
