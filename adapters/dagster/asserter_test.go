package dagster_test

import (
	"context"
	"errors"
	"testing"

	"github.com/dwsmith1983/chaos-data/adapters/dagster"
	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// Compile-time interface compliance checks.
var (
	_ adapter.Asserter       = (*dagster.DagsterAsserter)(nil)
	_ adapter.TargetValidator = (*dagster.DagsterAsserter)(nil)
)

// ---- Supports ---------------------------------------------------------------

func TestDagsterAsserter_Supports(t *testing.T) {
	t.Parallel()
	a := dagster.NewDagsterAsserter(&mockDagsterAPI{})

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

func TestDagsterAsserter_ValidateTarget(t *testing.T) {
	t.Parallel()
	a := dagster.NewDagsterAsserter(&mockDagsterAPI{})

	tests := []struct {
		name      string
		assertion types.Assertion
		wantErr   bool
	}{
		{
			name:      "sensor_state single segment valid",
			assertion: types.Assertion{Type: types.AssertSensorState, Target: "my_sensor", Condition: types.CondExists},
			wantErr:   false,
		},
		{
			name:      "sensor_state multi-segment invalid",
			assertion: types.Assertion{Type: types.AssertSensorState, Target: "pipeline/sensor", Condition: types.CondExists},
			wantErr:   true,
		},
		{
			name:      "trigger_state single segment valid",
			assertion: types.Assertion{Type: types.AssertTriggerState, Target: "my_schedule", Condition: types.CondWasTriggered},
			wantErr:   false,
		},
		{
			name:      "trigger_state multi-segment invalid",
			assertion: types.Assertion{Type: types.AssertTriggerState, Target: "a/b", Condition: types.CondWasTriggered},
			wantErr:   true,
		},
		{
			name:      "job_state single segment valid",
			assertion: types.Assertion{Type: types.AssertJobState, Target: "my_job", Condition: types.CondStatusRunning},
			wantErr:   false,
		},
		{
			name:      "job_state multi-segment invalid",
			assertion: types.Assertion{Type: types.AssertJobState, Target: "a/b", Condition: types.CondStatusRunning},
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

// ---- sensor_state -----------------------------------------------------------

func TestDagsterAsserter_Evaluate_SensorState(t *testing.T) {
	t.Parallel()

	skipped := dagster.TickSkipped
	failure := dagster.TickFailure
	success := dagster.TickSuccess

	tests := []struct {
		name      string
		condition types.Condition
		state     dagster.SensorState
		apiErr    error
		wantOK    bool
		wantErr   bool
	}{
		// is_stale
		{
			name:      "is_stale tick SKIPPED",
			condition: types.CondIsStale,
			state:     dagster.SensorState{LatestTick: &skipped},
			wantOK:    true,
		},
		{
			name:      "is_stale tick FAILURE",
			condition: types.CondIsStale,
			state:     dagster.SensorState{LatestTick: &failure},
			wantOK:    true,
		},
		{
			name:      "is_stale tick SUCCESS",
			condition: types.CondIsStale,
			state:     dagster.SensorState{LatestTick: &success},
			wantOK:    false,
		},
		// is_ready
		{
			name:      "is_ready tick SUCCESS",
			condition: types.CondIsReady,
			state:     dagster.SensorState{LatestTick: &success},
			wantOK:    true,
		},
		{
			name:      "is_ready tick SKIPPED",
			condition: types.CondIsReady,
			state:     dagster.SensorState{LatestTick: &skipped},
			wantOK:    false,
		},
		// is_pending
		{
			name:      "is_pending no ticks (nil LatestTick)",
			condition: types.CondIsPending,
			state:     dagster.SensorState{LatestTick: nil},
			wantOK:    true,
		},
		{
			name:      "is_pending has ticks",
			condition: types.CondIsPending,
			state:     dagster.SensorState{LatestTick: &success},
			wantOK:    false,
		},
		// exists
		{
			name:      "exists sensor found",
			condition: types.CondExists,
			state:     dagster.SensorState{Name: "my_sensor"},
			wantOK:    true,
		},
		{
			name:      "exists ErrDagsterNotFound",
			condition: types.CondExists,
			apiErr:    dagster.ErrDagsterNotFound,
			wantOK:    false,
			wantErr:   false,
		},
		// status:running
		{
			name:      "status:running instigator RUNNING",
			condition: types.CondStatusRunning,
			state:     dagster.SensorState{InstigationStatus: dagster.InstigationRunning},
			wantOK:    true,
		},
		{
			name:      "status:running instigator STOPPED",
			condition: types.CondStatusRunning,
			state:     dagster.SensorState{InstigationStatus: dagster.InstigationStopped},
			wantOK:    false,
		},
		// status:stopped
		{
			name:      "status:stopped instigator STOPPED",
			condition: types.CondStatusStopped,
			state:     dagster.SensorState{InstigationStatus: dagster.InstigationStopped},
			wantOK:    true,
		},
		{
			name:      "status:stopped instigator RUNNING",
			condition: types.CondStatusStopped,
			state:     dagster.SensorState{InstigationStatus: dagster.InstigationRunning},
			wantOK:    false,
		},
		// non-exists condition + ErrDagsterNotFound
		{
			name:      "non-exists condition ErrDagsterNotFound",
			condition: types.CondIsReady,
			apiErr:    dagster.ErrDagsterNotFound,
			wantOK:    false,
			wantErr:   true,
		},
		// API error
		{
			name:      "API error",
			condition: types.CondExists,
			apiErr:    errors.New("network failure"),
			wantOK:    false,
			wantErr:   true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			mock := &mockDagsterAPI{
				getSensorFn: func(_ context.Context, _ string) (dagster.SensorState, error) {
					return tc.state, tc.apiErr
				},
			}
			a := dagster.NewDagsterAsserter(mock)

			assertion := types.Assertion{
				Type:      types.AssertSensorState,
				Target:    "my_sensor",
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

func TestDagsterAsserter_Evaluate_TriggerState(t *testing.T) {
	t.Parallel()

	failure := dagster.TickFailure
	success := dagster.TickSuccess

	tests := []struct {
		name      string
		condition types.Condition
		state     dagster.ScheduleState
		apiErr    error
		wantOK    bool
		wantErr   bool
	}{
		// status:failed
		{
			name:      "status:failed tick FAILURE",
			condition: types.CondStatusFailed,
			state:     dagster.ScheduleState{LatestTick: &failure},
			wantOK:    true,
		},
		// status:success
		{
			name:      "status:success tick SUCCESS",
			condition: types.CondStatusSuccess,
			state:     dagster.ScheduleState{LatestTick: &success},
			wantOK:    true,
		},
		// was_triggered
		{
			name:      "was_triggered tick SUCCESS",
			condition: types.CondWasTriggered,
			state:     dagster.ScheduleState{LatestTick: &success},
			wantOK:    true,
		},
		{
			name:      "was_triggered no ticks",
			condition: types.CondWasTriggered,
			state:     dagster.ScheduleState{LatestTick: nil},
			wantOK:    false,
		},
		// status:running
		{
			name:      "status:running instigator RUNNING",
			condition: types.CondStatusRunning,
			state:     dagster.ScheduleState{InstigationStatus: dagster.InstigationRunning},
			wantOK:    true,
		},
		// status:stopped
		{
			name:      "status:stopped instigator STOPPED",
			condition: types.CondStatusStopped,
			state:     dagster.ScheduleState{InstigationStatus: dagster.InstigationStopped},
			wantOK:    true,
		},
		// status:killed — unsupported V1
		{
			name:      "status:killed unsupported",
			condition: types.CondStatusKilled,
			state:     dagster.ScheduleState{},
			wantOK:    false,
			wantErr:   true,
		},
		// status:timeout — unsupported V1
		{
			name:      "status:timeout unsupported",
			condition: types.CondStatusTimeout,
			state:     dagster.ScheduleState{},
			wantOK:    false,
			wantErr:   true,
		},
		// ErrDagsterNotFound
		{
			name:      "ErrDagsterNotFound",
			condition: types.CondWasTriggered,
			apiErr:    dagster.ErrDagsterNotFound,
			wantOK:    false,
			wantErr:   true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			mock := &mockDagsterAPI{
				getScheduleFn: func(_ context.Context, _ string) (dagster.ScheduleState, error) {
					return tc.state, tc.apiErr
				},
			}
			a := dagster.NewDagsterAsserter(mock)

			assertion := types.Assertion{
				Type:      types.AssertTriggerState,
				Target:    "my_schedule",
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

// ---- job_state --------------------------------------------------------------

func TestDagsterAsserter_Evaluate_JobState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		condition types.Condition
		state     dagster.RunState
		apiErr    error
		wantOK    bool
		wantErr   bool
	}{
		// status:failed
		{
			name:      "status:failed FAILURE",
			condition: types.CondStatusFailed,
			state:     dagster.RunState{Status: dagster.RunFailure},
			wantOK:    true,
		},
		// status:running
		{
			name:      "status:running STARTED",
			condition: types.CondStatusRunning,
			state:     dagster.RunState{Status: dagster.RunStarted},
			wantOK:    true,
		},
		{
			name:      "status:running STARTING",
			condition: types.CondStatusRunning,
			state:     dagster.RunState{Status: dagster.RunStarting},
			wantOK:    true,
		},
		{
			name:      "status:running QUEUED",
			condition: types.CondStatusRunning,
			state:     dagster.RunState{Status: dagster.RunQueued},
			wantOK:    false,
		},
		// status:success
		{
			name:      "status:success SUCCESS",
			condition: types.CondStatusSuccess,
			state:     dagster.RunState{Status: dagster.RunSuccess},
			wantOK:    true,
		},
		// status:killed
		{
			name:      "status:killed CANCELED",
			condition: types.CondStatusKilled,
			state:     dagster.RunState{Status: dagster.RunCanceled},
			wantOK:    true,
		},
		// is_pending
		{
			name:      "is_pending QUEUED",
			condition: types.CondIsPending,
			state:     dagster.RunState{Status: dagster.RunQueued},
			wantOK:    true,
		},
		{
			name:      "is_pending NOT_STARTED",
			condition: types.CondIsPending,
			state:     dagster.RunState{Status: dagster.RunNotStarted},
			wantOK:    true,
		},
		{
			name:      "is_pending STARTED",
			condition: types.CondIsPending,
			state:     dagster.RunState{Status: dagster.RunStarted},
			wantOK:    false,
		},
		// ErrDagsterNotFound
		{
			name:      "ErrDagsterNotFound",
			condition: types.CondStatusRunning,
			apiErr:    dagster.ErrDagsterNotFound,
			wantOK:    false,
			wantErr:   true,
		},
		// API error
		{
			name:      "API error",
			condition: types.CondStatusFailed,
			apiErr:    errors.New("connection refused"),
			wantOK:    false,
			wantErr:   true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			mock := &mockDagsterAPI{
				getRunFn: func(_ context.Context, _ string) (dagster.RunState, error) {
					return tc.state, tc.apiErr
				},
			}
			a := dagster.NewDagsterAsserter(mock)

			assertion := types.Assertion{
				Type:      types.AssertJobState,
				Target:    "my_job",
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

func TestDagsterAsserter_Evaluate_UnsupportedType(t *testing.T) {
	t.Parallel()
	api := &mockDagsterAPI{}
	a := dagster.NewDagsterAsserter(api)

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
