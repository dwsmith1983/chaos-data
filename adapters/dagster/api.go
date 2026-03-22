package dagster

import (
	"context"
	"errors"
)

// DagsterAPI abstracts dagit GraphQL queries for testability.
type DagsterAPI interface {
	// GetSensor returns the current state of a sensor by name.
	GetSensor(ctx context.Context, name string) (SensorState, error)

	// GetSchedule returns the current state of a schedule by name.
	GetSchedule(ctx context.Context, name string) (ScheduleState, error)

	// GetRun returns the latest run matching the given job name.
	GetRun(ctx context.Context, jobName string) (RunState, error)
}

// ErrDagsterNotFound is returned when a sensor, schedule, or job does not
// exist in the Dagster workspace.
var ErrDagsterNotFound = errors.New("dagster: not found")

// InstigationStatus represents the running/stopped state of a sensor or schedule.
type InstigationStatus string

const (
	InstigationRunning InstigationStatus = "RUNNING"
	InstigationStopped InstigationStatus = "STOPPED"
)

// TickStatus represents the result of a sensor or schedule tick.
type TickStatus string

const (
	TickSuccess TickStatus = "SUCCESS"
	TickSkipped TickStatus = "SKIPPED"
	TickFailure TickStatus = "FAILURE"
)

// SensorState holds the state of a Dagster sensor.
type SensorState struct {
	Name              string
	InstigationStatus InstigationStatus
	LatestTick        *TickStatus // nil = no ticks yet (pending)
}

// ScheduleState holds the state of a Dagster schedule.
type ScheduleState struct {
	Name              string
	InstigationStatus InstigationStatus
	LatestTick        *TickStatus // nil = no ticks yet (pending)
}

// RunStatus represents the status of a Dagster pipeline run.
type RunStatus string

const (
	RunNotStarted RunStatus = "NOT_STARTED"
	RunQueued     RunStatus = "QUEUED"
	RunStarting   RunStatus = "STARTING"
	RunStarted    RunStatus = "STARTED"
	RunSuccess    RunStatus = "SUCCESS"
	RunFailure    RunStatus = "FAILURE"
	RunCanceling  RunStatus = "CANCELING"
	RunCanceled   RunStatus = "CANCELED"
)

// RunState holds the state of a Dagster pipeline run.
type RunState struct {
	RunID  string
	Status RunStatus
}
