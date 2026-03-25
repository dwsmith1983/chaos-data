package airflow

import (
	"context"
	"errors"
)

// AirflowAPI abstracts Airflow REST API v1 queries for testability.
type AirflowAPI interface {
	// GetDAGRun returns the latest DAG run for the given DAG ID.
	GetDAGRun(ctx context.Context, dagID string) (DAGRunState, error)

	// GetDAG returns the metadata (paused/active state) of a DAG.
	GetDAG(ctx context.Context, dagID string) (DAGState, error)

	// GetTaskInstance returns the state of a task in the latest run of a DAG.
	// Internally resolves the latest dag_run_id, then fetches the task instance.
	GetTaskInstance(ctx context.Context, dagID, taskID string) (TaskInstanceState, error)
}

// ErrAirflowNotFound is returned when a DAG, DAG run, or task instance
// does not exist in the Airflow workspace.
var ErrAirflowNotFound = errors.New("airflow: not found")

// DAGRunStatus represents the status of an Airflow DAG run.
type DAGRunStatus string

const (
	DAGRunQueued  DAGRunStatus = "queued"
	DAGRunRunning DAGRunStatus = "running"
	DAGRunSuccess DAGRunStatus = "success"
	DAGRunFailed  DAGRunStatus = "failed"
)

// DAGRunState holds the state of an Airflow DAG run.
type DAGRunState struct {
	DagID  string
	RunID  string
	Status DAGRunStatus
}

// DAGState holds the metadata of an Airflow DAG.
type DAGState struct {
	DagID    string
	IsPaused bool
	IsActive bool
}

// TaskInstanceStatus represents the status of an Airflow task instance.
type TaskInstanceStatus string

const (
	TaskRemoved         TaskInstanceStatus = "removed"
	TaskScheduled       TaskInstanceStatus = "scheduled"
	TaskQueued          TaskInstanceStatus = "queued"
	TaskRunning         TaskInstanceStatus = "running"
	TaskSuccess         TaskInstanceStatus = "success"
	TaskRestarting      TaskInstanceStatus = "restarting"
	TaskFailed          TaskInstanceStatus = "failed"
	TaskUpForRetry      TaskInstanceStatus = "up_for_retry"
	TaskUpForReschedule TaskInstanceStatus = "up_for_reschedule"
	TaskUpstreamFailed  TaskInstanceStatus = "upstream_failed"
	TaskSkipped         TaskInstanceStatus = "skipped"
	TaskDeferred        TaskInstanceStatus = "deferred"
)

// TaskInstanceState holds the state of an Airflow task instance.
type TaskInstanceState struct {
	DagID  string
	TaskID string
	RunID  string
	Status TaskInstanceStatus
}
