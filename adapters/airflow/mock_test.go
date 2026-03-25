package airflow_test

import (
	"context"

	"github.com/dwsmith1983/chaos-data/adapters/airflow"
)

// mockAirflowAPI is a test double for AirflowAPI that delegates each method to
// a configurable function field. Methods return zero values when the
// corresponding field is nil.
type mockAirflowAPI struct {
	getDAGRunFn       func(ctx context.Context, dagID string) (airflow.DAGRunState, error)
	getDAGFn          func(ctx context.Context, dagID string) (airflow.DAGState, error)
	getTaskInstanceFn func(ctx context.Context, dagID, taskID string) (airflow.TaskInstanceState, error)
}

func (m *mockAirflowAPI) GetDAGRun(ctx context.Context, dagID string) (airflow.DAGRunState, error) {
	if m.getDAGRunFn != nil {
		return m.getDAGRunFn(ctx, dagID)
	}
	return airflow.DAGRunState{}, nil
}

func (m *mockAirflowAPI) GetDAG(ctx context.Context, dagID string) (airflow.DAGState, error) {
	if m.getDAGFn != nil {
		return m.getDAGFn(ctx, dagID)
	}
	return airflow.DAGState{}, nil
}

func (m *mockAirflowAPI) GetTaskInstance(ctx context.Context, dagID, taskID string) (airflow.TaskInstanceState, error) {
	if m.getTaskInstanceFn != nil {
		return m.getTaskInstanceFn(ctx, dagID, taskID)
	}
	return airflow.TaskInstanceState{}, nil
}
