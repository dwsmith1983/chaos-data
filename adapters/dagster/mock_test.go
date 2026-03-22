package dagster_test

import (
	"context"

	"github.com/dwsmith1983/chaos-data/adapters/dagster"
)

// mockDagsterAPI is a test double for DagsterAPI that delegates each method to
// a configurable function field. Methods return zero values when the
// corresponding field is nil.
type mockDagsterAPI struct {
	getSensorFn   func(ctx context.Context, name string) (dagster.SensorState, error)
	getScheduleFn func(ctx context.Context, name string) (dagster.ScheduleState, error)
	getRunFn      func(ctx context.Context, jobName string) (dagster.RunState, error)
}

func (m *mockDagsterAPI) GetSensor(ctx context.Context, name string) (dagster.SensorState, error) {
	if m.getSensorFn != nil {
		return m.getSensorFn(ctx, name)
	}
	return dagster.SensorState{}, nil
}

func (m *mockDagsterAPI) GetSchedule(ctx context.Context, name string) (dagster.ScheduleState, error) {
	if m.getScheduleFn != nil {
		return m.getScheduleFn(ctx, name)
	}
	return dagster.ScheduleState{}, nil
}

func (m *mockDagsterAPI) GetRun(ctx context.Context, jobName string) (dagster.RunState, error) {
	if m.getRunFn != nil {
		return m.getRunFn(ctx, jobName)
	}
	return dagster.RunState{}, nil
}
