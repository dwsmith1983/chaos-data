package mutation

import (
	"context"
	"fmt"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// FalseSuccessMutation writes a "succeeded" status for a pipeline trigger,
// simulating a job that reports success without producing expected output.
type FalseSuccessMutation struct {
	store adapter.TriggerStore
}

// NewFalseSuccessMutation creates a FalseSuccessMutation with the given trigger store.
func NewFalseSuccessMutation(store adapter.TriggerStore) *FalseSuccessMutation {
	return &FalseSuccessMutation{store: store}
}

// Type returns "false-success".
func (f *FalseSuccessMutation) Type() string { return "false-success" }

// Apply writes a "succeeded" status to the state store for the specified trigger key.
// Params:
//   - "pipeline" (required): pipeline name.
//   - "schedule" (required): schedule name.
//   - "date" (required): date string.
//   - "job_type" (optional, default "glue"): type of job.
//   - "missing_output" (optional): path of expected output that is missing.
func (f *FalseSuccessMutation) Apply(ctx context.Context, obj types.DataObject, _ adapter.DataTransport, params map[string]string, clock adapter.Clock) (types.MutationRecord, error) {
	pipeline, ok := params["pipeline"]
	if !ok || pipeline == "" {
		err := fmt.Errorf("false-success mutation: missing required param \"pipeline\"")
		return types.MutationRecord{Applied: false, Mutation: "false-success", Error: err.Error()}, err
	}
	schedule, ok := params["schedule"]
	if !ok || schedule == "" {
		err := fmt.Errorf("false-success mutation: missing required param \"schedule\"")
		return types.MutationRecord{Applied: false, Mutation: "false-success", Error: err.Error()}, err
	}
	date, ok := params["date"]
	if !ok || date == "" {
		err := fmt.Errorf("false-success mutation: missing required param \"date\"")
		return types.MutationRecord{Applied: false, Mutation: "false-success", Error: err.Error()}, err
	}

	triggerKey := adapter.TriggerKey{
		Pipeline: pipeline,
		Schedule: schedule,
		Date:     date,
	}

	if err := f.store.WriteTriggerStatus(ctx, triggerKey, "succeeded"); err != nil {
		err = fmt.Errorf("false-success mutation: write trigger status failed: %w", err)
		return types.MutationRecord{Applied: false, Mutation: "false-success", Error: err.Error()}, err
	}

	// Record defaults in params.
	recordParams := make(map[string]string, len(params)+1)
	for k, v := range params {
		recordParams[k] = v
	}
	if _, ok := recordParams["job_type"]; !ok {
		recordParams["job_type"] = "glue"
	}

	return types.MutationRecord{
		ObjectKey: obj.Key,
		Mutation:  "false-success",
		Params:    recordParams,
		Applied:   true,
		Timestamp: clock.Now(),
	}, nil
}
