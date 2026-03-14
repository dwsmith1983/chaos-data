package mutation

import (
	"context"
	"fmt"
	"time"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// JobKillMutation writes a "killed" status for a pipeline job, simulating a
// job that was terminated mid-execution.
type JobKillMutation struct {
	store adapter.StateStore
}

// NewJobKillMutation creates a JobKillMutation with the given state store.
func NewJobKillMutation(store adapter.StateStore) *JobKillMutation {
	return &JobKillMutation{store: store}
}

// Type returns "job-kill".
func (j *JobKillMutation) Type() string { return "job-kill" }

// Apply writes a "killed" status to the state store for the specified trigger key.
// Params:
//   - "pipeline" (required): pipeline name.
//   - "schedule" (required): schedule name.
//   - "date" (required): date string.
//   - "kill_after_pct" (optional, default "50"): percentage completion before kill.
//   - "job_type" (optional, default "glue"): type of job being killed.
func (j *JobKillMutation) Apply(ctx context.Context, obj types.DataObject, _ adapter.DataTransport, params map[string]string) (types.MutationRecord, error) {
	pipeline, ok := params["pipeline"]
	if !ok || pipeline == "" {
		err := fmt.Errorf("job-kill mutation: missing required param \"pipeline\"")
		return types.MutationRecord{Applied: false, Mutation: "job-kill", Error: err.Error()}, err
	}
	schedule, ok := params["schedule"]
	if !ok || schedule == "" {
		err := fmt.Errorf("job-kill mutation: missing required param \"schedule\"")
		return types.MutationRecord{Applied: false, Mutation: "job-kill", Error: err.Error()}, err
	}
	date, ok := params["date"]
	if !ok || date == "" {
		err := fmt.Errorf("job-kill mutation: missing required param \"date\"")
		return types.MutationRecord{Applied: false, Mutation: "job-kill", Error: err.Error()}, err
	}

	triggerKey := adapter.TriggerKey{
		Pipeline: pipeline,
		Schedule: schedule,
		Date:     date,
	}

	if err := j.store.WriteTriggerStatus(ctx, triggerKey, "killed"); err != nil {
		err = fmt.Errorf("job-kill mutation: write trigger status failed: %w", err)
		return types.MutationRecord{Applied: false, Mutation: "job-kill", Error: err.Error()}, err
	}

	// Record defaults in params.
	recordParams := make(map[string]string, len(params)+2)
	for k, v := range params {
		recordParams[k] = v
	}
	if _, ok := recordParams["kill_after_pct"]; !ok {
		recordParams["kill_after_pct"] = "50"
	}
	if _, ok := recordParams["job_type"]; !ok {
		recordParams["job_type"] = "glue"
	}

	return types.MutationRecord{
		ObjectKey: obj.Key,
		Mutation:  "job-kill",
		Params:    recordParams,
		Applied:   true,
		Timestamp: time.Now(),
	}, nil
}
