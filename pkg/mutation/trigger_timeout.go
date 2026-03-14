package mutation

import (
	"context"
	"fmt"
	"time"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// TriggerTimeoutMutation writes a "timeout" status for a pipeline trigger,
// simulating a trigger that timed out waiting for data.
type TriggerTimeoutMutation struct {
	store adapter.StateStore
}

// NewTriggerTimeoutMutation creates a TriggerTimeoutMutation with the given state store.
func NewTriggerTimeoutMutation(store adapter.StateStore) *TriggerTimeoutMutation {
	return &TriggerTimeoutMutation{store: store}
}

// Type returns "trigger-timeout".
func (t *TriggerTimeoutMutation) Type() string { return "trigger-timeout" }

// Apply writes a "timeout" status to the state store for the specified trigger key.
// Params:
//   - "pipeline" (required): pipeline name.
//   - "schedule" (required): schedule name.
//   - "date" (required): date string.
//   - "timeout_duration" (optional, default "30m"): duration before timeout.
func (t *TriggerTimeoutMutation) Apply(ctx context.Context, obj types.DataObject, _ adapter.DataTransport, params map[string]string) (types.MutationRecord, error) {
	pipeline, ok := params["pipeline"]
	if !ok || pipeline == "" {
		err := fmt.Errorf("trigger-timeout mutation: missing required param \"pipeline\"")
		return types.MutationRecord{Applied: false, Mutation: "trigger-timeout", Error: err.Error()}, err
	}
	schedule, ok := params["schedule"]
	if !ok || schedule == "" {
		err := fmt.Errorf("trigger-timeout mutation: missing required param \"schedule\"")
		return types.MutationRecord{Applied: false, Mutation: "trigger-timeout", Error: err.Error()}, err
	}
	date, ok := params["date"]
	if !ok || date == "" {
		err := fmt.Errorf("trigger-timeout mutation: missing required param \"date\"")
		return types.MutationRecord{Applied: false, Mutation: "trigger-timeout", Error: err.Error()}, err
	}

	triggerKey := adapter.TriggerKey{
		Pipeline: pipeline,
		Schedule: schedule,
		Date:     date,
	}

	if err := t.store.WriteTriggerStatus(ctx, triggerKey, "timeout"); err != nil {
		err = fmt.Errorf("trigger-timeout mutation: write trigger status failed: %w", err)
		return types.MutationRecord{Applied: false, Mutation: "trigger-timeout", Error: err.Error()}, err
	}

	// Record timeout_duration in params (use default if not set).
	recordParams := make(map[string]string, len(params)+1)
	for k, v := range params {
		recordParams[k] = v
	}
	if _, ok := recordParams["timeout_duration"]; !ok {
		recordParams["timeout_duration"] = "30m"
	}

	return types.MutationRecord{
		ObjectKey: obj.Key,
		Mutation:  "trigger-timeout",
		Params:    recordParams,
		Applied:   true,
		Timestamp: time.Now(),
	}, nil
}
