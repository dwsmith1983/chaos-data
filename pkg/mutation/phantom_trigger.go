package mutation

import (
	"context"
	"fmt"
	"time"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// PhantomTriggerMutation writes a "triggered" status for a pipeline trigger
// that should not exist, simulating a phantom/spurious trigger event.
type PhantomTriggerMutation struct {
	store adapter.StateStore
}

// NewPhantomTriggerMutation creates a PhantomTriggerMutation with the given state store.
func NewPhantomTriggerMutation(store adapter.StateStore) *PhantomTriggerMutation {
	return &PhantomTriggerMutation{store: store}
}

// Type returns "phantom-trigger".
func (p *PhantomTriggerMutation) Type() string { return "phantom-trigger" }

// Apply writes a "triggered" status to the state store for the specified trigger key.
// Params:
//   - "pipeline" (required): pipeline name.
//   - "schedule" (required): schedule name.
//   - "date" (required): date string.
//   - "trigger_type" (optional, default "scheduled"): type of trigger.
func (p *PhantomTriggerMutation) Apply(ctx context.Context, obj types.DataObject, _ adapter.DataTransport, params map[string]string) (types.MutationRecord, error) {
	pipeline, ok := params["pipeline"]
	if !ok || pipeline == "" {
		err := fmt.Errorf("phantom-trigger mutation: missing required param \"pipeline\"")
		return types.MutationRecord{Applied: false, Mutation: "phantom-trigger", Error: err.Error()}, err
	}
	schedule, ok := params["schedule"]
	if !ok || schedule == "" {
		err := fmt.Errorf("phantom-trigger mutation: missing required param \"schedule\"")
		return types.MutationRecord{Applied: false, Mutation: "phantom-trigger", Error: err.Error()}, err
	}
	date, ok := params["date"]
	if !ok || date == "" {
		err := fmt.Errorf("phantom-trigger mutation: missing required param \"date\"")
		return types.MutationRecord{Applied: false, Mutation: "phantom-trigger", Error: err.Error()}, err
	}

	triggerKey := adapter.TriggerKey{
		Pipeline: pipeline,
		Schedule: schedule,
		Date:     date,
	}

	if err := p.store.WriteTriggerStatus(ctx, triggerKey, "triggered"); err != nil {
		err = fmt.Errorf("phantom-trigger mutation: write trigger status failed: %w", err)
		return types.MutationRecord{Applied: false, Mutation: "phantom-trigger", Error: err.Error()}, err
	}

	// Record trigger_type in params (use default if not set).
	recordParams := make(map[string]string, len(params)+1)
	for k, v := range params {
		recordParams[k] = v
	}
	if _, ok := recordParams["trigger_type"]; !ok {
		recordParams["trigger_type"] = "scheduled"
	}

	return types.MutationRecord{
		ObjectKey: obj.Key,
		Mutation:  "phantom-trigger",
		Params:    recordParams,
		Applied:   true,
		Timestamp: time.Now(),
	}, nil
}
