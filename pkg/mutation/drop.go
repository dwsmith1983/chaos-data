package mutation

import (
	"context"
	"time"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// DropMutation silently drops a data object by not forwarding it through the transport.
// The engine checks the returned MutationRecord and skips forwarding when Applied is true.
type DropMutation struct{}

// Type returns "drop".
func (d *DropMutation) Type() string { return "drop" }

// Apply records a drop mutation without calling any transport methods.
// Params: "scope" (optional: "object" or "partition", default "object").
func (d *DropMutation) Apply(_ context.Context, obj types.DataObject, _ adapter.DataTransport, params map[string]string) (types.MutationRecord, error) {
	scope := "object"
	if s, ok := params["scope"]; ok && s != "" {
		scope = s
	}

	// Include the resolved scope in the recorded params.
	recordParams := make(map[string]string, len(params)+1)
	for k, v := range params {
		recordParams[k] = v
	}
	recordParams["scope"] = scope // always use resolved value

	return types.MutationRecord{
		ObjectKey: obj.Key,
		Mutation:  "drop",
		Params:    recordParams,
		Applied:   true,
		Timestamp: time.Now(),
	}, nil
}
