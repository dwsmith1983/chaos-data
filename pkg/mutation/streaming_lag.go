package mutation

import (
	"context"
	"fmt"
	"time"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// StreamingLagMutation simulates lag in streaming consumers by holding data
// for a specified duration, preventing it from being visible downstream.
type StreamingLagMutation struct{}

// Type returns "streaming-lag".
func (s *StreamingLagMutation) Type() string { return "streaming-lag" }

// Apply calls transport.Hold with the specified lag duration to simulate
// streaming consumer lag.
// Params:
//   - "lag_duration" (required): Go duration string for the hold period.
//   - "consumer_group" (optional): consumer group name, recorded in params.
func (s *StreamingLagMutation) Apply(ctx context.Context, obj types.DataObject, transport adapter.DataTransport, params map[string]string, clock adapter.Clock) (types.MutationRecord, error) {
	lagStr, ok := params["lag_duration"]
	if !ok || lagStr == "" {
		err := fmt.Errorf("streaming-lag mutation: missing required param \"lag_duration\"")
		return types.MutationRecord{Applied: false, Mutation: "streaming-lag", Error: err.Error()}, err
	}

	lagDuration, err := time.ParseDuration(lagStr)
	if err != nil {
		err = fmt.Errorf("streaming-lag mutation: invalid lag_duration %q: %w", lagStr, err)
		return types.MutationRecord{Applied: false, Mutation: "streaming-lag", Error: err.Error()}, err
	}

	releaseAt := clock.Now().Add(lagDuration)
	if err := transport.Hold(ctx, obj.Key, releaseAt); err != nil {
		err = fmt.Errorf("streaming-lag mutation: hold failed: %w", err)
		return types.MutationRecord{Applied: false, Mutation: "streaming-lag", Error: err.Error()}, err
	}

	return types.MutationRecord{
		ObjectKey: obj.Key,
		Mutation:  "streaming-lag",
		Params:    params,
		Applied:   true,
		Timestamp: clock.Now(),
	}, nil
}
