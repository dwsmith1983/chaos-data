package mutation

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// StaleReplayMutation writes a copy of the object with a date-prefixed key,
// simulating stale data being replayed from a past date.
type StaleReplayMutation struct{}

// Type returns "stale-replay".
func (s *StaleReplayMutation) Type() string { return "stale-replay" }

// Apply reads the object and writes it to a key incorporating the replay_date.
// Params:
//   - "replay_date" (required): date string like "2024-01-15".
//   - "prefix" (optional): key prefix to prepend.
//
// The resulting key is "date=<replay_date>/<original_key>" or
// "<prefix>/date=<replay_date>/<original_key>" when prefix is set.
func (s *StaleReplayMutation) Apply(ctx context.Context, obj types.DataObject, transport adapter.DataTransport, params map[string]string, clock adapter.Clock) (types.MutationRecord, error) {
	replayDate, ok := params["replay_date"]
	if !ok || replayDate == "" {
		err := fmt.Errorf("stale-replay mutation: missing required param \"replay_date\"")
		return types.MutationRecord{Applied: false, Mutation: "stale-replay", Error: err.Error()}, err
	}

	reader, err := transport.Read(ctx, obj.Key)
	if err != nil {
		err = fmt.Errorf("stale-replay mutation: read failed: %w", err)
		return types.MutationRecord{Applied: false, Mutation: "stale-replay", Error: err.Error()}, err
	}
	defer reader.Close()

	data, err := io.ReadAll(reader)
	if err != nil {
		err = fmt.Errorf("stale-replay mutation: read data failed: %w", err)
		return types.MutationRecord{Applied: false, Mutation: "stale-replay", Error: err.Error()}, err
	}

	// Build the date-prefixed key.
	replayKey := fmt.Sprintf("date=%s/%s", replayDate, obj.Key)
	if prefix, hasPrefix := params["prefix"]; hasPrefix && prefix != "" {
		replayKey = fmt.Sprintf("%s/date=%s/%s", prefix, replayDate, obj.Key)
	}

	if err := transport.Write(ctx, replayKey, bytes.NewReader(data)); err != nil {
		err = fmt.Errorf("stale-replay mutation: write failed: %w", err)
		return types.MutationRecord{Applied: false, Mutation: "stale-replay", Error: err.Error()}, err
	}

	return types.MutationRecord{
		ObjectKey: obj.Key,
		Mutation:  "stale-replay",
		Params:    params,
		Applied:   true,
		Timestamp: clock.Now(),
	}, nil
}
