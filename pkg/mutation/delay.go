package mutation

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// DelayMutation introduces a time delay on data objects, simulating late-arriving data.
type DelayMutation struct{}

// Type returns "delay".
func (d *DelayMutation) Type() string { return "delay" }

// Apply executes the delay mutation. Required params: "duration" (Go duration string).
// Optional params: "jitter" (Go duration string), "release" ("true"/"false", default "true").
// When release is "true", the object is held until the computed time.
// When release is "false", the object is deleted, simulating data that never arrives.
func (d *DelayMutation) Apply(ctx context.Context, obj types.DataObject, transport adapter.DataTransport, params map[string]string, clock adapter.Clock) (types.MutationRecord, error) {
	durationStr, ok := params["duration"]
	if !ok || durationStr == "" {
		err := fmt.Errorf("delay mutation: missing required param \"duration\"")
		return types.MutationRecord{Applied: false, Mutation: "delay", Error: err.Error()}, err
	}

	duration, err := time.ParseDuration(durationStr)
	if err != nil {
		err = fmt.Errorf("delay mutation: invalid duration %q: %w", durationStr, err)
		return types.MutationRecord{Applied: false, Mutation: "delay", Error: err.Error()}, err
	}

	var jitter time.Duration
	if jitterStr, hasJitter := params["jitter"]; hasJitter {
		jitter, err = time.ParseDuration(jitterStr)
		if err != nil {
			err = fmt.Errorf("delay mutation: invalid jitter %q: %w", jitterStr, err)
			return types.MutationRecord{Applied: false, Mutation: "delay", Error: err.Error()}, err
		}
		//nolint:gosec // Cryptographic randomness not needed for chaos jitter.
		if jitter > 0 {
			jitter = time.Duration(rand.Int63n(int64(jitter)))
		}
	}

	release := true
	if releaseStr, hasRelease := params["release"]; hasRelease && releaseStr == "false" {
		release = false
	}

	record := types.MutationRecord{
		ObjectKey: obj.Key,
		Mutation:  "delay",
		Params:    params,
		Applied:   true,
		Timestamp: clock.Now(),
	}

	if release {
		releaseAt := clock.Now().Add(duration + jitter)
		if err := transport.Hold(ctx, obj.Key, releaseAt); err != nil {
			err = fmt.Errorf("delay mutation: hold failed: %w", err)
			return types.MutationRecord{Applied: false, Mutation: "delay", Error: err.Error()}, err
		}
	} else {
		if err := transport.Delete(ctx, obj.Key); err != nil {
			err = fmt.Errorf("delay mutation: delete failed: %w", err)
			return types.MutationRecord{Applied: false, Mutation: "delay", Error: err.Error()}, err
		}
	}

	return record, nil
}
