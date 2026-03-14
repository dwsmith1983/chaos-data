package mutation

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"time"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// SlowWriteMutation simulates a slow write by adding artificial latency
// before forwarding data through the transport.
type SlowWriteMutation struct{}

// Type returns "slow-write".
func (s *SlowWriteMutation) Type() string { return "slow-write" }

// Apply reads the object, sleeps for the specified latency (plus optional jitter),
// then writes the data to the output.
// Params:
//   - "latency" (required): Go duration string for the base delay.
//   - "jitter" (optional): Go duration string for random additional delay.
func (s *SlowWriteMutation) Apply(ctx context.Context, obj types.DataObject, transport adapter.DataTransport, params map[string]string) (types.MutationRecord, error) {
	latencyStr, ok := params["latency"]
	if !ok || latencyStr == "" {
		err := fmt.Errorf("slow-write mutation: missing required param \"latency\"")
		return types.MutationRecord{Applied: false, Mutation: "slow-write", Error: err.Error()}, err
	}

	latency, err := time.ParseDuration(latencyStr)
	if err != nil {
		err = fmt.Errorf("slow-write mutation: invalid latency %q: %w", latencyStr, err)
		return types.MutationRecord{Applied: false, Mutation: "slow-write", Error: err.Error()}, err
	}

	var jitter time.Duration
	if jitterStr, hasJitter := params["jitter"]; hasJitter {
		jitter, err = time.ParseDuration(jitterStr)
		if err != nil {
			err = fmt.Errorf("slow-write mutation: invalid jitter %q: %w", jitterStr, err)
			return types.MutationRecord{Applied: false, Mutation: "slow-write", Error: err.Error()}, err
		}
		//nolint:gosec // Cryptographic randomness not needed for chaos jitter.
		if jitter > 0 {
			jitter = time.Duration(rand.Int63n(int64(jitter)))
		}
	}

	reader, err := transport.Read(ctx, obj.Key)
	if err != nil {
		err = fmt.Errorf("slow-write mutation: read failed: %w", err)
		return types.MutationRecord{Applied: false, Mutation: "slow-write", Error: err.Error()}, err
	}
	defer reader.Close()

	data, err := io.ReadAll(reader)
	if err != nil {
		err = fmt.Errorf("slow-write mutation: read data failed: %w", err)
		return types.MutationRecord{Applied: false, Mutation: "slow-write", Error: err.Error()}, err
	}

	// Artificial latency.
	time.Sleep(latency + jitter)

	if err := transport.Write(ctx, obj.Key, bytes.NewReader(data)); err != nil {
		err = fmt.Errorf("slow-write mutation: write failed: %w", err)
		return types.MutationRecord{Applied: false, Mutation: "slow-write", Error: err.Error()}, err
	}

	return types.MutationRecord{
		ObjectKey: obj.Key,
		Mutation:  "slow-write",
		Params:    params,
		Applied:   true,
		Timestamp: time.Now(),
	}, nil
}
