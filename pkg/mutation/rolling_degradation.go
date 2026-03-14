package mutation

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// RollingDegradationMutation gradually corrupts data records, simulating
// degradation that increases over time. The end_pct determines the final
// percentage of records to corrupt.
type RollingDegradationMutation struct{}

// Type returns "rolling-degradation".
func (r *RollingDegradationMutation) Type() string { return "rolling-degradation" }

// Apply reads JSONL data and corrupts a percentage of records based on end_pct.
// Params:
//   - "start_pct" (required): starting corruption percentage (recorded but end_pct used for apply).
//   - "end_pct" (required): ending corruption percentage (0-100).
//   - "ramp_duration" (required): duration string for the ramp period (recorded in params).
func (r *RollingDegradationMutation) Apply(ctx context.Context, obj types.DataObject, transport adapter.DataTransport, params map[string]string) (types.MutationRecord, error) {
	startPctStr, ok := params["start_pct"]
	if !ok || startPctStr == "" {
		err := fmt.Errorf("rolling-degradation mutation: missing required param \"start_pct\"")
		return types.MutationRecord{Applied: false, Mutation: "rolling-degradation", Error: err.Error()}, err
	}
	_, err := strconv.Atoi(startPctStr)
	if err != nil {
		err = fmt.Errorf("rolling-degradation mutation: invalid start_pct %q: %w", startPctStr, err)
		return types.MutationRecord{Applied: false, Mutation: "rolling-degradation", Error: err.Error()}, err
	}

	endPctStr, ok := params["end_pct"]
	if !ok || endPctStr == "" {
		err := fmt.Errorf("rolling-degradation mutation: missing required param \"end_pct\"")
		return types.MutationRecord{Applied: false, Mutation: "rolling-degradation", Error: err.Error()}, err
	}
	endPct, err := strconv.Atoi(endPctStr)
	if err != nil {
		err = fmt.Errorf("rolling-degradation mutation: invalid end_pct %q: %w", endPctStr, err)
		return types.MutationRecord{Applied: false, Mutation: "rolling-degradation", Error: err.Error()}, err
	}
	if endPct < 0 || endPct > 100 {
		err := fmt.Errorf("rolling-degradation mutation: end_pct must be 0-100, got %d", endPct)
		return types.MutationRecord{Applied: false, Mutation: "rolling-degradation", Error: err.Error()}, err
	}

	rampStr, ok := params["ramp_duration"]
	if !ok || rampStr == "" {
		err := fmt.Errorf("rolling-degradation mutation: missing required param \"ramp_duration\"")
		return types.MutationRecord{Applied: false, Mutation: "rolling-degradation", Error: err.Error()}, err
	}
	if _, err := time.ParseDuration(rampStr); err != nil {
		err = fmt.Errorf("rolling-degradation mutation: invalid ramp_duration %q: %w", rampStr, err)
		return types.MutationRecord{Applied: false, Mutation: "rolling-degradation", Error: err.Error()}, err
	}

	reader, err := transport.Read(ctx, obj.Key)
	if err != nil {
		err = fmt.Errorf("rolling-degradation mutation: read failed: %w", err)
		return types.MutationRecord{Applied: false, Mutation: "rolling-degradation", Error: err.Error()}, err
	}
	defer reader.Close()

	data, err := io.ReadAll(reader)
	if err != nil {
		err = fmt.Errorf("rolling-degradation mutation: read data failed: %w", err)
		return types.MutationRecord{Applied: false, Mutation: "rolling-degradation", Error: err.Error()}, err
	}

	records, err := parseJSONLRecords(data)
	if err != nil {
		err = fmt.Errorf("rolling-degradation mutation: parse failed: %w", err)
		return types.MutationRecord{Applied: false, Mutation: "rolling-degradation", Error: err.Error()}, err
	}

	// Corrupt end_pct of records using null corruption.
	numToCorrupt := int(int64(endPct) * int64(len(records)) / 100)
	indices := selectIndices(len(records), numToCorrupt)
	for _, idx := range indices {
		applyNullCorruption(records[idx])
	}

	output, err := marshalJSONL(records)
	if err != nil {
		err = fmt.Errorf("rolling-degradation mutation: marshal failed: %w", err)
		return types.MutationRecord{Applied: false, Mutation: "rolling-degradation", Error: err.Error()}, err
	}

	if err := transport.Write(ctx, obj.Key, bytes.NewReader(output)); err != nil {
		err = fmt.Errorf("rolling-degradation mutation: write failed: %w", err)
		return types.MutationRecord{Applied: false, Mutation: "rolling-degradation", Error: err.Error()}, err
	}

	return types.MutationRecord{
		ObjectKey: obj.Key,
		Mutation:  "rolling-degradation",
		Params:    params,
		Applied:   true,
		Timestamp: time.Now(),
	}, nil
}
