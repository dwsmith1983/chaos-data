package mutation

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// PostRunDriftMutation simulates late-arriving data by splitting JSONL records
// by partition key, delivering the main batch immediately, and holding back a
// percentage of matching records for delayed release.
type PostRunDriftMutation struct{}

// Type returns "post-run-drift".
func (p *PostRunDriftMutation) Type() string { return "post-run-drift" }

// Apply reads JSONL, splits by partition key/value, writes on-time records
// to output, and holds late records via HoldData for delayed delivery.
func (p *PostRunDriftMutation) Apply(ctx context.Context, obj types.DataObject, transport adapter.DataTransport, params map[string]string) (types.MutationRecord, error) {
	const mutType = "post-run-drift"

	partKey := params["partition_key"]
	if partKey == "" {
		err := fmt.Errorf("%s: missing required param \"partition_key\"", mutType)
		return types.MutationRecord{Applied: false, Mutation: mutType, Error: err.Error()}, err
	}
	partValue := params["partition_value"]
	if partValue == "" {
		err := fmt.Errorf("%s: missing required param \"partition_value\"", mutType)
		return types.MutationRecord{Applied: false, Mutation: mutType, Error: err.Error()}, err
	}

	latePct := 20
	if pctStr, ok := params["late_pct"]; ok && pctStr != "" {
		var err error
		latePct, err = strconv.Atoi(pctStr)
		if err != nil {
			err = fmt.Errorf("%s: invalid late_pct %q: %w", mutType, pctStr, err)
			return types.MutationRecord{Applied: false, Mutation: mutType, Error: err.Error()}, err
		}
		if latePct < 1 || latePct > 100 {
			err := fmt.Errorf("%s: late_pct must be 1-100, got %d", mutType, latePct)
			return types.MutationRecord{Applied: false, Mutation: mutType, Error: err.Error()}, err
		}
	}

	delayStr, ok := params["drift_delay"]
	if !ok || delayStr == "" {
		err := fmt.Errorf("%s: missing required param \"drift_delay\"", mutType)
		return types.MutationRecord{Applied: false, Mutation: mutType, Error: err.Error()}, err
	}
	driftDelay, err := time.ParseDuration(delayStr)
	if err != nil {
		err = fmt.Errorf("%s: invalid drift_delay %q: %w", mutType, delayStr, err)
		return types.MutationRecord{Applied: false, Mutation: mutType, Error: err.Error()}, err
	}
	if driftDelay <= 0 {
		err := fmt.Errorf("%s: drift_delay must be > 0, got %v", mutType, driftDelay)
		return types.MutationRecord{Applied: false, Mutation: mutType, Error: err.Error()}, err
	}

	// Read and parse JSONL.
	rc, err := transport.Read(ctx, obj.Key)
	if err != nil {
		err = fmt.Errorf("%s: read failed: %w", mutType, err)
		return types.MutationRecord{Applied: false, Mutation: mutType, Error: err.Error()}, err
	}
	defer rc.Close()

	rawData, err := io.ReadAll(rc)
	if err != nil {
		err = fmt.Errorf("%s: read data failed: %w", mutType, err)
		return types.MutationRecord{Applied: false, Mutation: mutType, Error: err.Error()}, err
	}

	lines := splitJSONL(rawData)
	if len(lines) == 0 {
		if writeErr := transport.Write(ctx, obj.Key, bytes.NewReader(rawData)); writeErr != nil {
			return types.MutationRecord{Applied: false, Mutation: mutType, Error: writeErr.Error()}, writeErr
		}
		err := fmt.Errorf("%s: no records found", mutType)
		return types.MutationRecord{Applied: false, Mutation: mutType, ObjectKey: obj.Key, Error: err.Error()}, err
	}

	// Partition records: find indices of matching lines.
	var matchIdx []int
	for i, line := range lines {
		var rec map[string]any
		if err := json.Unmarshal(line, &rec); err != nil {
			// Non-JSONL: pass through unmodified.
			if writeErr := transport.Write(ctx, obj.Key, bytes.NewReader(rawData)); writeErr != nil {
				return types.MutationRecord{Applied: false, Mutation: mutType, Error: writeErr.Error()}, writeErr
			}
			err = fmt.Errorf("%s: parse error at line %d: %w", mutType, i+1, err)
			return types.MutationRecord{Applied: false, Mutation: mutType, ObjectKey: obj.Key, Error: err.Error()}, err
		}
		if val, exists := rec[partKey]; exists && fmt.Sprintf("%v", val) == partValue {
			matchIdx = append(matchIdx, i)
		}
	}

	if len(matchIdx) == 0 {
		if writeErr := transport.Write(ctx, obj.Key, bytes.NewReader(rawData)); writeErr != nil {
			return types.MutationRecord{Applied: false, Mutation: mutType, Error: writeErr.Error()}, writeErr
		}
		err := fmt.Errorf("%s: no matching records for partition", mutType)
		return types.MutationRecord{Applied: false, Mutation: mutType, ObjectKey: obj.Key, Error: err.Error()}, err
	}

	// Compute late count from tail of matched indices.
	lateCount := len(matchIdx) * latePct / 100
	if lateCount == 0 {
		if writeErr := transport.Write(ctx, obj.Key, bytes.NewReader(rawData)); writeErr != nil {
			return types.MutationRecord{Applied: false, Mutation: mutType, Error: writeErr.Error()}, writeErr
		}
		err := fmt.Errorf("%s: late_pct too low for record count", mutType)
		return types.MutationRecord{Applied: false, Mutation: mutType, ObjectKey: obj.Key, Error: err.Error()}, err
	}

	// Mark the tail lateCount matched indices as late.
	lateSet := make(map[int]bool, lateCount)
	for _, idx := range matchIdx[len(matchIdx)-lateCount:] {
		lateSet[idx] = true
	}

	// Build on-time and late buffers.
	var onTime, late bytes.Buffer
	for i, line := range lines {
		if lateSet[i] {
			late.Write(line)
			late.WriteByte('\n')
		} else {
			onTime.Write(line)
			onTime.WriteByte('\n')
		}
	}

	// Write on-time records to output.
	if err := transport.Write(ctx, obj.Key, &onTime); err != nil {
		err = fmt.Errorf("%s: write on-time failed: %w", mutType, err)
		return types.MutationRecord{Applied: false, Mutation: mutType, Error: err.Error()}, err
	}

	// Compute drift key and hold late records.
	driftKey := driftKeyName(obj.Key)
	releaseAt := time.Now().Add(driftDelay)
	if err := transport.HoldData(ctx, driftKey, &late, releaseAt); err != nil {
		err = fmt.Errorf("%s: hold drift data failed: %w", mutType, err)
		return types.MutationRecord{Applied: false, Mutation: mutType, Error: err.Error()}, err
	}

	return types.MutationRecord{
		ObjectKey: obj.Key,
		Mutation:  mutType,
		Params:    params,
		Applied:   true,
		Timestamp: time.Now(),
	}, nil
}

// splitJSONL splits raw bytes into non-empty trimmed lines.
func splitJSONL(data []byte) [][]byte {
	var lines [][]byte
	for _, line := range bytes.Split(data, []byte("\n")) {
		trimmed := bytes.TrimSpace(line)
		if len(trimmed) > 0 {
			lines = append(lines, trimmed)
		}
	}
	return lines
}

// driftKeyName generates a drift key from the original key by inserting
// "_drift_<unix>" before the extension.
// Example: "ingest/data.jsonl" → "ingest/data_drift_1710748200.jsonl"
func driftKeyName(key string) string {
	dir := filepath.Dir(key)
	base := filepath.Base(key)
	ext := filepath.Ext(base)
	name := strings.TrimSuffix(base, ext)
	driftBase := fmt.Sprintf("%s_drift_%d%s", name, time.Now().Unix(), ext)
	if dir == "." {
		return driftBase
	}
	return filepath.Join(dir, driftBase)
}
