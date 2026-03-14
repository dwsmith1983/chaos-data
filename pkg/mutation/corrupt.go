package mutation

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"strconv"
	"time"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// CorruptMutation corrupts data within objects by modifying individual JSON records.
type CorruptMutation struct{}

// Type returns "corrupt".
func (c *CorruptMutation) Type() string { return "corrupt" }

// Apply reads the object as JSONL, corrupts a percentage of records, and writes back.
// Params:
//   - "affected_pct" (optional, default "10"): percentage of records to corrupt (0-100).
//   - "corruption_type" (optional, default "null"): type of corruption to apply.
//     Currently supported: "null" (set a random field to nil).
func (c *CorruptMutation) Apply(ctx context.Context, obj types.DataObject, transport adapter.DataTransport, params map[string]string) (types.MutationRecord, error) {
	// Read the object data.
	reader, err := transport.Read(ctx, obj.Key)
	if err != nil {
		err = fmt.Errorf("corrupt mutation: read failed: %w", err)
		return types.MutationRecord{Applied: false, Mutation: "corrupt", Error: err.Error()}, err
	}
	defer reader.Close()

	data, err := io.ReadAll(reader)
	if err != nil {
		err = fmt.Errorf("corrupt mutation: read data failed: %w", err)
		return types.MutationRecord{Applied: false, Mutation: "corrupt", Error: err.Error()}, err
	}

	// Parse affected percentage.
	affectedPct := 10
	if pctStr, ok := params["affected_pct"]; ok {
		affectedPct, err = strconv.Atoi(pctStr)
		if err != nil {
			err = fmt.Errorf("corrupt mutation: invalid affected_pct %q: %w", pctStr, err)
			return types.MutationRecord{Applied: false, Mutation: "corrupt", Error: err.Error()}, err
		}
		if affectedPct < 0 || affectedPct > 100 {
			err = fmt.Errorf("corrupt mutation: affected_pct must be 0-100, got %d", affectedPct)
			return types.MutationRecord{Applied: false, Mutation: "corrupt", Error: err.Error()}, err
		}
	}

	// Validate corruption type.
	corruptionType := params["corruption_type"]
	if corruptionType == "" {
		corruptionType = "null"
	}
	if corruptionType != "null" {
		err := fmt.Errorf("corrupt mutation: unsupported corruption type: %q", corruptionType)
		return types.MutationRecord{Applied: false, Mutation: "corrupt", Error: err.Error()}, err
	}

	// Parse JSONL records.
	records, err := parseJSONLRecords(data)
	if err != nil {
		err = fmt.Errorf("corrupt mutation: parse failed: %w", err)
		return types.MutationRecord{Applied: false, Mutation: "corrupt", Error: err.Error()}, err
	}

	// Determine which records to corrupt.
	numToCorrupt := int(int64(affectedPct) * int64(len(records)) / 100)
	indices := selectIndices(len(records), numToCorrupt)

	// Apply corruption.
	for _, idx := range indices {
		applyNullCorruption(records[idx])
	}

	// Marshal back to JSONL.
	output, err := marshalJSONL(records)
	if err != nil {
		err = fmt.Errorf("corrupt mutation: marshal failed: %w", err)
		return types.MutationRecord{Applied: false, Mutation: "corrupt", Error: err.Error()}, err
	}

	// Write corrupted data back.
	if err := transport.Write(ctx, obj.Key, bytes.NewReader(output)); err != nil {
		err = fmt.Errorf("corrupt mutation: write failed: %w", err)
		return types.MutationRecord{Applied: false, Mutation: "corrupt", Error: err.Error()}, err
	}

	return types.MutationRecord{
		ObjectKey: obj.Key,
		Mutation:  "corrupt",
		Params:    params,
		Applied:   true,
		Timestamp: time.Now(),
	}, nil
}

// parseJSONLRecords parses line-delimited JSON data into a slice of maps.
func parseJSONLRecords(data []byte) ([]map[string]interface{}, error) {
	var records []map[string]interface{}
	scanner := bufio.NewScanner(bytes.NewReader(data))
	scanner.Buffer(make([]byte, 0, 1<<20), 1<<20)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var m map[string]interface{}
		if err := json.Unmarshal(line, &m); err != nil {
			return nil, fmt.Errorf("unmarshal line: %w", err)
		}
		records = append(records, m)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan: %w", err)
	}
	return records, nil
}

// marshalJSONL serializes records back to line-delimited JSON.
func marshalJSONL(records []map[string]interface{}) ([]byte, error) {
	var buf bytes.Buffer
	for _, r := range records {
		line, err := json.Marshal(r)
		if err != nil {
			return nil, err
		}
		buf.Write(line)
		buf.WriteByte('\n')
	}
	return buf.Bytes(), nil
}

// selectIndices picks numToSelect unique random indices from [0, total).
func selectIndices(total, numToSelect int) []int {
	if numToSelect >= total {
		indices := make([]int, total)
		for i := range total {
			indices[i] = i
		}
		return indices
	}

	//nolint:gosec // Cryptographic randomness not needed for chaos corruption.
	perm := rand.Perm(total)
	return perm[:numToSelect]
}

// applyNullCorruption picks a random field in the record and sets its value to nil.
func applyNullCorruption(record map[string]interface{}) {
	keys := make([]string, 0, len(record))
	for k := range record {
		keys = append(keys, k)
	}
	if len(keys) == 0 {
		return
	}
	//nolint:gosec // Cryptographic randomness not needed for chaos corruption.
	idx := rand.Intn(len(keys))
	record[keys[idx]] = nil
}
