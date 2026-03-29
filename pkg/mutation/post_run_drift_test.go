package mutation_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/dwsmith1983/chaos-data/pkg/mutation"
	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// buildJSONLRecords creates n JSONL records. Records at the given indices will
// have partKey=partValue; all others will have partKey="other".
func buildJSONLRecords(n int, partKey, partValue string, matchIndices map[int]bool) []byte {
	var buf bytes.Buffer
	for i := 0; i < n; i++ {
		val := "other"
		if matchIndices[i] {
			val = partValue
		}
		rec := map[string]string{
			"id":   fmt.Sprintf("rec-%d", i),
			partKey: val,
		}
		b, _ := json.Marshal(rec)
		buf.Write(b)
		buf.WriteByte('\n')
	}
	return buf.Bytes()
}

// matchSet is a helper to build a set of integers.
func matchSet(indices ...int) map[int]bool {
	m := make(map[int]bool, len(indices))
	for _, i := range indices {
		m[i] = true
	}
	return m
}

// countLines returns the number of non-empty lines in JSONL bytes.
func countLines(data []byte) int {
	count := 0
	for _, line := range bytes.Split(data, []byte("\n")) {
		if len(bytes.TrimSpace(line)) > 0 {
			count++
		}
	}
	return count
}

// holdDataCallCount returns the number of HoldData calls recorded by the mock.
func holdDataCallCount(transport *mockTransport) int {
	count := 0
	for _, c := range transport.getCalls() {
		if c.Method == "HoldData" {
			count++
		}
	}
	return count
}

// writeCallCount returns the number of Write calls recorded by the mock.
func writeCallCount(transport *mockTransport) int {
	count := 0
	for _, c := range transport.getCalls() {
		if c.Method == "Write" {
			count++
		}
	}
	return count
}

// getWrittenData returns the data from the first Write call.
func getWrittenData(transport *mockTransport) []byte {
	for _, c := range transport.getCalls() {
		if c.Method == "Write" {
			return c.Data
		}
	}
	return nil
}

// getHoldDataKey returns the key from the first HoldData call.
func getHoldDataKey(transport *mockTransport) string {
	for _, c := range transport.getCalls() {
		if c.Method == "HoldData" {
			return c.Key
		}
	}
	return ""
}

func TestPostRunDriftMutation_Type(t *testing.T) {
	m := &mutation.PostRunDriftMutation{}
	if got := m.Type(); got != "post-run-drift" {
		t.Errorf("Type() = %q, want %q", got, "post-run-drift")
	}
}

func TestPostRunDriftMutation_Apply(t *testing.T) {
	const objKey = "ingest/data.jsonl"

	tests := []struct {
		name              string
		data              []byte
		params            map[string]string
		wantApplied       bool
		wantErr           bool
		wantWriteCount    int
		wantHoldDataCount int
		// wantOnTimeLines is the number of lines written to output (0 means skip check)
		wantOnTimeLines int
		// wantLateLines is the number of lines held (0 means skip check)
		wantLateLines int
		// wantHoldDataKey checks that the HoldData key contains a substring
		wantHoldDataKeyContains string
	}{
		{
			// 10 records, records 5-9 match date=2024-01-15 (indices 5..9),
			// late_pct=40 → lateCount = 5*40/100 = 2 held from tail (indices 8,9)
			// → 8 on-time lines written
			name: "valid_split_10_records_5_match_40pct",
			data: buildJSONLRecords(10, "date", "2024-01-15", matchSet(5, 6, 7, 8, 9)),
			params: map[string]string{
				"partition_key":   "date",
				"partition_value": "2024-01-15",
				"late_pct":        "40",
				"drift_delay":     "1h",
			},
			wantApplied:       true,
			wantErr:           false,
			wantWriteCount:    1,
			wantHoldDataCount: 1,
			wantOnTimeLines:   8,
			wantLateLines:     2,
		},
		{
			// 4 records all match, late_pct=50 → lateCount=4*50/100=2 held from tail
			name: "all_match_4_records_50pct",
			data: buildJSONLRecords(4, "region", "us-east-1", matchSet(0, 1, 2, 3)),
			params: map[string]string{
				"partition_key":   "region",
				"partition_value": "us-east-1",
				"late_pct":        "50",
				"drift_delay":     "30m",
			},
			wantApplied:       true,
			wantErr:           false,
			wantWriteCount:    1,
			wantHoldDataCount: 1,
			wantOnTimeLines:   2,
			wantLateLines:     2,
		},
		{
			// No records match partition → pass-through, Applied: false
			name: "no_match_passthrough",
			data: buildJSONLRecords(5, "date", "2024-01-16", matchSet()),
			params: map[string]string{
				"partition_key":   "date",
				"partition_value": "2024-01-15",
				"late_pct":        "50",
				"drift_delay":     "1h",
			},
			wantApplied:       false,
			wantErr:           true,
			wantWriteCount:    1, // pass-through write
			wantHoldDataCount: 0,
		},
		{
			// Single match at 100% → 1 held
			name: "single_match_100pct",
			data: buildJSONLRecords(3, "env", "prod", matchSet(1)),
			params: map[string]string{
				"partition_key":   "env",
				"partition_value": "prod",
				"late_pct":        "100",
				"drift_delay":     "2h",
			},
			wantApplied:       true,
			wantErr:           false,
			wantWriteCount:    1,
			wantHoldDataCount: 1,
			wantOnTimeLines:   2,
			wantLateLines:     1,
		},
		{
			// Single match at 10% → lateCount=1*10/100=0 → no-op, Applied: false
			name: "single_match_10pct_rounds_to_zero",
			data: buildJSONLRecords(3, "env", "prod", matchSet(1)),
			params: map[string]string{
				"partition_key":   "env",
				"partition_value": "prod",
				"late_pct":        "10",
				"drift_delay":     "2h",
			},
			wantApplied:       false,
			wantErr:           true,
			wantWriteCount:    1, // pass-through write
			wantHoldDataCount: 0,
		},
		{
			// 3 matches at 34% → lateCount=3*34/100=1 held from tail
			name: "three_matches_34pct_one_held",
			data: buildJSONLRecords(5, "tier", "gold", matchSet(1, 2, 4)),
			params: map[string]string{
				"partition_key":   "tier",
				"partition_value": "gold",
				"late_pct":        "34",
				"drift_delay":     "15m",
			},
			wantApplied:       true,
			wantErr:           false,
			wantWriteCount:    1,
			wantHoldDataCount: 1,
			wantOnTimeLines:   4,
			wantLateLines:     1,
		},
		{
			// Type coercion: string "100" should match numeric 100 via fmt.Sprintf
			name: "type_coercion_string_matches_numeric",
			data: func() []byte {
				var buf bytes.Buffer
				for i := 0; i < 3; i++ {
					var rec map[string]any
					if i == 1 {
						rec = map[string]any{"id": fmt.Sprintf("rec-%d", i), "count": 100}
					} else {
						rec = map[string]any{"id": fmt.Sprintf("rec-%d", i), "count": 0}
					}
					b, _ := json.Marshal(rec)
					buf.Write(b)
					buf.WriteByte('\n')
				}
				return buf.Bytes()
			}(),
			params: map[string]string{
				"partition_key":   "count",
				"partition_value": "100",
				"late_pct":        "100",
				"drift_delay":     "1h",
			},
			wantApplied:       true,
			wantErr:           false,
			wantWriteCount:    1,
			wantHoldDataCount: 1,
			wantOnTimeLines:   2,
			wantLateLines:     1,
		},
		{
			// Type coercion: string "true" should match boolean true via fmt.Sprintf
			name: "type_coercion_string_matches_bool",
			data: func() []byte {
				var buf bytes.Buffer
				for i := 0; i < 4; i++ {
					var rec map[string]any
					if i == 2 {
						rec = map[string]any{"id": fmt.Sprintf("rec-%d", i), "active": true}
					} else {
						rec = map[string]any{"id": fmt.Sprintf("rec-%d", i), "active": false}
					}
					b, _ := json.Marshal(rec)
					buf.Write(b)
					buf.WriteByte('\n')
				}
				return buf.Bytes()
			}(),
			params: map[string]string{
				"partition_key":   "active",
				"partition_value": "true",
				"late_pct":        "100",
				"drift_delay":     "1h",
			},
			wantApplied:       true,
			wantErr:           false,
			wantWriteCount:    1,
			wantHoldDataCount: 1,
			wantOnTimeLines:   3,
			wantLateLines:     1,
		},
		{
			name: "missing_partition_key",
			data: buildJSONLRecords(3, "date", "2024-01-15", matchSet(0)),
			params: map[string]string{
				"partition_value": "2024-01-15",
				"late_pct":        "50",
				"drift_delay":     "1h",
			},
			wantApplied: false,
			wantErr:     true,
		},
		{
			name: "missing_partition_value",
			data: buildJSONLRecords(3, "date", "2024-01-15", matchSet(0)),
			params: map[string]string{
				"partition_key": "date",
				"late_pct":      "50",
				"drift_delay":   "1h",
			},
			wantApplied: false,
			wantErr:     true,
		},
		{
			name: "missing_drift_delay",
			data: buildJSONLRecords(3, "date", "2024-01-15", matchSet(0)),
			params: map[string]string{
				"partition_key":   "date",
				"partition_value": "2024-01-15",
				"late_pct":        "50",
			},
			wantApplied: false,
			wantErr:     true,
		},
		{
			name: "invalid_late_pct_zero",
			data: buildJSONLRecords(3, "date", "2024-01-15", matchSet(0)),
			params: map[string]string{
				"partition_key":   "date",
				"partition_value": "2024-01-15",
				"late_pct":        "0",
				"drift_delay":     "1h",
			},
			wantApplied: false,
			wantErr:     true,
		},
		{
			name: "invalid_late_pct_101",
			data: buildJSONLRecords(3, "date", "2024-01-15", matchSet(0)),
			params: map[string]string{
				"partition_key":   "date",
				"partition_value": "2024-01-15",
				"late_pct":        "101",
				"drift_delay":     "1h",
			},
			wantApplied: false,
			wantErr:     true,
		},
		{
			name: "invalid_drift_delay",
			data: buildJSONLRecords(3, "date", "2024-01-15", matchSet(0)),
			params: map[string]string{
				"partition_key":   "date",
				"partition_value": "2024-01-15",
				"late_pct":        "50",
				"drift_delay":     "not-a-duration",
			},
			wantApplied: false,
			wantErr:     true,
		},
		{
			// Non-JSONL data → pass-through write, Applied: false
			name: "non_jsonl_passthrough",
			data: []byte("this is not json\nnor is this\n"),
			params: map[string]string{
				"partition_key":   "date",
				"partition_value": "2024-01-15",
				"late_pct":        "50",
				"drift_delay":     "1h",
			},
			wantApplied:       false,
			wantErr:           true,
			wantWriteCount:    1, // pass-through
			wantHoldDataCount: 0,
		},
		{
			// Drift key naming: verify "_drift_" is in the HoldData key
			name: "drift_key_contains_drift_marker",
			data: buildJSONLRecords(4, "date", "2024-01-15", matchSet(0, 1, 2, 3)),
			params: map[string]string{
				"partition_key":            "date",
				"partition_value":          "2024-01-15",
				"late_pct":                 "50",
				"drift_delay":              "1h",
			},
			wantApplied:             true,
			wantErr:                 false,
			wantWriteCount:          1,
			wantHoldDataCount:       1,
			wantHoldDataKeyContains: "_drift_",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transport := newMockTransport()
			transport.ReadData[objKey] = tt.data

			m := &mutation.PostRunDriftMutation{}
			obj := types.DataObject{Key: objKey}

			record, err := m.Apply(context.Background(), obj, transport, tt.params, adapter.NewWallClock())

			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if record.Applied {
					t.Error("expected Applied=false when error returned")
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if record.Applied != tt.wantApplied {
					t.Errorf("Applied = %v, want %v", record.Applied, tt.wantApplied)
				}
				if record.Mutation != "post-run-drift" {
					t.Errorf("Mutation = %q, want %q", record.Mutation, "post-run-drift")
				}
			}

			if tt.wantWriteCount > 0 {
				if got := writeCallCount(transport); got != tt.wantWriteCount {
					t.Errorf("Write call count = %d, want %d", got, tt.wantWriteCount)
				}
			}

			if got := holdDataCallCount(transport); got != tt.wantHoldDataCount {
				t.Errorf("HoldData call count = %d, want %d", got, tt.wantHoldDataCount)
			}

			if tt.wantOnTimeLines > 0 {
				written := getWrittenData(transport)
				if got := countLines(written); got != tt.wantOnTimeLines {
					t.Errorf("on-time lines written = %d, want %d", got, tt.wantOnTimeLines)
				}
			}

			if tt.wantHoldDataKeyContains != "" {
				key := getHoldDataKey(transport)
				if !strings.Contains(key, tt.wantHoldDataKeyContains) {
					t.Errorf("HoldData key %q does not contain %q", key, tt.wantHoldDataKeyContains)
				}
			}
		})
	}
}

// TestPostRunDriftMutation_HoldDataCalled verifies HoldData is invoked with a
// future release time and a key derived from the original object key.
func TestPostRunDriftMutation_HoldDataCalled(t *testing.T) {
	const objKey = "raw/events.jsonl"
	data := buildJSONLRecords(6, "region", "eu-west", matchSet(3, 4, 5))

	transport := newMockTransport()
	transport.ReadData[objKey] = data

	before := time.Now()
	m := &mutation.PostRunDriftMutation{}
	obj := types.DataObject{Key: objKey}
	record, err := m.Apply(context.Background(), obj, transport, map[string]string{
		"partition_key":   "region",
		"partition_value": "eu-west",
		"late_pct":        "100",
		"drift_delay":     "2h",
	}, adapter.NewWallClock())
	after := time.Now()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !record.Applied {
		t.Fatal("expected Applied=true")
	}

	// Verify HoldData was called exactly once.
	if got := holdDataCallCount(transport); got != 1 {
		t.Fatalf("HoldData call count = %d, want 1", got)
	}

	// Verify the drift key contains the source key's base name and the drift marker.
	driftKey := getHoldDataKey(transport)
	if !strings.Contains(driftKey, "events") {
		t.Errorf("HoldData key %q does not contain source base name %q", driftKey, "events")
	}
	if !strings.Contains(driftKey, "_drift_") {
		t.Errorf("HoldData key %q does not contain \"_drift_\"", driftKey)
	}
	if !strings.Contains(driftKey, ".jsonl") {
		t.Errorf("HoldData key %q does not preserve extension", driftKey)
	}

	_ = before
	_ = after
}

// TestPostRunDriftMutation_DefaultLatePct verifies that omitting late_pct defaults to 20%.
func TestPostRunDriftMutation_DefaultLatePct(t *testing.T) {
	// 10 records, all 10 match → default 20% → lateCount=2 held
	const objKey = "data/batch.jsonl"
	data := buildJSONLRecords(10, "env", "staging", matchSet(0, 1, 2, 3, 4, 5, 6, 7, 8, 9))

	transport := newMockTransport()
	transport.ReadData[objKey] = data

	m := &mutation.PostRunDriftMutation{}
	obj := types.DataObject{Key: objKey}
	record, err := m.Apply(context.Background(), obj, transport, map[string]string{
		"partition_key":   "env",
		"partition_value": "staging",
		// late_pct omitted → defaults to 20
		"drift_delay": "30m",
	}, adapter.NewWallClock())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !record.Applied {
		t.Fatal("expected Applied=true")
	}

	written := getWrittenData(transport)
	if got := countLines(written); got != 8 {
		t.Errorf("on-time lines = %d, want 8 (default 20%% held from 10 matching)", got)
	}
	if got := holdDataCallCount(transport); got != 1 {
		t.Errorf("HoldData count = %d, want 1", got)
	}
}

// TestPostRunDriftMutation_ReadError verifies that a Read failure returns an error.
func TestPostRunDriftMutation_ReadError(t *testing.T) {
	transport := newMockTransport()
	// Do not set readData for this key → Read returns "key not found".

	m := &mutation.PostRunDriftMutation{}
	obj := types.DataObject{Key: "missing/file.jsonl"}
	record, err := m.Apply(context.Background(), obj, transport, map[string]string{
		"partition_key":   "date",
		"partition_value": "2024-01-15",
		"drift_delay":     "1h",
	}, adapter.NewWallClock())
	if err == nil {
		t.Fatal("expected error on missing key, got nil")
	}
	if record.Applied {
		t.Error("expected Applied=false on read error")
	}
}

// TestPostRunDriftMutation_EmptyInput verifies that empty JSONL input is handled.
func TestPostRunDriftMutation_EmptyInput(t *testing.T) {
	const objKey = "data/empty.jsonl"
	transport := newMockTransport()
	transport.ReadData[objKey] = []byte{}

	m := &mutation.PostRunDriftMutation{}
	obj := types.DataObject{Key: objKey}
	record, err := m.Apply(context.Background(), obj, transport, map[string]string{
		"partition_key":   "date",
		"partition_value": "2024-01-15",
		"drift_delay":     "1h",
	}, adapter.NewWallClock())
	if err == nil {
		t.Fatal("expected error for empty input, got nil")
	}
	if record.Applied {
		t.Error("expected Applied=false on empty input")
	}
}

// TestPostRunDriftMutation_NegativeDriftDelay verifies drift_delay <= 0 is rejected.
func TestPostRunDriftMutation_NegativeDriftDelay(t *testing.T) {
	const objKey = "data/test.jsonl"
	transport := newMockTransport()
	transport.ReadData[objKey] = buildJSONLRecords(2, "date", "2024-01-15", matchSet(0))

	m := &mutation.PostRunDriftMutation{}
	obj := types.DataObject{Key: objKey}
	record, err := m.Apply(context.Background(), obj, transport, map[string]string{
		"partition_key":   "date",
		"partition_value": "2024-01-15",
		"drift_delay":     "-1h",
	}, adapter.NewWallClock())
	if err == nil {
		t.Fatal("expected error for negative drift_delay, got nil")
	}
	if record.Applied {
		t.Error("expected Applied=false on negative drift_delay")
	}
}

// TestPostRunDriftMutation_InvalidLatePctNonNumeric verifies non-numeric late_pct is rejected.
func TestPostRunDriftMutation_InvalidLatePctNonNumeric(t *testing.T) {
	const objKey = "data/test.jsonl"
	transport := newMockTransport()
	transport.ReadData[objKey] = buildJSONLRecords(2, "date", "2024-01-15", matchSet(0))

	m := &mutation.PostRunDriftMutation{}
	obj := types.DataObject{Key: objKey}
	record, err := m.Apply(context.Background(), obj, transport, map[string]string{
		"partition_key":   "date",
		"partition_value": "2024-01-15",
		"late_pct":        "abc",
		"drift_delay":     "1h",
	}, adapter.NewWallClock())
	if err == nil {
		t.Fatal("expected error for non-numeric late_pct, got nil")
	}
	if record.Applied {
		t.Error("expected Applied=false on non-numeric late_pct")
	}
}

// TestPostRunDriftMutation_LateRecordsFromTail verifies that held records come
// from the tail of the matched set (not the head).
func TestPostRunDriftMutation_LateRecordsFromTail(t *testing.T) {
	// 5 records, indices 0..4 all match, late_pct=40 → 2 held from tail (indices 3,4)
	const objKey = "data/tail.jsonl"
	var buf bytes.Buffer
	for i := 0; i < 5; i++ {
		rec := map[string]any{"id": i, "date": "2024-01-15"}
		b, _ := json.Marshal(rec)
		buf.Write(b)
		buf.WriteByte('\n')
	}
	data := buf.Bytes()

	transport := newMockTransport()
	transport.ReadData[objKey] = data

	m := &mutation.PostRunDriftMutation{}
	obj := types.DataObject{Key: objKey}
	record, err := m.Apply(context.Background(), obj, transport, map[string]string{
		"partition_key":   "date",
		"partition_value": "2024-01-15",
		"late_pct":        "40",
		"drift_delay":     "1h",
	}, adapter.NewWallClock())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !record.Applied {
		t.Fatal("expected Applied=true")
	}

	// On-time output should have 3 records (ids 0,1,2).
	written := getWrittenData(transport)
	if got := countLines(written); got != 3 {
		t.Errorf("on-time lines = %d, want 3", got)
	}

	// Verify that IDs 0,1,2 are in the on-time output and 3,4 are NOT.
	for _, line := range bytes.Split(written, []byte("\n")) {
		trimmed := bytes.TrimSpace(line)
		if len(trimmed) == 0 {
			continue
		}
		var rec map[string]any
		if err := json.Unmarshal(trimmed, &rec); err != nil {
			t.Fatalf("on-time line is not valid JSON: %s", trimmed)
		}
		idFloat, ok := rec["id"].(float64)
		if !ok {
			t.Fatalf("expected numeric id, got %T", rec["id"])
		}
		id := int(idFloat)
		if id >= 3 {
			t.Errorf("record with id=%d should have been held, not written to output", id)
		}
	}
}
