package mutation_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/dwsmith1983/chaos-data/pkg/mutation"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

func TestCorruptMutation_Type(t *testing.T) {
	c := &mutation.CorruptMutation{}
	if got := c.Type(); got != "corrupt" {
		t.Errorf("Type() = %q, want %q", got, "corrupt")
	}
}

// makeJSONL creates JSONL content from a slice of maps.
func makeJSONL(records []map[string]interface{}) []byte {
	var buf bytes.Buffer
	for _, r := range records {
		line, _ := json.Marshal(r)
		buf.Write(line)
		buf.WriteByte('\n')
	}
	return buf.Bytes()
}

// parseJSONL parses JSONL content back into a slice of maps.
func parseJSONL(data []byte) ([]map[string]interface{}, error) {
	var records []map[string]interface{}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	for _, line := range lines {
		if line == "" {
			continue
		}
		var m map[string]interface{}
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			return nil, fmt.Errorf("unmarshal line %q: %w", line, err)
		}
		records = append(records, m)
	}
	return records, nil
}

func TestCorruptMutation_Apply(t *testing.T) {
	// Generate 10 records with known fields.
	inputRecords := make([]map[string]interface{}, 10)
	for i := range 10 {
		inputRecords[i] = map[string]interface{}{
			"id":    float64(i),
			"name":  fmt.Sprintf("record_%d", i),
			"value": float64(i * 100),
		}
	}
	inputData := makeJSONL(inputRecords)

	tests := []struct {
		name              string
		params            map[string]string
		readData          []byte
		readErr           error
		wantApplied       bool
		wantErr           bool
		wantCorruptedPct  int // expected percentage of corrupted records
		wantWriteCount    int // expected number of Write calls
		wantReadCount     int // expected number of Read calls
	}{
		{
			name: "default 10% corruption with null type",
			params: map[string]string{
				"corruption_type": "null",
			},
			readData:         inputData,
			wantApplied:      true,
			wantErr:          false,
			wantCorruptedPct: 10,
			wantWriteCount:   1,
			wantReadCount:    1,
		},
		{
			name: "50% corruption rate",
			params: map[string]string{
				"affected_pct":    "50",
				"corruption_type": "null",
			},
			readData:         inputData,
			wantApplied:      true,
			wantErr:          false,
			wantCorruptedPct: 50,
			wantWriteCount:   1,
			wantReadCount:    1,
		},
		{
			name: "100% corruption rate",
			params: map[string]string{
				"affected_pct":    "100",
				"corruption_type": "null",
			},
			readData:         inputData,
			wantApplied:      true,
			wantErr:          false,
			wantCorruptedPct: 100,
			wantWriteCount:   1,
			wantReadCount:    1,
		},
		{
			name:        "missing object returns error",
			params:      map[string]string{},
			readErr:     fmt.Errorf("object not found"),
			wantApplied: false,
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transport := newMockTransport()
			obj := types.DataObject{Key: "test/data.jsonl"}

			if tt.readData != nil {
				transport.readData[obj.Key] = tt.readData
			}
			if tt.readErr != nil {
				transport.readErr[obj.Key] = tt.readErr
			}

			c := &mutation.CorruptMutation{}
			record, err := c.Apply(context.Background(), obj, transport, tt.params)

			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if record.Applied {
					t.Error("expected Applied=false on error")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if record.Applied != tt.wantApplied {
				t.Errorf("Applied = %v, want %v", record.Applied, tt.wantApplied)
			}
			if record.Mutation != "corrupt" {
				t.Errorf("Mutation = %q, want %q", record.Mutation, "corrupt")
			}

			// Verify Read and Write call counts.
			if got := transport.callCount("Read"); got != tt.wantReadCount {
				t.Errorf("Read call count = %d, want %d", got, tt.wantReadCount)
			}
			if got := transport.callCount("Write"); got != tt.wantWriteCount {
				t.Errorf("Write call count = %d, want %d", got, tt.wantWriteCount)
			}

			// Parse the written data and verify corruption.
			calls := transport.getCalls()
			var writeData []byte
			for _, c := range calls {
				if c.Method == "Write" {
					writeData = c.Data
					break
				}
			}
			if writeData == nil {
				t.Fatal("no Write call found")
			}

			outputRecords, parseErr := parseJSONL(writeData)
			if parseErr != nil {
				t.Fatalf("failed to parse written JSONL: %v", parseErr)
			}
			if len(outputRecords) != len(inputRecords) {
				t.Fatalf("output record count = %d, want %d", len(outputRecords), len(inputRecords))
			}

			// Count how many records have a null field.
			corruptedCount := 0
			for i, rec := range outputRecords {
				hasNull := false
				for k, v := range rec {
					if v == nil {
						hasNull = true
						// Verify the original had a non-nil value for this field.
						if inputRecords[i][k] == nil {
							t.Errorf("record %d field %q was already nil in input", i, k)
						}
						break
					}
				}
				if hasNull {
					corruptedCount++
				}
			}

			expectedCorrupted := (tt.wantCorruptedPct * len(inputRecords)) / 100
			if corruptedCount != expectedCorrupted {
				t.Errorf("corrupted record count = %d, want %d (%d%% of %d)",
					corruptedCount, expectedCorrupted, tt.wantCorruptedPct, len(inputRecords))
			}

			// Verify uncorrupted records are unchanged.
			for i, rec := range outputRecords {
				hasNull := false
				for _, v := range rec {
					if v == nil {
						hasNull = true
						break
					}
				}
				if !hasNull {
					// This record should be identical to the input.
					for k, v := range inputRecords[i] {
						gotV, ok := rec[k]
						if !ok {
							t.Errorf("uncorrupted record %d missing field %q", i, k)
						}
						if fmt.Sprintf("%v", gotV) != fmt.Sprintf("%v", v) {
							t.Errorf("uncorrupted record %d field %q = %v, want %v", i, k, gotV, v)
						}
					}
				}
			}
		})
	}
}

func TestCorruptMutation_NullCorruption(t *testing.T) {
	// Test with a single record and 100% corruption to verify null behavior.
	input := makeJSONL([]map[string]interface{}{
		{"name": "Alice", "age": float64(30), "email": "alice@test.com"},
	})

	transport := newMockTransport()
	obj := types.DataObject{Key: "test/single.jsonl"}
	transport.readData[obj.Key] = input

	c := &mutation.CorruptMutation{}
	params := map[string]string{
		"affected_pct":    "100",
		"corruption_type": "null",
	}

	_, err := c.Apply(context.Background(), obj, transport, params)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Parse written data.
	calls := transport.getCalls()
	var writeData []byte
	for _, cl := range calls {
		if cl.Method == "Write" {
			writeData = cl.Data
			break
		}
	}

	records, parseErr := parseJSONL(writeData)
	if parseErr != nil {
		t.Fatalf("failed to parse JSONL: %v", parseErr)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}

	// Exactly one field should be nil.
	nilCount := 0
	for _, v := range records[0] {
		if v == nil {
			nilCount++
		}
	}
	if nilCount != 1 {
		t.Errorf("expected exactly 1 nil field, got %d in record: %v", nilCount, records[0])
	}
}

func TestCorruptMutation_ZeroPctNoCorruption(t *testing.T) {
	inputRecords := []map[string]interface{}{
		{"id": float64(1), "name": "Alice"},
		{"id": float64(2), "name": "Bob"},
	}
	inputData := makeJSONL(inputRecords)

	transport := newMockTransport()
	obj := types.DataObject{Key: "test/zero.jsonl"}
	transport.readData[obj.Key] = inputData

	c := &mutation.CorruptMutation{}
	record, err := c.Apply(context.Background(), obj, transport, map[string]string{
		"affected_pct": "0",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !record.Applied {
		t.Error("expected Applied=true")
	}

	// Parse written data and verify no records are corrupted.
	calls := transport.getCalls()
	var writeData []byte
	for _, cl := range calls {
		if cl.Method == "Write" {
			writeData = cl.Data
			break
		}
	}
	outputRecords, parseErr := parseJSONL(writeData)
	if parseErr != nil {
		t.Fatalf("failed to parse JSONL: %v", parseErr)
	}
	for i, rec := range outputRecords {
		for _, v := range rec {
			if v == nil {
				t.Errorf("record %d has unexpected nil field", i)
			}
		}
	}
}

func TestCorruptMutation_UnsupportedCorruptionType(t *testing.T) {
	inputData := makeJSONL([]map[string]interface{}{
		{"id": float64(1), "name": "Alice"},
	})

	transport := newMockTransport()
	obj := types.DataObject{Key: "test/unsupported.jsonl"}
	transport.readData[obj.Key] = inputData

	c := &mutation.CorruptMutation{}
	record, err := c.Apply(context.Background(), obj, transport, map[string]string{
		"corruption_type": "shuffle",
	})
	if err == nil {
		t.Fatal("expected error for unsupported corruption type, got nil")
	}
	if record.Applied {
		t.Error("expected Applied=false")
	}
	if !strings.Contains(err.Error(), "unsupported corruption type") {
		t.Errorf("error should mention unsupported corruption type, got: %v", err)
	}
}

func TestCorruptMutation_EmptyJSONLBody(t *testing.T) {
	transport := newMockTransport()
	obj := types.DataObject{Key: "test/empty.jsonl"}
	transport.readData[obj.Key] = []byte{}

	c := &mutation.CorruptMutation{}
	record, err := c.Apply(context.Background(), obj, transport, map[string]string{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !record.Applied {
		t.Error("expected Applied=true")
	}

	// Verify Write was called (even with empty content).
	if got := transport.callCount("Write"); got != 1 {
		t.Errorf("Write call count = %d, want 1", got)
	}
}
