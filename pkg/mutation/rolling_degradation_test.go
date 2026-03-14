package mutation_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/dwsmith1983/chaos-data/pkg/mutation"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

func TestRollingDegradationMutation_Type(t *testing.T) {
	m := &mutation.RollingDegradationMutation{}
	if got := m.Type(); got != "rolling-degradation" {
		t.Errorf("Type() = %q, want %q", got, "rolling-degradation")
	}
}

func TestRollingDegradationMutation_Apply(t *testing.T) {
	// Generate 20 records with known fields.
	inputRecords := make([]map[string]interface{}, 20)
	for i := range 20 {
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
		wantApplied       bool
		wantErr           bool
		wantCorruptedPct  int
		wantWriteCount    int
	}{
		{
			name: "corrupts end_pct of records",
			params: map[string]string{
				"start_pct":     "10",
				"end_pct":       "50",
				"ramp_duration": "1h",
			},
			wantApplied:      true,
			wantCorruptedPct: 50,
			wantWriteCount:   1,
		},
		{
			name: "corrupts 100% of records",
			params: map[string]string{
				"start_pct":     "50",
				"end_pct":       "100",
				"ramp_duration": "30m",
			},
			wantApplied:      true,
			wantCorruptedPct: 100,
			wantWriteCount:   1,
		},
		{
			name: "corrupts 0% of records when end_pct is 0",
			params: map[string]string{
				"start_pct":     "0",
				"end_pct":       "0",
				"ramp_duration": "1h",
			},
			wantApplied:      true,
			wantCorruptedPct: 0,
			wantWriteCount:   1,
		},
		{
			name: "missing start_pct returns error",
			params: map[string]string{
				"end_pct":       "50",
				"ramp_duration": "1h",
			},
			wantApplied: false,
			wantErr:     true,
		},
		{
			name: "missing end_pct returns error",
			params: map[string]string{
				"start_pct":     "10",
				"ramp_duration": "1h",
			},
			wantApplied: false,
			wantErr:     true,
		},
		{
			name: "missing ramp_duration returns error",
			params: map[string]string{
				"start_pct": "10",
				"end_pct":   "50",
			},
			wantApplied: false,
			wantErr:     true,
		},
		{
			name: "invalid end_pct returns error",
			params: map[string]string{
				"start_pct":     "10",
				"end_pct":       "not-a-number",
				"ramp_duration": "1h",
			},
			wantApplied: false,
			wantErr:     true,
		},
		{
			name: "end_pct out of range returns error",
			params: map[string]string{
				"start_pct":     "10",
				"end_pct":       "150",
				"ramp_duration": "1h",
			},
			wantApplied: false,
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transport := newMockTransport()
			obj := types.DataObject{Key: "data/records.jsonl"}
			transport.readData[obj.Key] = inputData

			m := &mutation.RollingDegradationMutation{}
			record, err := m.Apply(context.Background(), obj, transport, tt.params)

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
			if record.Mutation != "rolling-degradation" {
				t.Errorf("Mutation = %q, want %q", record.Mutation, "rolling-degradation")
			}

			// Verify Write call count.
			if got := transport.callCount("Write"); got != tt.wantWriteCount {
				t.Errorf("Write call count = %d, want %d", got, tt.wantWriteCount)
			}

			if tt.wantWriteCount == 0 {
				return
			}

			// Parse the written data and verify corruption percentage.
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

			// Count records with a null field (corrupted).
			corruptedCount := 0
			for _, rec := range outputRecords {
				for _, v := range rec {
					if v == nil {
						corruptedCount++
						break
					}
				}
			}

			expectedCorrupted := (tt.wantCorruptedPct * len(inputRecords)) / 100
			if corruptedCount != expectedCorrupted {
				t.Errorf("corrupted record count = %d, want %d (%d%% of %d)",
					corruptedCount, expectedCorrupted, tt.wantCorruptedPct, len(inputRecords))
			}
		})
	}
}

func TestRollingDegradationMutation_ReadError(t *testing.T) {
	transport := newMockTransport()
	obj := types.DataObject{Key: "data/missing.jsonl"}
	// No readData set, so Read will return "key not found" error.

	m := &mutation.RollingDegradationMutation{}
	record, err := m.Apply(context.Background(), obj, transport, map[string]string{
		"start_pct":     "10",
		"end_pct":       "50",
		"ramp_duration": "1h",
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if record.Applied {
		t.Error("expected Applied=false on error")
	}
}

func TestRollingDegradationMutation_RampDurationRecorded(t *testing.T) {
	inputRecords := make([]map[string]interface{}, 10)
	for i := range 10 {
		inputRecords[i] = map[string]interface{}{
			"id":   float64(i),
			"name": fmt.Sprintf("record_%d", i),
		}
	}

	transport := newMockTransport()
	obj := types.DataObject{Key: "data/records.jsonl"}
	transport.readData[obj.Key] = makeJSONL(inputRecords)

	m := &mutation.RollingDegradationMutation{}
	record, err := m.Apply(context.Background(), obj, transport, map[string]string{
		"start_pct":     "10",
		"end_pct":       "50",
		"ramp_duration": "2h30m",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify ramp_duration is recorded in the params.
	if record.Params["ramp_duration"] != "2h30m" {
		t.Errorf("ramp_duration = %q, want %q", record.Params["ramp_duration"], "2h30m")
	}
}
