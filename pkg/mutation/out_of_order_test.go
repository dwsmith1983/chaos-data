package mutation_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/dwsmith1983/chaos-data/pkg/mutation"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

func TestOutOfOrderMutation_Type(t *testing.T) {
	m := &mutation.OutOfOrderMutation{}
	if got := m.Type(); got != "out-of-order" {
		t.Errorf("Type() = %q, want %q", got, "out-of-order")
	}
}

func TestOutOfOrderMutation_Apply(t *testing.T) {
	tests := []struct {
		name           string
		objKey         string
		params         map[string]string
		setupTransport func(t *mockTransport, key string)
		wantApplied    bool
		wantErr        bool
		wantHold       bool
		checkTimeRange bool
		minDuration    string
		maxDuration    string
	}{
		{
			name:   "older_partition_held",
			objKey: "par_hour=14/data.jsonl",
			params: map[string]string{
				"delay_older_by":  "1h",
				"partition_field": "par_hour",
				"older_value":     "14",
				"newer_value":     "15",
			},
			wantApplied:    true,
			wantErr:        false,
			wantHold:       true,
			checkTimeRange: true,
			minDuration:    "59m",
			maxDuration:    "61m",
		},
		{
			name:   "newer_partition_passthrough",
			objKey: "par_hour=15/data.jsonl",
			params: map[string]string{
				"delay_older_by":  "1h",
				"partition_field": "par_hour",
				"older_value":     "14",
				"newer_value":     "15",
			},
			wantApplied: false,
			wantErr:     false,
			wantHold:    false,
		},
		{
			name:   "unrelated_partition_passthrough",
			objKey: "par_hour=16/data.jsonl",
			params: map[string]string{
				"delay_older_by":  "1h",
				"partition_field": "par_hour",
				"older_value":     "14",
				"newer_value":     "15",
			},
			wantApplied: false,
			wantErr:     false,
			wantHold:    false,
		},
		{
			name:   "numeric_suffix_no_false_positive",
			objKey: "raw/par_hour=140/data.jsonl",
			params: map[string]string{
				"delay_older_by":  "1h",
				"partition_field": "par_hour",
				"older_value":     "14",
				"newer_value":     "15",
			},
			wantApplied: false,
			wantErr:     false,
			wantHold:    false,
		},
		{
			name:   "no_partition_passthrough",
			objKey: "data.jsonl",
			params: map[string]string{
				"delay_older_by":  "1h",
				"partition_field": "par_hour",
				"older_value":     "14",
				"newer_value":     "15",
			},
			wantApplied: false,
			wantErr:     false,
			wantHold:    false,
		},
		{
			name:   "missing_delay_older_by",
			objKey: "par_hour=14/data.jsonl",
			params: map[string]string{
				"partition_field": "par_hour",
				"older_value":     "14",
				"newer_value":     "15",
			},
			wantApplied: false,
			wantErr:     true,
		},
		{
			name:   "missing_partition_field",
			objKey: "par_hour=14/data.jsonl",
			params: map[string]string{
				"delay_older_by": "1h",
				"older_value":    "14",
				"newer_value":    "15",
			},
			wantApplied: false,
			wantErr:     true,
		},
		{
			name:   "missing_older_value",
			objKey: "par_hour=14/data.jsonl",
			params: map[string]string{
				"delay_older_by":  "1h",
				"partition_field": "par_hour",
				"newer_value":     "15",
			},
			wantApplied: false,
			wantErr:     true,
		},
		{
			name:   "missing_newer_value",
			objKey: "par_hour=14/data.jsonl",
			params: map[string]string{
				"delay_older_by":  "1h",
				"partition_field": "par_hour",
				"older_value":     "14",
			},
			wantApplied: false,
			wantErr:     true,
		},
		{
			name:   "invalid_duration",
			objKey: "par_hour=14/data.jsonl",
			params: map[string]string{
				"delay_older_by":  "bad",
				"partition_field": "par_hour",
				"older_value":     "14",
				"newer_value":     "15",
			},
			wantApplied: false,
			wantErr:     true,
		},
		{
			name:   "hold_error",
			objKey: "par_hour=14/data.jsonl",
			params: map[string]string{
				"delay_older_by":  "1h",
				"partition_field": "par_hour",
				"older_value":     "14",
				"newer_value":     "15",
			},
			setupTransport: func(t *mockTransport, key string) {
				t.holdErr[key] = fmt.Errorf("storage unavailable")
			},
			wantApplied: false,
			wantErr:     true,
			wantHold:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transport := newMockTransport()
			if tt.setupTransport != nil {
				tt.setupTransport(transport, tt.objKey)
			}

			m := &mutation.OutOfOrderMutation{}
			obj := types.DataObject{Key: tt.objKey}

			before := time.Now()
			record, err := m.Apply(context.Background(), obj, transport, tt.params)
			after := time.Now()

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
			if record.Mutation != "out-of-order" {
				t.Errorf("Mutation = %q, want %q", record.Mutation, "out-of-order")
			}

			holdCalls := transport.callCount("Hold")
			if tt.wantHold {
				if holdCalls != 1 {
					t.Fatalf("expected 1 Hold call, got %d", holdCalls)
				}
				calls := transport.getCalls()
				if calls[0].Key != tt.objKey {
					t.Errorf("Hold key = %q, want %q", calls[0].Key, tt.objKey)
				}
				if tt.checkTimeRange {
					minDur, _ := time.ParseDuration(tt.minDuration)
					maxDur, _ := time.ParseDuration(tt.maxDuration)
					earliestHold := before.Add(minDur)
					latestHold := after.Add(maxDur)
					holdTime := calls[0].Until
					if holdTime.Before(earliestHold) {
						t.Errorf("Hold time %v is before earliest expected %v", holdTime, earliestHold)
					}
					if holdTime.After(latestHold) {
						t.Errorf("Hold time %v is after latest expected %v", holdTime, latestHold)
					}
				}
			} else {
				if holdCalls != 0 {
					t.Errorf("expected no Hold calls, got %d", holdCalls)
				}
			}
		})
	}
}
