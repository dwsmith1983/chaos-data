package mutation_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/dwsmith1983/chaos-data/pkg/mutation"
	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

func TestDelayMutation_Type(t *testing.T) {
	d := &mutation.DelayMutation{}
	if got := d.Type(); got != "delay" {
		t.Errorf("Type() = %q, want %q", got, "delay")
	}
}

func TestDelayMutation_Apply(t *testing.T) {
	tests := []struct {
		name           string
		params         map[string]string
		wantMethod     string // expected transport method called
		wantApplied    bool
		wantErr        bool
		checkTimeRange bool   // whether to validate the Hold time
		minDuration    string // minimum expected duration for Hold
		maxDuration    string // maximum expected duration for Hold
	}{
		{
			name: "basic delay calls Hold",
			params: map[string]string{
				"duration": "5s",
			},
			wantMethod:     "Hold",
			wantApplied:    true,
			wantErr:        false,
			checkTimeRange: true,
			minDuration:    "4s",
			maxDuration:    "6s",
		},
		{
			name: "delay with jitter calls Hold within range",
			params: map[string]string{
				"duration": "5s",
				"jitter":   "2s",
			},
			wantMethod:     "Hold",
			wantApplied:    true,
			wantErr:        false,
			checkTimeRange: true,
			minDuration:    "4s",
			maxDuration:    "8s",
		},
		{
			name: "release false calls Delete instead of Hold",
			params: map[string]string{
				"duration": "5s",
				"release":  "false",
			},
			wantMethod:  "Delete",
			wantApplied: true,
			wantErr:     false,
		},
		{
			name: "release true calls Hold",
			params: map[string]string{
				"duration": "5s",
				"release":  "true",
			},
			wantMethod:  "Hold",
			wantApplied: true,
			wantErr:     false,
		},
		{
			name:        "missing duration param returns error",
			params:      map[string]string{},
			wantApplied: false,
			wantErr:     true,
		},
		{
			name: "invalid duration param returns error",
			params: map[string]string{
				"duration": "not-a-duration",
			},
			wantApplied: false,
			wantErr:     true,
		},
		{
			name: "invalid jitter param returns error",
			params: map[string]string{
				"duration": "5s",
				"jitter":   "bad",
			},
			wantApplied: false,
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transport := newMockTransport()
			d := &mutation.DelayMutation{}
			obj := types.DataObject{Key: "test/data.csv"}

			before := time.Now()
			record, err := d.Apply(context.Background(), obj, transport, tt.params, adapter.NewWallClock())
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
			if record.Mutation != "delay" {
				t.Errorf("Mutation = %q, want %q", record.Mutation, "delay")
			}

			// Verify the correct transport method was called.
			calls := transport.getCalls()
			if len(calls) != 1 {
				t.Fatalf("expected 1 transport call, got %d", len(calls))
			}
			if calls[0].Method != tt.wantMethod {
				t.Errorf("transport method = %q, want %q", calls[0].Method, tt.wantMethod)
			}
			if calls[0].Key != obj.Key {
				t.Errorf("transport key = %q, want %q", calls[0].Key, obj.Key)
			}

			// Verify time range for Hold calls.
			if tt.checkTimeRange && tt.wantMethod == "Hold" {
				minDur, _ := time.ParseDuration(tt.minDuration)
				maxDur, _ := time.ParseDuration(tt.maxDuration)

				// The Hold time should be between before+minDur and after+maxDur.
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
		})
	}
}

func TestDelayMutation_HoldError(t *testing.T) {
	transport := newMockTransport()
	obj := types.DataObject{Key: "test/data.csv"}
	transport.HoldErr[obj.Key] = fmt.Errorf("storage unavailable")

	d := &mutation.DelayMutation{}
	record, err := d.Apply(context.Background(), obj, transport, map[string]string{
		"duration": "5s",
	}, adapter.NewWallClock())

	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if record.Applied {
		t.Error("expected Applied=false when Hold fails")
	}
	if record.Error == "" {
		t.Error("expected MutationRecord.Error to be populated")
	}
}

func TestDelayMutation_ZeroJitterNoPanic(t *testing.T) {
	transport := newMockTransport()
	d := &mutation.DelayMutation{}
	obj := types.DataObject{Key: "test/data.csv"}

	record, err := d.Apply(context.Background(), obj, transport, map[string]string{
		"duration": "1s",
		"jitter":   "0s",
	}, adapter.NewWallClock())

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !record.Applied {
		t.Error("expected Applied=true")
	}
}
