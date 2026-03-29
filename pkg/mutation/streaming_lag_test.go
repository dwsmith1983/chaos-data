package mutation_test

import (
	"context"
	"testing"
	"time"

	"github.com/dwsmith1983/chaos-data/pkg/mutation"
	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

func TestStreamingLagMutation_Type(t *testing.T) {
	s := &mutation.StreamingLagMutation{}
	if got := s.Type(); got != "streaming-lag" {
		t.Errorf("Type() = %q, want %q", got, "streaming-lag")
	}
}

func TestStreamingLagMutation_Apply(t *testing.T) {
	tests := []struct {
		name        string
		params      map[string]string
		wantApplied bool
		wantErr     bool
		wantHold    bool
		minDuration string
		maxDuration string
	}{
		{
			name: "basic streaming lag calls Hold",
			params: map[string]string{
				"lag_duration": "5s",
			},
			wantApplied: true,
			wantHold:    true,
			minDuration: "4s",
			maxDuration: "6s",
		},
		{
			name: "streaming lag with consumer_group",
			params: map[string]string{
				"lag_duration":   "10s",
				"consumer_group": "analytics-consumer",
			},
			wantApplied: true,
			wantHold:    true,
			minDuration: "9s",
			maxDuration: "11s",
		},
		{
			name:        "missing lag_duration returns error",
			params:      map[string]string{},
			wantApplied: false,
			wantErr:     true,
		},
		{
			name: "invalid lag_duration returns error",
			params: map[string]string{
				"lag_duration": "not-a-duration",
			},
			wantApplied: false,
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transport := newMockTransport()
			obj := types.DataObject{Key: "data/records.jsonl"}

			s := &mutation.StreamingLagMutation{}
			before := time.Now()
			record, err := s.Apply(context.Background(), obj, transport, tt.params, adapter.NewWallClock())
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
			if record.Mutation != "streaming-lag" {
				t.Errorf("Mutation = %q, want %q", record.Mutation, "streaming-lag")
			}

			// Verify Hold was called.
			if tt.wantHold {
				if got := transport.callCount("Hold"); got != 1 {
					t.Fatalf("Hold call count = %d, want 1", got)
				}
				calls := transport.getCalls()
				for _, c := range calls {
					if c.Method == "Hold" {
						if c.Key != obj.Key {
							t.Errorf("Hold key = %q, want %q", c.Key, obj.Key)
						}
						// Verify the Hold time is roughly now + lag_duration.
						minDur, _ := time.ParseDuration(tt.minDuration)
						maxDur, _ := time.ParseDuration(tt.maxDuration)
						earliestHold := before.Add(minDur)
						latestHold := after.Add(maxDur)
						if c.Until.Before(earliestHold) {
							t.Errorf("Hold time %v is before earliest expected %v", c.Until, earliestHold)
						}
						if c.Until.After(latestHold) {
							t.Errorf("Hold time %v is after latest expected %v", c.Until, latestHold)
						}
						break
					}
				}
			}

			// Verify consumer_group is recorded in params.
			if cg, ok := tt.params["consumer_group"]; ok {
				if record.Params["consumer_group"] != cg {
					t.Errorf("consumer_group = %q, want %q", record.Params["consumer_group"], cg)
				}
			}
		})
	}
}

func TestStreamingLagMutation_HoldError(t *testing.T) {
	transport := newMockTransport()
	obj := types.DataObject{Key: "data/records.jsonl"}
	transport.HoldErr[obj.Key] = errInjected

	s := &mutation.StreamingLagMutation{}
	record, err := s.Apply(context.Background(), obj, transport, map[string]string{
		"lag_duration": "5s",
	}, adapter.NewWallClock())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if record.Applied {
		t.Error("expected Applied=false on error")
	}
}
