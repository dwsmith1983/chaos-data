package mutation_test

import (
	"context"
	"testing"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/mutation"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// BenchmarkRegistryGet measures the cost of looking up a mutation by name from
// a populated registry.
func BenchmarkRegistryGet(b *testing.B) {
	reg := mutation.NewRegistry()
	if err := reg.Register(&mutation.DelayMutation{}); err != nil {
		b.Fatalf("register delay: %v", err)
	}
	if err := reg.Register(&mutation.CorruptMutation{}); err != nil {
		b.Fatalf("register corrupt: %v", err)
	}
	if err := reg.Register(&mutation.DropMutation{}); err != nil {
		b.Fatalf("register drop: %v", err)
	}

	b.ResetTimer()
	for range b.N {
		_, err := reg.Get("delay")
		if err != nil {
			b.Fatalf("Get: %v", err)
		}
	}
}

// BenchmarkRegistryList measures the cost of listing all registered mutation
// names (including the sort).
func BenchmarkRegistryList(b *testing.B) {
	reg := mutation.NewRegistry()
	if err := reg.Register(&mutation.DelayMutation{}); err != nil {
		b.Fatalf("register delay: %v", err)
	}
	if err := reg.Register(&mutation.CorruptMutation{}); err != nil {
		b.Fatalf("register corrupt: %v", err)
	}
	if err := reg.Register(&mutation.DropMutation{}); err != nil {
		b.Fatalf("register drop: %v", err)
	}

	b.ResetTimer()
	for range b.N {
		names := reg.List()
		if len(names) != 3 {
			b.Fatalf("List() returned %d names, want 3", len(names))
		}
	}
}

// BenchmarkDelayApply measures applying the delay mutation with a mock
// transport that immediately succeeds on Hold.
func BenchmarkDelayApply(b *testing.B) {
	transport := newMockTransport()
	m := &mutation.DelayMutation{}
	obj := types.DataObject{Key: "data/bench.csv", Size: 1024}
	params := map[string]string{"duration": "10m", "release": "true"}
	ctx := context.Background()

	b.ResetTimer()
	for range b.N {
		record, err := m.Apply(ctx, obj, transport, params, adapter.NewWallClock())
		if err != nil {
			b.Fatalf("Apply: %v", err)
		}
		if !record.Applied {
			b.Fatal("expected Applied=true")
		}
	}
}

// BenchmarkCorruptApply measures applying the corrupt mutation against a small
// JSONL payload with a mock transport.
func BenchmarkCorruptApply(b *testing.B) {
	jsonlData := []byte(`{"sensor":"temp","value":42}
{"sensor":"humidity","value":85}
{"sensor":"pressure","value":1013}
{"sensor":"wind","value":12}
{"sensor":"rain","value":0}
`)
	transport := newMockTransport()
	transport.ReadData["data/bench.jsonl"] = jsonlData

	m := &mutation.CorruptMutation{}
	obj := types.DataObject{Key: "data/bench.jsonl", Size: int64(len(jsonlData))}
	params := map[string]string{"affected_pct": "20", "corruption_type": "null"}
	ctx := context.Background()

	b.ResetTimer()
	for range b.N {
		record, err := m.Apply(ctx, obj, transport, params, adapter.NewWallClock())
		if err != nil {
			b.Fatalf("Apply: %v", err)
		}
		if !record.Applied {
			b.Fatal("expected Applied=true")
		}
	}
}
