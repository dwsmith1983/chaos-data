package concurrency

import (
	"encoding/json"
	"sync"
	"testing"

	"github.com/dwsmith1983/chaos-data/chaosdata"
)

func TestConcurrencyGenerator_Category(t *testing.T) {
	gen := &ConcurrencyGenerator{}
	if gen.Category() != "concurrency" {
		t.Errorf("expected 'concurrency', got '%s'", gen.Category())
	}
}

func TestConcurrencyGenerator_Generate(t *testing.T) {
	gen := &ConcurrencyGenerator{}
	vals, err := gen.Generate(chaosdata.GenerateOpts{Count: 1})
	if err != nil {
		t.Fatalf("Generate() err = %v", err)
	}

	expectedDesc := []string{
		"Shared mutable map",
		"Shared mutable slice",
		"Identical timestamps",
		"Overlapping ranges",
		"Race condition trigger value",
	}

	found := make(map[string]bool)
	var parsed []map[string]any
	if err := json.Unmarshal(vals.Data, &parsed); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}
	for _, v := range parsed {
		if typ, ok := v["type"].(string); ok {
			found[typ] = true
		}
	}

	for _, desc := range expectedDesc {
		if !found[desc] {
			t.Errorf("Missing expected chaos value with description: %s", desc)
		}
	}
}

func TestConcurrencyGenerator_ConcurrentGenerate(t *testing.T) {
	gen := &ConcurrencyGenerator{}
	var wg sync.WaitGroup

	// Must run with -race flag
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := gen.Generate(chaosdata.GenerateOpts{Count: 1})
			if err != nil {
				t.Errorf("Concurrent Generate() failed: %v", err)
			}
		}()
	}
	wg.Wait()
}
