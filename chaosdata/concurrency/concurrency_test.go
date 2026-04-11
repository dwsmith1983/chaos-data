package concurrency

import (
	"context"
	"sync"
	"testing"
)

func TestConcurrencyGenerator_Category(t *testing.T) {
	gen := &ConcurrencyGenerator{}
	if gen.Category() != "concurrency" {
		t.Errorf("expected 'concurrency', got '%s'", gen.Category())
	}
}

func TestConcurrencyGenerator_Generate(t *testing.T) {
	gen := &ConcurrencyGenerator{}
	vals, err := gen.Generate(context.Background())
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
	for _, v := range vals {
		found[v.Description()] = true
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
			_, err := gen.Generate(context.Background())
			if err != nil {
				t.Errorf("Concurrent Generate() failed: %v", err)
			}
		}()
	}
	wg.Wait()
}
