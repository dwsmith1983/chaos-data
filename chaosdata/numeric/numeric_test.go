package numeric

import (
	"context"
	"testing"
)

func TestNumericGenerator_Category(t *testing.T) {
	gen := &NumericGenerator{}
	if gen.Category() != "numeric" {
		t.Errorf("expected category 'numeric', got '%s'", gen.Category())
	}
}

func TestNumericGenerator_Generate(t *testing.T) {
	gen := &NumericGenerator{}
	vals, err := gen.Generate(context.Background())
	if err != nil {
		t.Fatalf("Generate returned error: %v", err)
	}

	descriptions := map[string]bool{
		"Zero": false,
		"Negative Zero": false,
		"MaxInt64": false,
		"MinInt64": false,
		"MaxFloat64": false,
		"SmallestNonzeroFloat64": false,
		"NaN": false,
		"+Inf": false,
		"-Inf": false,
		"MaxInt32+1": false,
		"High-precision float": false,
	}

	for _, v := range vals {
		descriptions[v.Description()] = true
	}

	for desc, found := range descriptions {
		if !found {
			t.Errorf("Missing expected chaos value: %s", desc)
		}
	}
}

func FuzzNumericGenerator(f *testing.F) {
	f.Add(int64(0))
	f.Fuzz(func(t *testing.T, n int64) {
		t.Errorf("Not implemented: call boundary helper with fuzzed input to ensure no panics")
	})
}
