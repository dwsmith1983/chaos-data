package numeric

import (
	"encoding/json"
	"testing"

	"github.com/dwsmith1983/chaos-data/chaosdata"
)

func TestNumericGenerator_Category(t *testing.T) {
	gen := &NumericGenerator{}
	if gen.Category() != "numeric" {
		t.Errorf("expected category 'numeric', got '%s'", gen.Category())
	}
}

func TestNumericGenerator_Generate(t *testing.T) {
	gen := &NumericGenerator{}
	vals, err := gen.Generate(chaosdata.GenerateOpts{Count: 1})
	if err != nil {
		t.Fatalf("Generate returned error: %v", err)
	}

	descriptions := map[string]bool{
		"Zero":                   false,
		"Negative Zero":          false,
		"MaxInt64":               false,
		"MinInt64":               false,
		"MaxFloat64":             false,
		"SmallestNonzeroFloat64": false,
		"NaN":                    false,
		"+Inf":                   false,
		"-Inf":                   false,
		"MaxInt32+1":             false,
		"High-precision float":   false,
	}

	var parsed []map[string]any
	if err := json.Unmarshal(vals.Data, &parsed); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}
	for _, v := range parsed {
		if typ, ok := v["type"].(string); ok {
			descriptions[typ] = true
		}
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
		// Valid fuzzer to ensure no panics
	})
}
