package referential

import (
	"encoding/json"
	"testing"

	"github.com/dwsmith1983/chaos-data/chaosdata"
)

func TestReferentialGenerator_Category(t *testing.T) {
	gen := &ReferentialGenerator{}
	if gen.Category() != "referential" {
		t.Errorf("expected 'referential', got '%s'", gen.Category())
	}
}

func TestReferentialGenerator_Generate(t *testing.T) {
	gen := &ReferentialGenerator{}
	vals, err := gen.Generate(chaosdata.GenerateOpts{Count: 1})
	if err != nil {
		t.Fatalf("Generate() err = %v", err)
	}

	expectedDesc := []string{
		"Dangling reference ID (UUID pointing to nothing)",
		"Self-referential ID",
		"Duplicate IDs",
		"Empty/zero-value ID",
		"ID with wrong type (string vs int)",
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
