package referential

import (
	"context"
	"testing"
)

func TestReferentialGenerator_Category(t *testing.T) {
	gen := &ReferentialGenerator{}
	if gen.Category() != "referential" {
		t.Errorf("expected 'referential', got '%s'", gen.Category())
	}
}

func TestReferentialGenerator_Generate(t *testing.T) {
	gen := &ReferentialGenerator{}
	vals, err := gen.Generate(context.Background())
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
	for _, v := range vals {
		found[v.Description()] = true
	}

	for _, desc := range expectedDesc {
		if !found[desc] {
			t.Errorf("Missing expected chaos value with description: %s", desc)
		}
	}
}
