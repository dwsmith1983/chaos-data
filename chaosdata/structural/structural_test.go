package structural

import (
	"context"
	"testing"
)

func TestStructuralGenerator_Category(t *testing.T) {
	gen := &StructuralGenerator{}
	if gen.Category() != "structural" {
		t.Errorf("expected 'structural', got '%s'", gen.Category())
	}
}

func TestStructuralGenerator_Generate(t *testing.T) {
	gen := &StructuralGenerator{}
	vals, err := gen.Generate(context.Background())
	if err != nil {
		t.Fatalf("Generate() err = %v", err)
	}

	expectedDesc := []string{
		"Deeply nested map",
		"empty nested containers",
		"mixed-type slice",
		"single-element slice",
		"empty slice vs nil distinction",
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

func TestStructuralGenerator_MaxDepth(t *testing.T) {
	if MaxDepth != 20 {
		t.Errorf("MaxDepth should default to 20, got %d", MaxDepth)
	}
	
	original := MaxDepth
	MaxDepth = 5
	defer func() { MaxDepth = original }()
	
	t.Errorf("Not implemented: verify generator respects modified MaxDepth")
}
