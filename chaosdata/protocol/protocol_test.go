package protocol

import (
	"context"
	"testing"
)

func TestProtocolGenerator_Category(t *testing.T) {
	gen := &ProtocolGenerator{}
	if gen.Category() != "protocol" {
		t.Errorf("expected 'protocol', got '%s'", gen.Category())
	}
}

func TestProtocolGenerator_Generate(t *testing.T) {
	gen := &ProtocolGenerator{}
	vals, err := gen.Generate(context.Background())
	if err != nil {
		t.Fatalf("Generate() err = %v", err)
	}

	expectedDesc := []string{
		"Malformed JSON (trailing comma)",
		"Malformed JSON (single quotes)",
		"Malformed JSON (unquoted keys)",
		"Malformed HTTP header (oversized)",
		"Malformed HTTP header (missing colon)",
		"Malformed HTTP header (null bytes)",
		"Invalid URL (missing scheme)",
		"Invalid URL (port 99999)",
		"Invalid URL (unicode host)",
		"gRPC/protobuf invalid field numbers",
		"gRPC/protobuf unknown field tags",
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
