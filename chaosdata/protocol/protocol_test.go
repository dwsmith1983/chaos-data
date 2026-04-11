package protocol

import (
	"encoding/json"
	"testing"

	"github.com/dwsmith1983/chaos-data/chaosdata"
)

func TestProtocolGenerator_Category(t *testing.T) {
	gen := &ProtocolGenerator{}
	if gen.Category() != "protocol" {
		t.Errorf("expected 'protocol', got '%s'", gen.Category())
	}
}

func TestProtocolGenerator_Generate(t *testing.T) {
	gen := &ProtocolGenerator{}
	vals, err := gen.Generate(chaosdata.GenerateOpts{Count: 1})
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
