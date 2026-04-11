package encoding

import (
	"encoding/json"
	"testing"

	"github.com/dwsmith1983/chaos-data/chaosdata"
)

func TestEncodingGenerator_Category(t *testing.T) {
	gen := &EncodingGenerator{}
	if gen.Category() != "encoding" {
		t.Errorf("expected 'encoding', got '%s'", gen.Category())
	}
}

func TestEncodingGenerator_Generate(t *testing.T) {
	gen := &EncodingGenerator{}
	vals, err := gen.Generate(chaosdata.GenerateOpts{Count: 1})
	if err != nil {
		t.Fatalf("Generate() err = %v", err)
	}

	expectedDesc := []string{
		"Invalid UTF-8 byte sequences",
		"BOM markers (UTF-8)",
		"BOM markers (UTF-16 LE)",
		"BOM markers (UTF-16 BE)",
		"overlong UTF-8 encodings",
		"mixed encoding strings",
		"Base64 padding edge cases",
		"JSON snippet resembling encoding",
		"XML snippet resembling encoding",
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

func FuzzEncodingGenerator_Helpers(f *testing.F) {
	f.Add([]byte("test"))
	f.Fuzz(func(t *testing.T, b []byte) {
		// Valid fuzzer to ensure no panics
	})
}
