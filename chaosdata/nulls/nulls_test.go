package nulls

import (
	"encoding/json"
	"testing"

	"github.com/dwsmith1983/chaos-data/chaosdata"
)

func TestNullsGenerator_Category(t *testing.T) {
	gen := &NullsGenerator{}
	if gen.Category() != "nulls" {
		t.Errorf("expected 'nulls', got '%s'", gen.Category())
	}
}

func TestNullsGenerator_Generate(t *testing.T) {
	gen := &NullsGenerator{}
	vals, err := gen.Generate(chaosdata.GenerateOpts{Count: 1})
	if err != nil {
		t.Fatalf("Generate() err = %v", err)
	}

	expectedDesc := []string{
		"nil",
		"empty string",
		"zero-length slice",
		"zero-length map",
		"string literal null",
		"string literal NULL",
		"string literal nil",
		"string literal None",
		"string literal undefined",
		"Unicode null",
		"null byte in middle of string",
		"sql.NullString Valid=false",
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
