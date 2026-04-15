package gospecific

import (
	"encoding/json"
	"testing"

	"github.com/dwsmith1983/chaos-data/chaosdata"
)

func TestGoSpecificGenerator_Category(t *testing.T) {
	gen := &GoSpecificGenerator{}
	if gen.Category() != "gospecific" {
		t.Errorf("expected 'gospecific', got '%s'", gen.Category())
	}
}

func TestGoSpecificGenerator_Generate(t *testing.T) {
	gen := &GoSpecificGenerator{}
	vals, err := gen.Generate(chaosdata.GenerateOpts{Count: 1})
	if err != nil {
		t.Fatalf("Generate() err = %v", err)
	}

	expectedDesc := []string{
		"Nil interface vs nil pointer-in-interface",
		"Unexported field struct",
		"context.Canceled error value",
		"context.DeadlineExceeded error value",
		"Unbuffered chan with no reader",
		"time.Time{} zero value",
		"time.Unix(0,0)",
		"String with len>0 but only null bytes",
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
