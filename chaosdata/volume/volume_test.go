package volume

import (
	"encoding/json"
	"testing"

	"github.com/dwsmith1983/chaos-data/chaosdata"
)

func TestVolumeGenerator_Category(t *testing.T) {
	gen := &VolumeGenerator{}
	if gen.Category() != "volume" {
		t.Errorf("expected 'volume', got '%s'", gen.Category())
	}
}

func TestVolumeGenerator_Generate(t *testing.T) {
	gen := &VolumeGenerator{}
	vals, err := gen.Generate(chaosdata.GenerateOpts{Count: 1})
	if err != nil {
		t.Fatalf("Generate() err = %v", err)
	}

	expectedDesc := []string{
		"1MB string (materialized)",
		"10MB payload (io.Reader)",
		"Slice with 1M elements",
		"Map with 100K keys",
		"Power-of-2 boundary strings",
		"Empty (0 byte) payload",
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
