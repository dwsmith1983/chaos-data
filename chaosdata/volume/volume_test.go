package volume

import (
	"context"
	"io"
	"testing"
)

func TestVolumeGenerator_Category(t *testing.T) {
	gen := &VolumeGenerator{}
	if gen.Category() != "volume" {
		t.Errorf("expected 'volume', got '%s'", gen.Category())
	}
}

func TestVolumeGenerator_Generate(t *testing.T) {
	gen := &VolumeGenerator{}
	vals, err := gen.Generate(context.Background())
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
	for _, v := range vals {
		found[v.Description()] = true
		
		if reader, ok := v.Value().(io.Reader); ok {
			buf := make([]byte, 1024)
			_, _ = reader.Read(buf)
		}
	}

	for _, desc := range expectedDesc {
		if !found[desc] {
			t.Errorf("Missing expected chaos value with description: %s", desc)
		}
	}
}
