package gospecific

import (
	"context"
	"testing"
)

func TestGoSpecificGenerator_Category(t *testing.T) {
	gen := &GoSpecificGenerator{}
	if gen.Category() != "gospecific" {
		t.Errorf("expected 'gospecific', got '%s'", gen.Category())
	}
}

func TestGoSpecificGenerator_Generate(t *testing.T) {
	gen := &GoSpecificGenerator{}
	vals, err := gen.Generate(context.Background())
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
	for _, v := range vals {
		found[v.Description()] = true
	}

	for _, desc := range expectedDesc {
		if !found[desc] {
			t.Errorf("Missing expected chaos value with description: %s", desc)
		}
	}
}
