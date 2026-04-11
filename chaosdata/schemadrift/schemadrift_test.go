package schemadrift

import (
	"context"
	"testing"
)

func TestSchemaDriftGenerator_Category(t *testing.T) {
	gen := &SchemaDriftGenerator{}
	if gen.Category() != "schemadrift" {
		t.Errorf("expected 'schemadrift', got '%s'", gen.Category())
	}
}

func TestSchemaDriftGenerator_Generate(t *testing.T) {
	gen := &SchemaDriftGenerator{}
	vals, err := gen.Generate(context.Background())
	if err != nil {
		t.Fatalf("Generate() err = %v", err)
	}

	expectedDesc := []string{
		"Field added (extra key)",
		"Field removed (missing expected key)",
		"Field type changed (string where int expected)",
		"Field renamed (camelCase)",
		"Field renamed (snake_case)",
		"Field renamed (PascalCase)",
		"Array where object expected",
		"Object where array expected",
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
