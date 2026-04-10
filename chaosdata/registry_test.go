package chaosdata

import (
	"testing"
)

type mockGenerator struct {
	name     string
	category string
}

func (m *mockGenerator) Name() string     { return m.name }
func (m *mockGenerator) Category() string { return m.category }
func (m *mockGenerator) Generate(opts GenerateOpts) (Payload, error) {
	return Payload{}, nil
}

func TestRegistry(t *testing.T) {
	// Reset registry for clean test
	mu.Lock()
	registry = nil
	mu.Unlock()

	g1 := &mockGenerator{name: "gen1", category: "cat1"}
	g2 := &mockGenerator{name: "gen2", category: "cat1"}
	g3 := &mockGenerator{name: "gen3", category: "cat2"}

	Register(g1)
	Register(g2)
	Register(g3)

	all := All()
	if len(all) != 3 {
		t.Errorf("Expected 3 generators, got %d", len(all))
	}

	cat1 := ByCategory("cat1")
	if len(cat1) != 2 {
		t.Errorf("Expected 2 generators in cat1, got %d", len(cat1))
	}

	cat2 := ByCategory("cat2")
	if len(cat2) != 1 {
		t.Errorf("Expected 1 generator in cat2, got %d", len(cat2))
	}

	cat3 := ByCategory("cat3")
	if len(cat3) != 0 {
		t.Errorf("Expected 0 generators in cat3, got %d", len(cat3))
	}
}
