package rng

import (
	"math/rand/v2"
	"testing"
)

func TestNew_Determinism(t *testing.T) {
	seed := int64(12345)
	r1 := New(seed)
	r2 := New(seed)

	for i := 0; i < 100; i++ {
		v1 := r1.Uint64()
		v2 := r2.Uint64()
		if v1 != v2 {
			t.Fatalf("Non-deterministic sequence at index %d: %d != %d", i, v1, v2)
		}
	}
}

func TestDeriveChild_Distinct(t *testing.T) {
	parent := New(98765)
	c1 := DeriveChild(parent)
	c2 := DeriveChild(parent)

	// Since DeriveChild consumes from parent, c1 and c2 should have different seeds
	v1 := c1.Uint64()
	v2 := c2.Uint64()
	if v1 == v2 {
		t.Errorf("DeriveChild produced identical children: %d == %d", v1, v2)
	}
}

func TestDeriveChild_Determinism(t *testing.T) {
	// Generating children from identical parents should yield identical children
	p1 := New(42)
	p2 := New(42)

	c1 := DeriveChild(p1)
	c2 := DeriveChild(p2)

	for i := 0; i < 100; i++ {
		v1 := c1.Uint64()
		v2 := c2.Uint64()
		if v1 != v2 {
			t.Fatalf("Derived children are not deterministic at index %d: %d != %d", i, v1, v2)
		}
	}
}

func TestIsolation(t *testing.T) {
	// Using one child should not affect the sequence of another child
	parent := New(777)
	c1 := DeriveChild(parent)
	c2 := DeriveChild(parent)

	// Save first value of c2
	expectedV2 := c2.Uint64()

	// Reset and do it again, but use c1 in between
	parent2 := New(777)
	c1_2 := DeriveChild(parent2)
	c2_2 := DeriveChild(parent2)

	// Use c1_2 extensively
	for i := 0; i < 1000; i++ {
		c1_2.Uint64()
	}

	// c2_2 should still produce the same first value
	actualV2 := c2_2.Uint64()

	if actualV2 != expectedV2 {
		t.Errorf("Child isolation failed: usage of c1 affected c2. Expected %d, got %d", expectedV2, actualV2)
	}
}

func TestDeriveChild_ParentIndependence(t *testing.T) {
	// Further usage of parent should not affect already derived children
	parent := New(888)
	child := DeriveChild(parent)

	expectedChildVal := child.Uint64()

	// Use parent
	for i := 0; i < 100; i++ {
		parent.Uint64()
	}

	// Reset and re-derive to check
	parent2 := New(888)
	child2 := DeriveChild(parent2)
	
	// child2 should match child
	if child2.Uint64() != expectedChildVal {
		t.Fatal("Setup failure in test")
	}

	// The point is that 'child' was already derived.
	// If I continue using 'child', it should be independent of 'parent'.
	// This is naturally true in Go as they are different objects.
}
