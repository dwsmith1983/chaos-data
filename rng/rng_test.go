package rng

import (
	"testing"
)

func TestNewDeterminism(t *testing.T) {
	seed := int64(42)
	rng1 := New(seed)
	rng2 := New(seed)

	for i := 0; i < 100; i++ {
		v1 := rng1.Uint64()
		v2 := rng2.Uint64()
		if v1 != v2 {
			t.Fatalf("at index %d: expected %v, got %v", i, v1, v2)
		}
	}
}

func TestDeriveChildDeterminism(t *testing.T) {
	seed := int64(12345)
	
	// Create two identical parents
	parent1 := New(seed)
	parent2 := New(seed)
	
	// Derive children at the same state
	child1 := DeriveChild(parent1)
	child2 := DeriveChild(parent2)
	
	for i := 0; i < 100; i++ {
		v1 := child1.Uint64()
		v2 := child2.Uint64()
		if v1 != v2 {
			t.Fatalf("child mismatch at index %d: expected %v, got %v", i, v1, v2)
		}
	}
}

func TestIsolation(t *testing.T) {
	seed := int64(789)
	parent := New(seed)
	
	child1 := DeriveChild(parent)
	child2 := DeriveChild(parent)
	
	// Sequence from child2 before child1 is used
	_ = child2.Uint64()
	
	// Use child1 extensively
	for i := 0; i < 1000; i++ {
		child1.Uint64()
	}
	
	// Sequence from child2 after child1 is used
	v2_2 := child2.Uint64()
	
	// child2's internal state should not be affected by child1's usage
	// To verify this, we can recreate the same child2 from a fresh parent
	parentRef := New(seed)
	_ = DeriveChild(parentRef) // This would be child1
	child2Ref := DeriveChild(parentRef)
	
	_ = child2Ref.Uint64() // This corresponds to v2_1
	v2_2Ref := child2Ref.Uint64()
	
	if v2_2 != v2_2Ref {
		t.Errorf("child2 was affected by child1 usage: expected %v, got %v", v2_2Ref, v2_2)
	}
}

func TestDistinctChildren(t *testing.T) {
	parent := New(1)
	child1 := DeriveChild(parent)
	child2 := DeriveChild(parent)
	
	v1 := child1.Uint64()
	v2 := child2.Uint64()
	
	if v1 == v2 {
		t.Error("consecutive children should produce different sequences")
	}
}

func TestParentIndependence(t *testing.T) {
	parent := New(100)
	child := DeriveChild(parent)
	
	// Record parent state
	_ = parent.Uint64()
	
	// Use child
	child.Uint64()
	child.Uint64()
	
	// Parent should not be affected by child usage
	pv2 := parent.Uint64()
	
	// Reference parent
	parentRef := New(100)
	_ = DeriveChild(parentRef) // consumes 2 uint64s
	_ = parentRef.Uint64()     // this was pv1
	pv2Ref := parentRef.Uint64()
	
	if pv2 != pv2Ref {
		t.Errorf("parent was affected by child usage: expected %v, got %v", pv2Ref, pv2)
	}
}
