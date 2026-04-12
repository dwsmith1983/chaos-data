package rng

import (
	"testing"
)

func TestNew_Determinism(t *testing.T) {
	seed := int64(42)
	r1 := New(seed)
	r2 := New(seed)

	for i := 0; i < 100; i++ {
		v1 := r1.Uint64()
		v2 := r2.Uint64()
		if v1 != v2 {
			t.Fatalf("PRNGs with same seed produced different values at index %d: %d != %d", i, v1, v2)
		}
	}
}

func TestDeriveChild_Determinism(t *testing.T) {
	seed := int64(123)
	parent1 := New(seed)
	parent2 := New(seed)

	child1 := DeriveChild(parent1)
	child2 := DeriveChild(parent2)

	for i := 0; i < 100; i++ {
		v1 := child1.Uint64()
		v2 := child2.Uint64()
		if v1 != v2 {
			t.Fatalf("Derived children with same parent state produced different values at index %d: %d != %d", i, v1, v2)
		}
	}
}

func TestDeriveChild_DistinctChildren(t *testing.T) {
	parent := New(99)
	child1 := DeriveChild(parent)
	child2 := DeriveChild(parent)

	// Since child1 and child2 are derived sequentially from the same parent,
	// they should have different seeds and thus different sequences.
	different := false
	for i := 0; i < 10; i++ {
		if child1.Uint64() != child2.Uint64() {
			different = true
			break
		}
	}
	if !different {
		t.Error("Derived children produced identical sequences")
	}
}

func TestDeriveChild_Isolation(t *testing.T) {
	parent := New(100)
	
	child1 := DeriveChild(parent)
	child2 := DeriveChild(parent)
	
	// Record first value of child2
	val2_orig := child2.Uint64()
	
	// Create another child2 from a fresh parent advanced to the same point
	parentRef := New(100)
	_ = DeriveChild(parentRef) // child1 derivation
	child2_ref := DeriveChild(parentRef)
	val2_ref := child2_ref.Uint64()
	
	if val2_orig != val2_ref {
		t.Fatal("Setup failure: child2_ref does not match child2")
	}

	// Use child1 extensively
	for i := 0; i < 100; i++ {
		child1.Uint64()
	}

	// Verify child2 is unaffected
	val2_post := child2.Uint64()
	if val2_post != child2_ref.Uint64() {
		t.Error("Usage of child1 affected child2 sequence")
	}
}

func TestDeriveChild_ParentIsolation(t *testing.T) {
    parent := New(101)
    
    // Derive a child
    child := DeriveChild(parent)
    
    // Using child should not affect parent
    parentValBefore := parent.Uint64()
    _ = child.Uint64()
    parentValAfter := parent.Uint64()
    
    // This is a bit tricky because parent.Uint64() advances parent.
    // So we should compare parentValAfter with what it would be if child was not used.
    
    parent2 := New(101)
    _ = DeriveChild(parent2)
    expectedParentValAfter := parent2.Uint64()
    
    if parentValAfter != expectedParentValAfter {
        t.Errorf("Usage of child affected parent sequence: %d != %d", parentValAfter, expectedParentValAfter)
    }
}
