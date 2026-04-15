package profiles

import (
	"testing"
	"time"
)

func TestSeededRand_Deterministic(t *testing.T) {
	r1 := NewSeededRand(42)
	r2 := NewSeededRand(42)

	for i := 0; i < 100; i++ {
		b1 := r1.ShouldInject(0.5)
		b2 := r2.ShouldInject(0.5)
		if b1 != b2 {
			t.Fatalf("mismatch at iteration %d", i)
		}
	}
}

func TestSeededRand_DifferentSeeds(t *testing.T) {
	r1 := NewSeededRand(42)
	r2 := NewSeededRand(43)

	matchCount := 0
	for i := 0; i < 100; i++ {
		if r1.ShouldInject(0.5) == r2.ShouldInject(0.5) {
			matchCount++
		}
	}
	if matchCount == 100 {
		t.Fatal("expected different sequences for different seeds")
	}
}

func TestSeededRand_ZeroSeedIsRandom(t *testing.T) {
	r1 := NewSeededRand(0)
	r2 := NewSeededRand(0)

	matchCount := 0
	for i := 0; i < 100; i++ {
		if r1.ShouldInject(0.5) == r2.ShouldInject(0.5) {
			matchCount++
		}
	}
	if matchCount == 100 {
		t.Fatal("expected different sequences for zero seed")
	}
}

func TestShouldInject_Rate0_NeverTrue(t *testing.T) {
	r := NewSeededRand(1)
	for i := 0; i < 1000; i++ {
		if r.ShouldInject(0.0) {
			t.Fatal("expected false for rate 0")
		}
	}
}

func TestShouldInject_Rate1_AlwaysTrue(t *testing.T) {
	r := NewSeededRand(1)
	for i := 0; i < 1000; i++ {
		if !r.ShouldInject(1.0) {
			t.Fatal("expected true for rate 1")
		}
	}
}

func TestPick_ReturnsElementFromSlice(t *testing.T) {
	r := NewSeededRand(1)
	items := []string{"a", "b", "c"}
	res := r.Pick(items)
	if res != "a" && res != "b" && res != "c" {
		t.Fatalf("expected one of %v, got %v", items, res)
	}
}

func TestJitter_WithinBounds(t *testing.T) {
	r := NewSeededRand(1)
	base := time.Now()
	maxJitter := 5 * time.Second

	for i := 0; i < 100; i++ {
		res := r.Jitter(base, maxJitter)
		diff := res.Sub(base)
		if diff < -maxJitter || diff > maxJitter {
			t.Fatalf("jittered time %v out of bounds (base %v, diff %v)", res, base, diff)
		}
	}
}

func TestShuffle_Permutation(t *testing.T) {
	r := NewSeededRand(1)
	n := 10
	perm := r.Shuffle(n)

	if len(perm) != n {
		t.Fatalf("expected len %d, got %d", n, len(perm))
	}

	seen := make(map[int]bool)
	for _, v := range perm {
		if v < 0 || v >= n {
			t.Fatalf("value %d out of bounds [0, %d)", v, n)
		}
		seen[v] = true
	}
	if len(seen) != n {
		t.Fatalf("not a valid permutation: %v", perm)
	}
}
