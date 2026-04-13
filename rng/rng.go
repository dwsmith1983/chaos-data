package rng

import (
	"math/rand/v2"
)

// New creates a new PRNG using the PCG source with the given seed.
func New(seed int64) *rand.Rand {
	// NewPCG takes two uint64s. We use the seed for both to have a deterministic starting point.
	return rand.New(rand.NewPCG(uint64(seed), uint64(seed)))
}

// DeriveChild creates a new PRNG derived from the parent PRNG.
// It consumes values from the parent to seed the child, ensuring the child's
// sequence is deterministic based on the parent's current state.
func DeriveChild(parent *rand.Rand) *rand.Rand {
	s1 := parent.Uint64()
	s2 := parent.Uint64()
	return rand.New(rand.NewPCG(s1, s2))
}
