package rng

import (
	"math/rand/v2"
)

// New creates a new PRNG using the PCG algorithm seeded with the provided value.
func New(seed int64) *rand.Rand {
	// PCG in math/rand/v2 requires two uint64 seeds.
	// We use the provided seed and a fixed constant to initialize it.
	return rand.New(rand.NewPCG(uint64(seed), 0x9E3779B97F4A7C15))
}

// DeriveChild creates a new PRNG derived from the parent PRNG.
// This allows for deterministic generation of child PRNGs.
func DeriveChild(parent *rand.Rand) *rand.Rand {
	s1 := parent.Uint64()
	s2 := parent.Uint64()
	return rand.New(rand.NewPCG(s1, s2))
}
