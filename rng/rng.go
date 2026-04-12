package rng

import (
	"math/rand/v2"
)

// New creates a new PRNG using PCG source with the given seed.
// It uses math/rand/v2 and a PCG source for better statistical properties
// and determinism across platforms.
func New(seed int64) *rand.Rand {
	// PCG requires two 128-bit state components. We derive them from the 64-bit seed.
	return rand.New(rand.NewPCG(uint64(seed), uint64(seed)^0x5851f42d4c957f2d))
}

// DeriveChild creates a new PRNG from an existing one deterministically.
// This allows for hierarchical PRNG structures where children are independent
// but their sequences are fully determined by the parent's state at derivation time.
func DeriveChild(parent *rand.Rand) *rand.Rand {
	seed1 := parent.Uint64()
	seed2 := parent.Uint64()
	return rand.New(rand.NewPCG(seed1, seed2))
}
