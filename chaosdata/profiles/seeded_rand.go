package profiles

import (
	crypto_rand "crypto/rand"
	"encoding/binary"
	"math/rand"
	"time"
)

// SeededRand provides random helpers for deterministic or random generation.
type SeededRand struct {
	rng *rand.Rand
}

// NewSeededRand creates a new SeededRand. If seed is 0, uses a random seed.
func NewSeededRand(seed int64) *SeededRand {
	if seed == 0 {
		var b [8]byte
		_, err := crypto_rand.Read(b[:])
		if err == nil {
			seed = int64(binary.LittleEndian.Uint64(b[:]))
		} else {
			seed = time.Now().UnixNano()
		}
	}
	return &SeededRand{
		rng: rand.New(rand.NewSource(seed)),
	}
}

// ShouldInject returns true with probability equal to rate.
func (s *SeededRand) ShouldInject(rate float64) bool {
	if rate <= 0 {
		return false
	}
	if rate >= 1.0 {
		return true
	}
	return s.rng.Float64() < rate
}

// Pick returns a random element from the slice.
func (s *SeededRand) Pick(items []string) string {
	if len(items) == 0 {
		return ""
	}
	return items[s.rng.Intn(len(items))]
}

// Jitter adds a random offset between -maxJitter and +maxJitter to the base time.
func (s *SeededRand) Jitter(base time.Time, maxJitter time.Duration) time.Time {
	if maxJitter <= 0 {
		return base
	}
	const maxSafe = 24 * time.Hour
	if maxJitter > maxSafe {
		maxJitter = maxSafe
	}
	offset := time.Duration(s.rng.Int63n(int64(maxJitter)*2+1)) - maxJitter
	return base.Add(offset)
}

// Shuffle returns a randomly permuted slice of integers from 0 to n-1.
func (s *SeededRand) Shuffle(n int) []int {
	perm := s.rng.Perm(n)
	return perm
}

// Float64 returns a random float64 in [0.0, 1.0).
func (s *SeededRand) Float64() float64 {
	return s.rng.Float64()
}

// Intn returns a random integer in [0, n).
func (s *SeededRand) Intn(n int) int {
	if n <= 0 {
		return 0
	}
	return s.rng.Intn(n)
}
