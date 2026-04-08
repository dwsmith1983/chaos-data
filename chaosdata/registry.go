package chaosdata

import (
	"sync"
)

var (
	registry []ChaosGenerator
	mu       sync.RWMutex
)

// Register adds a generator to the registry.
func Register(g ChaosGenerator) {
	mu.Lock()
	defer mu.Unlock()
	registry = append(registry, g)
}

// All returns all registered generators.
func All() []ChaosGenerator {
	mu.RLock()
	defer mu.RUnlock()

	// Return a copy to avoid external mutation
	result := make([]ChaosGenerator, len(registry))
	copy(result, registry)
	return result
}

// ByCategory returns all registered generators in a specific category.
func ByCategory(category string) []ChaosGenerator {
	mu.RLock()
	defer mu.RUnlock()

	var result []ChaosGenerator
	for _, g := range registry {
		if g.Category() == category {
			result = append(result, g)
		}
	}
	return result
}
