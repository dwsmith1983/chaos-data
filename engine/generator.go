package engine

// Record represents a single data entry.
type Record map[string]any

// Generator defines the interface for generating records.
type Generator interface {
	Next() (Record, error)
}
