package engine

// Record is a map representing a single data record.
type Record map[string]any

// Generator defines the interface for generating data records.
type Generator interface {
	// Next returns the next generated record or an error if generation fails or completes.
	Next() (Record, error)
}
