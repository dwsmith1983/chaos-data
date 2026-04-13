package engine

// Record is a type alias for a map representing a data record.
type Record map[string]any

// Generator defines the interface for generating data records.
type Generator interface {
	Next() (Record, error)
}
