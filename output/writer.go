package output

import "github.com/dwsmith1983/chaos-data/engine"

// Writer defines the interface for writing records to an output destination.
type Writer interface {
	// Write writes a single record to the output destination.
	Write(record engine.Record) error
	// Flush ensures any buffered records are written to the output destination.
	Flush() error
	// Close closes the writer and releases any associated resources.
	Close() error
}
