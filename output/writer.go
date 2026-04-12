package output

import "github.com/dwsmith1983/chaos-data/engine"

// Writer defines the interface for writing records to an output destination.
type Writer interface {
	// Write writes a single record to the output.
	Write(record engine.Record) error
	// Flush ensures all buffered records are written to the output.
	Flush() error
	// Close closes the writer and releases associated resources.
	Close() error
}
