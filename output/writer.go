package output

import "github.com/dwsmith1983/chaos-data/engine"

// Writer defines the interface for writing data records to a destination.
type Writer interface {
	// Write writes a single record to the output.
	Write(record engine.Record) error
	// Flush flushes any buffered data to the destination.
	Flush() error
	// Close closes the writer and releases any resources.
	Close() error
}
