package output

import "github.com/dwsmith1983/chaos-data/engine"

// Writer defines the interface for writing records to a destination.
type Writer interface {
	Write(record engine.Record) error
	Flush() error
	Close() error
}
