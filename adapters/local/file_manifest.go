package local

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// Compile-time interface assertion.
var _ adapter.EventEmitter = (*FileManifestObserver)(nil)

// FileManifestObserver implements adapter.EventEmitter by appending
// JSON-encoded chaos events as newline-delimited lines to a file.
// It is safe for concurrent use.
type FileManifestObserver struct {
	f      *os.File
	mu     sync.Mutex
	closed bool
}

// NewFileManifestObserver opens (or creates) the file at path in append mode
// and returns a FileManifestObserver that writes events to it.
func NewFileManifestObserver(path string) (*FileManifestObserver, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("file manifest observer: open %q: %w", path, err)
	}
	return &FileManifestObserver{f: f}, nil
}

// Emit marshals the event to JSON and appends it as a single JSONL line.
// It returns an error if the observer has been closed or if the write fails.
func (o *FileManifestObserver) Emit(_ context.Context, event types.ChaosEvent) error {
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("file manifest observer: marshal event: %w", err)
	}

	// Append newline to form a complete JSONL line.
	data = append(data, '\n')

	o.mu.Lock()
	defer o.mu.Unlock()

	if o.closed {
		return errors.New("file manifest observer: observer is closed")
	}

	if _, err := o.f.Write(data); err != nil {
		return fmt.Errorf("file manifest observer: write event: %w", err)
	}
	return nil
}

// Close flushes and closes the underlying file. Subsequent calls to Emit
// will return an error.
func (o *FileManifestObserver) Close() error {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.closed {
		return nil
	}
	o.closed = true
	if err := o.f.Close(); err != nil {
		return fmt.Errorf("file manifest observer: close: %w", err)
	}
	return nil
}
