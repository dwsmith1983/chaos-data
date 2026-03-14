package local

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// Compile-time interface assertion.
var _ adapter.EventEmitter = (*StdoutEmitter)(nil)

// StdoutEmitter implements adapter.EventEmitter by writing JSON-encoded
// events as newline-delimited lines to a writer.
type StdoutEmitter struct {
	w  io.Writer
	mu sync.Mutex
}

// NewStdoutEmitter creates a StdoutEmitter that writes to w.
func NewStdoutEmitter(w io.Writer) *StdoutEmitter {
	return &StdoutEmitter{w: w}
}

// Emit marshals the event to JSON and writes it as a single line.
func (e *StdoutEmitter) Emit(_ context.Context, event types.ChaosEvent) error {
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	// Append newline to form a complete JSONL line.
	data = append(data, '\n')

	e.mu.Lock()
	defer e.mu.Unlock()

	if _, err := e.w.Write(data); err != nil {
		return fmt.Errorf("write event: %w", err)
	}
	return nil
}
