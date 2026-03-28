package interlocksuite

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// InterlockEventRecord represents an Interlock-emitted event.
type InterlockEventRecord struct {
	PipelineID string                 `json:"pipeline_id"`
	EventType  string                 `json:"event_type"`
	Timestamp  time.Time              `json:"timestamp"`
	Detail     map[string]interface{} `json:"detail,omitempty"`
}

// InterlockEventReader reads Interlock-emitted events.
type InterlockEventReader interface {
	ReadEvents(ctx context.Context, pipeline string, eventType string) ([]InterlockEventRecord, error)
	Reset()
}

// ---------------------------------------------------------------------------
// Local (in-memory) implementation
// ---------------------------------------------------------------------------

// LocalEventReader stores events in memory. Thread-safe.
// The LocalInterlockEvaluator writes events here after rule evaluation.
type LocalEventReader struct {
	mu     sync.Mutex
	events []InterlockEventRecord
}

// NewLocalEventReader returns a new empty LocalEventReader.
func NewLocalEventReader() *LocalEventReader {
	return &LocalEventReader{}
}

// Emit adds an event. Called by LocalInterlockEvaluator.
func (r *LocalEventReader) Emit(event InterlockEventRecord) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.events = append(r.events, event)
}

// ReadEvents returns events matching the pipeline and optional event type filter.
// If eventType is empty, returns all events for the pipeline.
func (r *LocalEventReader) ReadEvents(_ context.Context, pipeline string, eventType string) ([]InterlockEventRecord, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	var matched []InterlockEventRecord
	for _, e := range r.events {
		if e.PipelineID != pipeline {
			continue
		}
		if eventType != "" && e.EventType != eventType {
			continue
		}
		matched = append(matched, e)
	}

	return matched, nil
}

// Reset clears all events. Used between scenarios for isolation.
func (r *LocalEventReader) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.events = nil
}

// ---------------------------------------------------------------------------
// AWS (DynamoDB) implementation — stub
// ---------------------------------------------------------------------------

// AWSEventReader reads from Interlock's DynamoDB events table.
// The events table is populated by Interlock's event-sink Lambda from EventBridge.
type AWSEventReader struct {
	// Will be populated when AWS target is wired up.
}

// NewAWSEventReader returns a new AWSEventReader.
func NewAWSEventReader() *AWSEventReader {
	return &AWSEventReader{}
}

// ReadEvents queries the DynamoDB events table.
// Not yet implemented; returns an error.
func (r *AWSEventReader) ReadEvents(_ context.Context, _ string, _ string) ([]InterlockEventRecord, error) {
	return nil, fmt.Errorf("AWS event reader not yet implemented")
}

// Reset is a no-op for AWSEventReader — events are managed externally.
func (r *AWSEventReader) Reset() {}
