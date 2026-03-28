package interlocksuite

import (
	"context"
	"testing"
	"time"
)

func TestLocalEventReader_Emit_ReadEvents(t *testing.T) {
	t.Parallel()
	r := NewLocalEventReader()

	r.Emit(InterlockEventRecord{PipelineID: "pipe-a", EventType: "POST_RUN_DRIFT", Timestamp: time.Now()})
	r.Emit(InterlockEventRecord{PipelineID: "pipe-a", EventType: "RERUN_ACCEPTED", Timestamp: time.Now()})
	r.Emit(InterlockEventRecord{PipelineID: "pipe-b", EventType: "POST_RUN_DRIFT", Timestamp: time.Now()})

	events, err := r.ReadEvents(context.Background(), "pipe-a", "POST_RUN_DRIFT")
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 {
		t.Fatalf("got %d events, want 1", len(events))
	}
	if events[0].EventType != "POST_RUN_DRIFT" {
		t.Errorf("event type = %q", events[0].EventType)
	}
}

func TestLocalEventReader_ReadEvents_AllTypes(t *testing.T) {
	t.Parallel()
	r := NewLocalEventReader()
	r.Emit(InterlockEventRecord{PipelineID: "pipe-a", EventType: "A"})
	r.Emit(InterlockEventRecord{PipelineID: "pipe-a", EventType: "B"})

	// Empty eventType returns all for pipeline
	events, err := r.ReadEvents(context.Background(), "pipe-a", "")
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 {
		t.Fatalf("got %d, want 2", len(events))
	}
}

func TestLocalEventReader_ReadEvents_NoMatch(t *testing.T) {
	t.Parallel()
	r := NewLocalEventReader()
	events, err := r.ReadEvents(context.Background(), "pipe-a", "NOPE")
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 0 {
		t.Errorf("got %d, want 0", len(events))
	}
}

func TestLocalEventReader_Reset(t *testing.T) {
	t.Parallel()
	r := NewLocalEventReader()
	r.Emit(InterlockEventRecord{PipelineID: "pipe-a", EventType: "X"})
	r.Reset()
	events, err := r.ReadEvents(context.Background(), "pipe-a", "")
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 0 {
		t.Errorf("got %d after reset, want 0", len(events))
	}
}

func TestLocalEventReader_ConcurrentEmitRead(t *testing.T) {
	t.Parallel()
	r := NewLocalEventReader()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 100; i++ {
			r.Emit(InterlockEventRecord{PipelineID: "pipe", EventType: "E"})
		}
	}()
	for i := 0; i < 100; i++ {
		_, _ = r.ReadEvents(context.Background(), "pipe", "E")
	}
	<-done
}

func TestAWSEventReader_ReturnsError(t *testing.T) {
	t.Parallel()
	r := NewAWSEventReader()
	_, err := r.ReadEvents(context.Background(), "pipe", "EVENT")
	if err == nil {
		t.Error("expected error from unimplemented AWS reader, got nil")
	}
}
