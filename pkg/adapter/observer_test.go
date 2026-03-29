package adapter_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dwsmith1983/chaos-data/internal/testutil"
	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

func sampleEvent() types.ChaosEvent {
	return types.ChaosEvent{
		ID:        "evt-1",
		Scenario:  "delay",
		Severity:  types.SeverityLow,
		Timestamp: time.Now(),
		Mode:      "deterministic",
	}
}

func TestCompositeEmitter_EmitFansOutToAllEmitters(t *testing.T) {
	em1 := &testutil.MockEmitter{}
	em2 := &testutil.MockEmitter{}
	em3 := &testutil.MockEmitter{}

	composite := adapter.NewCompositeEmitter(em1, em2, em3)
	event := sampleEvent()

	err := composite.Emit(context.Background(), event)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	for i, em := range []*testutil.MockEmitter{em1, em2, em3} {
		events := em.GetEvents()
		if len(events) != 1 {
			t.Errorf("emitter %d: expected 1 event, got %d", i, len(events))
		}
		if events[0].ID != event.ID {
			t.Errorf("emitter %d: expected event ID %q, got %q", i, event.ID, events[0].ID)
		}
	}
}

func TestCompositeEmitter_EmitContinuesOnErrorReturnsCombined(t *testing.T) {
	errFirst := errors.New("emitter 1 failed")
	errThird := errors.New("emitter 3 failed")

	em1 := &testutil.MockEmitter{Err: errFirst}
	em2 := &testutil.MockEmitter{}
	em3 := &testutil.MockEmitter{Err: errThird}

	composite := adapter.NewCompositeEmitter(em1, em2, em3)
	event := sampleEvent()

	err := composite.Emit(context.Background(), event)
	if err == nil {
		t.Fatal("expected an error, got nil")
	}

	// All emitters should have received the event despite errors.
	for i, em := range []*testutil.MockEmitter{em1, em2, em3} {
		if len(em.GetEvents()) != 1 {
			t.Errorf("emitter %d: expected 1 event, got %d", i, len(em.GetEvents()))
		}
	}

	// The combined error should contain both individual errors.
	if !errors.Is(err, errFirst) {
		t.Errorf("combined error should contain errFirst: %v", err)
	}
	if !errors.Is(err, errThird) {
		t.Errorf("combined error should contain errThird: %v", err)
	}
}

func TestCompositeEmitter_EmptyNoEmitters(t *testing.T) {
	composite := adapter.NewCompositeEmitter()

	// Emit with no emitters should return nil.
	err := composite.Emit(context.Background(), sampleEvent())
	if err != nil {
		t.Fatalf("expected no error from empty composite Emit, got %v", err)
	}
}
