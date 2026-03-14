package adapter_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// mockEmitter is a test double that records Emit calls and returns
// a pre-configured error.
type mockEmitter struct {
	emittedEvents []types.ChaosEvent
	emitErr       error
}

func (m *mockEmitter) Emit(_ context.Context, event types.ChaosEvent) error {
	m.emittedEvents = append(m.emittedEvents, event)
	return m.emitErr
}

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
	em1 := &mockEmitter{}
	em2 := &mockEmitter{}
	em3 := &mockEmitter{}

	composite := adapter.NewCompositeEmitter(em1, em2, em3)
	event := sampleEvent()

	err := composite.Emit(context.Background(), event)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	for i, em := range []*mockEmitter{em1, em2, em3} {
		if len(em.emittedEvents) != 1 {
			t.Errorf("emitter %d: expected 1 event, got %d", i, len(em.emittedEvents))
		}
		if em.emittedEvents[0].ID != event.ID {
			t.Errorf("emitter %d: expected event ID %q, got %q", i, event.ID, em.emittedEvents[0].ID)
		}
	}
}

func TestCompositeEmitter_EmitContinuesOnErrorReturnsCombined(t *testing.T) {
	errFirst := errors.New("emitter 1 failed")
	errThird := errors.New("emitter 3 failed")

	em1 := &mockEmitter{emitErr: errFirst}
	em2 := &mockEmitter{}
	em3 := &mockEmitter{emitErr: errThird}

	composite := adapter.NewCompositeEmitter(em1, em2, em3)
	event := sampleEvent()

	err := composite.Emit(context.Background(), event)
	if err == nil {
		t.Fatal("expected an error, got nil")
	}

	// All emitters should have received the event despite errors.
	for i, em := range []*mockEmitter{em1, em2, em3} {
		if len(em.emittedEvents) != 1 {
			t.Errorf("emitter %d: expected 1 event, got %d", i, len(em.emittedEvents))
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
