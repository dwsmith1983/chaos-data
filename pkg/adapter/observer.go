package adapter

import (
	"context"
	"errors"

	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// EventEmitter writes chaos events for monitoring/alerting.
type EventEmitter interface {
	Emit(ctx context.Context, event types.ChaosEvent) error
}

// EventReader retrieves recorded chaos events.
type EventReader interface {
	Manifest(ctx context.Context) ([]types.ChaosEvent, error)
}

// CompositeEmitter fans out events to multiple emitters.
// If any emitter returns an error, it continues to the rest and returns
// a combined error.
type CompositeEmitter struct {
	emitters []EventEmitter
}

// NewCompositeEmitter creates a CompositeEmitter that delegates to the
// given emitters in order.
func NewCompositeEmitter(emitters ...EventEmitter) *CompositeEmitter {
	return &CompositeEmitter{emitters: emitters}
}

// Emit sends the event to every emitter. If one or more emitters return
// an error, Emit continues to the remaining emitters and returns a
// combined error via errors.Join.
func (c *CompositeEmitter) Emit(ctx context.Context, event types.ChaosEvent) error {
	var errs []error
	for _, e := range c.emitters {
		if err := e.Emit(ctx, event); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
