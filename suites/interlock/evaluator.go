package interlocksuite

import (
	"context"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
)

// InterlockEvaluator triggers Interlock rule evaluation after chaos injection.
type InterlockEvaluator interface {
	EvaluateAfterInjection(ctx context.Context, pipeline, schedule, date string) error
}

// ---------------------------------------------------------------------------
// AWS implementation (no-op)
// ---------------------------------------------------------------------------

// AWSInterlockEvaluator is a no-op — DynamoDB Streams triggers evaluation
// automatically via the stream-router Lambda.
type AWSInterlockEvaluator struct{}

// NewAWSInterlockEvaluator returns a new AWSInterlockEvaluator.
func NewAWSInterlockEvaluator() *AWSInterlockEvaluator {
	return &AWSInterlockEvaluator{}
}

// EvaluateAfterInjection is a no-op for AWS. DynamoDB Streams → stream-router
// Lambda handles evaluation automatically when state changes.
func (e *AWSInterlockEvaluator) EvaluateAfterInjection(_ context.Context, _, _, _ string) error {
	return nil
}

// ---------------------------------------------------------------------------
// Local implementation (stub)
// ---------------------------------------------------------------------------

// LocalInterlockEvaluator embeds Interlock's validation engine for local testing.
// TODO(phase4-wiring): Wire in interlock/pkg/validation when go.mod replace
// directive is configured.
type LocalInterlockEvaluator struct {
	store       adapter.StateStore
	eventWriter *LocalEventReader
	clock       adapter.Clock
}

// NewLocalInterlockEvaluator returns a new LocalInterlockEvaluator.
func NewLocalInterlockEvaluator(store adapter.StateStore, eventWriter *LocalEventReader, clock adapter.Clock) *LocalInterlockEvaluator {
	return &LocalInterlockEvaluator{store: store, eventWriter: eventWriter, clock: clock}
}

// EvaluateAfterInjection is a stub. The actual interlock validation import will
// be wired in a follow-up task when the go.mod replace directive is configured.
// For now, callers must emit events directly for local testing.
func (e *LocalInterlockEvaluator) EvaluateAfterInjection(_ context.Context, _, _, _ string) error {
	// TODO(phase4-wiring): Import interlock/pkg/validation and evaluate rules.
	return nil
}
