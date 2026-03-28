package interlocksuite

import (
	"context"
	"testing"
)

// Compile-time interface satisfaction checks.
var _ InterlockEvaluator = (*AWSInterlockEvaluator)(nil)
var _ InterlockEvaluator = (*LocalInterlockEvaluator)(nil)

func TestAWSInterlockEvaluator_NoOp(t *testing.T) {
	t.Parallel()
	e := NewAWSInterlockEvaluator()
	err := e.EvaluateAfterInjection(context.Background(), "pipe-a", "hourly", "2026-03-27T10")
	if err != nil {
		t.Fatalf("expected nil error for AWS no-op, got: %v", err)
	}
}

func TestLocalInterlockEvaluator_Stub(t *testing.T) {
	t.Parallel()
	reader := NewLocalEventReader()
	e := NewLocalInterlockEvaluator(nil, reader, nil)
	err := e.EvaluateAfterInjection(context.Background(), "pipe-a", "hourly", "2026-03-27T10")
	if err != nil {
		t.Fatalf("expected nil error for stub, got: %v", err)
	}
}
