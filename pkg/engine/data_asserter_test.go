package engine_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/engine"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

func TestDataStateAsserter_Exists_Found(t *testing.T) {
	t.Parallel()
	transport := &mockTransport{
		ReadFn: func(_ context.Context, key string) (io.ReadCloser, error) {
			return io.NopCloser(strings.NewReader("data")), nil
		},
	}
	da := engine.NewDataStateAsserter(transport)
	ok, err := da.Evaluate(context.Background(), types.Assertion{
		Type: types.AssertDataState, Target: "file.jsonl", Condition: types.CondExists,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Error("expected true (object exists)")
	}
}

func TestDataStateAsserter_Exists_NotFound(t *testing.T) {
	t.Parallel()
	transport := &mockTransport{
		ReadFn: func(_ context.Context, key string) (io.ReadCloser, error) {
			return nil, fmt.Errorf("read %s: %w", key, adapter.ErrNotFound)
		},
	}
	da := engine.NewDataStateAsserter(transport)
	ok, err := da.Evaluate(context.Background(), types.Assertion{
		Type: types.AssertDataState, Target: "missing.jsonl", Condition: types.CondExists,
	})
	if err != nil {
		t.Fatalf("not-found should not be an error: %v", err)
	}
	if ok {
		t.Error("expected false (object not found)")
	}
}

func TestDataStateAsserter_Exists_OtherError(t *testing.T) {
	t.Parallel()
	transport := &mockTransport{
		ReadFn: func(_ context.Context, _ string) (io.ReadCloser, error) {
			return nil, errors.New("connection refused")
		},
	}
	da := engine.NewDataStateAsserter(transport)
	ok, err := da.Evaluate(context.Background(), types.Assertion{
		Type: types.AssertDataState, Target: "file.jsonl", Condition: types.CondExists,
	})
	if err == nil {
		t.Fatal("expected error for non-not-found failure")
	}
	if ok {
		t.Error("expected false on error")
	}
}

func TestDataStateAsserter_NotExists_NotFound(t *testing.T) {
	t.Parallel()
	transport := &mockTransport{
		ReadFn: func(_ context.Context, key string) (io.ReadCloser, error) {
			return nil, fmt.Errorf("read %s: %w", key, adapter.ErrNotFound)
		},
	}
	da := engine.NewDataStateAsserter(transport)
	ok, err := da.Evaluate(context.Background(), types.Assertion{
		Type: types.AssertDataState, Target: "gone.jsonl", Condition: types.CondNotExists,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Error("expected true (object not found)")
	}
}

func TestDataStateAsserter_NotExists_OtherError(t *testing.T) {
	t.Parallel()
	transport := &mockTransport{
		ReadFn: func(_ context.Context, _ string) (io.ReadCloser, error) {
			return nil, errors.New("connection refused")
		},
	}
	da := engine.NewDataStateAsserter(transport)
	ok, err := da.Evaluate(context.Background(), types.Assertion{
		Type: types.AssertDataState, Target: "file.jsonl", Condition: types.CondNotExists,
	})
	if err == nil {
		t.Fatal("expected error for non-not-found failure")
	}
	if ok {
		t.Error("expected false on error")
	}
}

func TestDataStateAsserter_NotExists_Exists(t *testing.T) {
	t.Parallel()
	transport := &mockTransport{
		ReadFn: func(_ context.Context, _ string) (io.ReadCloser, error) {
			return io.NopCloser(strings.NewReader("data")), nil
		},
	}
	da := engine.NewDataStateAsserter(transport)
	ok, err := da.Evaluate(context.Background(), types.Assertion{
		Type: types.AssertDataState, Target: "file.jsonl", Condition: types.CondNotExists,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok {
		t.Error("expected false (object exists)")
	}
}

func TestDataStateAsserter_IsHeld_Found(t *testing.T) {
	t.Parallel()
	transport := &mockTransport{
		ListHeldFn: func(_ context.Context) ([]types.HeldObject, error) {
			return []types.HeldObject{
				{DataObject: types.DataObject{Key: "other.jsonl"}},
				{DataObject: types.DataObject{Key: "target.jsonl"}},
			}, nil
		},
	}
	da := engine.NewDataStateAsserter(transport)
	ok, err := da.Evaluate(context.Background(), types.Assertion{
		Type: types.AssertDataState, Target: "target.jsonl", Condition: types.CondIsHeld,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Error("expected true (object is held)")
	}
}

func TestDataStateAsserter_IsHeld_NotFound(t *testing.T) {
	t.Parallel()
	transport := &mockTransport{
		ListHeldFn: func(_ context.Context) ([]types.HeldObject, error) {
			return []types.HeldObject{{DataObject: types.DataObject{Key: "other.jsonl"}}}, nil
		},
	}
	da := engine.NewDataStateAsserter(transport)
	ok, err := da.Evaluate(context.Background(), types.Assertion{
		Type: types.AssertDataState, Target: "missing.jsonl", Condition: types.CondIsHeld,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok {
		t.Error("expected false (object not held)")
	}
}

func TestDataStateAsserter_IsHeld_Error(t *testing.T) {
	t.Parallel()
	transport := &mockTransport{
		ListHeldFn: func(_ context.Context) ([]types.HeldObject, error) {
			return nil, errors.New("storage unavailable")
		},
	}
	da := engine.NewDataStateAsserter(transport)
	ok, err := da.Evaluate(context.Background(), types.Assertion{
		Type: types.AssertDataState, Target: "file.jsonl", Condition: types.CondIsHeld,
	})
	if err == nil {
		t.Fatal("expected error")
	}
	if ok {
		t.Error("expected false on error")
	}
}

func TestDataStateAsserter_UnsupportedCondition(t *testing.T) {
	t.Parallel()
	transport := &mockTransport{}
	da := engine.NewDataStateAsserter(transport)
	ok, err := da.Evaluate(context.Background(), types.Assertion{
		Type: types.AssertDataState, Target: "file.jsonl", Condition: types.CondIsStale,
	})
	if err == nil {
		t.Fatal("expected error for unsupported condition")
	}
	if ok {
		t.Error("expected false on error")
	}
}
