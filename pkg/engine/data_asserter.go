package engine

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// DataStateAsserter evaluates data_state assertions using DataTransport.
type DataStateAsserter struct {
	transport adapter.DataTransport
}

// NewDataStateAsserter creates a DataStateAsserter wrapping the given transport.
func NewDataStateAsserter(transport adapter.DataTransport) *DataStateAsserter {
	return &DataStateAsserter{transport: transport}
}

// Evaluate checks whether the data_state assertion's condition holds.
func (d *DataStateAsserter) Evaluate(ctx context.Context, a types.Assertion) (bool, error) {
	switch a.Condition {
	case types.CondExists:
		rc, err := d.transport.Read(ctx, a.Target)
		if err != nil {
			if isNotFound(err) {
				return false, nil
			}
			return false, err
		}
		_ = rc.Close() // presence-check only; close error not actionable
		return true, nil

	case types.CondNotExists:
		rc, err := d.transport.Read(ctx, a.Target)
		if err != nil {
			if isNotFound(err) {
				return true, nil
			}
			return false, err
		}
		_ = rc.Close() // presence-check only; close error not actionable
		return false, nil

	case types.CondIsHeld:
		held, err := d.transport.ListHeld(ctx)
		if err != nil {
			return false, err
		}
		for _, h := range held {
			if h.Key == a.Target {
				return true, nil
			}
		}
		return false, nil

	default:
		return false, fmt.Errorf("unsupported data_state condition: %q", a.Condition)
	}
}

// isNotFound reports whether the error indicates the key was absent rather than
// a genuine transport failure. It checks the adapter.ErrNotFound sentinel first,
// then falls back to string matching for transports not yet wrapping the sentinel.
func isNotFound(err error) bool {
	if errors.Is(err, adapter.ErrNotFound) {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "not found") ||
		strings.Contains(msg, "no such file")
}
