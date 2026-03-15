package adapter

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// ErrNotFound is returned by DataTransport.Read when the key does not exist.
// Implementations should wrap this sentinel so callers can use errors.Is.
var ErrNotFound = errors.New("not found")

// DataTransport moves data between staging and pipeline destinations.
type DataTransport interface {
	List(ctx context.Context, prefix string) ([]types.DataObject, error)
	Read(ctx context.Context, key string) (io.ReadCloser, error)
	Write(ctx context.Context, key string, data io.Reader) error
	Delete(ctx context.Context, key string) error

	// Hold prevents the object identified by key from being visible to
	// downstream consumers until the specified time. Implementations may
	// use side-channel storage (e.g., a holding prefix, metadata tags) to
	// track hold state.
	Hold(ctx context.Context, key string, until time.Time) error

	// Release makes a held object immediately visible to downstream
	// consumers, cancelling any pending Hold deadline. Implementations may
	// use side-channel storage (e.g., a holding prefix, metadata tags) to
	// track hold state.
	Release(ctx context.Context, key string) error

	// ListHeld returns DataObjects currently in hold state. Implementations
	// should exclude metadata sidecars from the result.
	ListHeld(ctx context.Context) ([]types.DataObject, error)

	// ReleaseAll immediately releases all currently held objects, making
	// them visible to downstream consumers. It is a best-effort, fail-open
	// operation: individual Release failures are collected and returned via
	// errors.Join but do not prevent remaining releases from being attempted.
	// This is intended as a kill-switch safety mechanism.
	ReleaseAll(ctx context.Context) error
}
