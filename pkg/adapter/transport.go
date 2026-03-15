package adapter

import (
	"context"
	"io"
	"time"

	"github.com/dwsmith1983/chaos-data/pkg/types"
)

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
}
