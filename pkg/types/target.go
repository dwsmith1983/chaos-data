package types

import (
	"errors"
	"fmt"
	"path"
	"strings"
	"time"
)

// ErrInvalidLayer is returned when a target layer value is not recognized.
var ErrInvalidLayer = errors.New("invalid layer")

// validLayer reports whether s is an allowed Target layer value.
func validLayer(s string) bool {
	switch s {
	case "data", "state", "orchestrator":
		return true
	default:
		return false
	}
}

// Target represents a chaos injection target within a specific architectural
// layer. Transport identifies the adapter-specific storage or system, and
// Filter narrows down which objects within that system are targeted.
type Target struct {
	Layer     string       `json:"layer"`
	Transport string       `json:"transport,omitempty"`
	Filter    ObjectFilter `json:"filter,omitempty"`
}

// Validate checks that the Target has a valid layer.
func (t Target) Validate() error {
	if !validLayer(t.Layer) {
		return fmt.Errorf("%w: %q", ErrInvalidLayer, t.Layer)
	}
	return nil
}

// ObjectFilter defines prefix and glob matching criteria for data objects.
// Both Prefix and Match must be satisfied when set (logical AND).
type ObjectFilter struct {
	Prefix string `json:"prefix,omitempty"`
	Match  string `json:"match,omitempty"`
}

// Matches returns true if obj satisfies the filter criteria.
//
// The filter applies a logical AND of all non-empty fields:
//   - Prefix: obj.Key must start with Prefix
//   - Match: obj.Key must match the glob pattern (using path.Match)
//
// An empty filter (both fields zero-valued) matches every object.
// An invalid glob pattern is treated as non-matching.
//
// Note: path.Match does not match across '/' separators.  A pattern like
// "*.csv" matches "report.csv" but NOT "data/report.csv".  To match objects
// nested under a directory, include the path structure in the glob pattern
// (e.g., "data/*.csv").
func (f ObjectFilter) Matches(obj DataObject) bool {
	if f.Prefix != "" && !strings.HasPrefix(obj.Key, f.Prefix) {
		return false
	}
	if f.Match != "" {
		matched, err := path.Match(f.Match, obj.Key)
		if err != nil || !matched {
			return false
		}
	}
	return true
}

// DataObject represents a data object within a storage system (e.g., an S3
// object or a database row). It carries enough metadata to support filtering,
// reporting, and mutation tracking.
type DataObject struct {
	Key          string            `json:"key"`
	Size         int64             `json:"size"`
	LastModified time.Time         `json:"last_modified"`
	ContentType  string            `json:"content_type"`
	Metadata     map[string]string `json:"metadata,omitempty"`
}

// HeldObject is a DataObject that has been held (delayed) by chaos injection.
// HeldUntil indicates when the hold expires, and Reason describes why the
// object was held.
type HeldObject struct {
	DataObject
	HeldUntil time.Time `json:"held_until"`
	Reason    string    `json:"reason"`
}
