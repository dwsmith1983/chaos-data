// Package referential provides chaos data generators for referential-integrity
// edge cases: dangling foreign keys, self-referencing IDs, zero/negative IDs,
// all-zero UUIDs, IDs exceeding int64 range, duplicate primary keys in a batch,
// orphaned child records, and circular $ref-style pointer references.
package referential

import (
	"encoding/json"
	"fmt"
)

// ---------------------------------------------------------------------------
// Interface + Options
// ---------------------------------------------------------------------------

// ChaosGenerator is the interface all referential generators implement.
// Generate returns raw []byte payloads; count in Options controls how many
// records are produced.
type ChaosGenerator interface {
	Name() string
	Generate(opts Options) ([][]byte, error)
}

// Options controls generator behaviour.
type Options struct {
	Count int // how many records; <= 0 defaults to defaultCount
	Seed  int64
}

const defaultCount = 5

func (o Options) countOrDefault() int {
	if o.Count <= 0 {
		return defaultCount
	}
	return o.Count
}

// ---------------------------------------------------------------------------
// Registry
// ---------------------------------------------------------------------------

// registry holds every registered ChaosGenerator, keyed by Name().
var registry = map[string]ChaosGenerator{}

// Register adds a generator to the global registry. It is called from init().
func Register(g ChaosGenerator) {
	registry[g.Name()] = g
}

// All returns a snapshot of all registered generators.
func All() map[string]ChaosGenerator {
	out := make(map[string]ChaosGenerator, len(registry))
	for k, v := range registry {
		out[k] = v
	}
	return out
}

// Lookup finds a generator by name.
func Lookup(name string) (ChaosGenerator, bool) {
	g, ok := registry[name]
	return g, ok
}

// ---------------------------------------------------------------------------
// Generator: DanglingForeignKey
// ---------------------------------------------------------------------------

// DanglingForeignKey emits JSON records whose foreign_key field references an
// ID that does not correspond to any existing parent row.
type DanglingForeignKey struct{}

func (DanglingForeignKey) Name() string { return "dangling-foreign-key" }

func (DanglingForeignKey) Generate(opts Options) ([][]byte, error) {
	n := opts.countOrDefault()
	out := make([][]byte, n)
	for i := 0; i < n; i++ {
		rec := map[string]any{
			"id":          i + 1,
			"foreign_key": 999_999 + i, // deliberately non-existent parent ID
			"chaos_type":  "dangling_foreign_key",
		}
		b, err := json.Marshal(rec)
		if err != nil {
			return nil, fmt.Errorf("dangling-foreign-key marshal: %w", err)
		}
		out[i] = b
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// Generator: SelfReferencingID
// ---------------------------------------------------------------------------

// SelfReferencingID emits JSON records where parent_id equals the record's own
// id, creating a self-loop in the reference graph.
type SelfReferencingID struct{}

func (SelfReferencingID) Name() string { return "self-referencing-id" }

func (SelfReferencingID) Generate(opts Options) ([][]byte, error) {
	n := opts.countOrDefault()
	out := make([][]byte, n)
	for i := 0; i < n; i++ {
		id := i + 1
		rec := map[string]any{
			"id":         id,
			"parent_id":  id, // self-reference
			"chaos_type": "self_referencing_id",
		}
		b, err := json.Marshal(rec)
		if err != nil {
			return nil, fmt.Errorf("self-referencing-id marshal: %w", err)
		}
		out[i] = b
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// Generator: ZeroValueID
// ---------------------------------------------------------------------------

// ZeroValueID emits JSON records containing zero-value IDs: numeric 0 and
// empty string "".
type ZeroValueID struct{}

func (ZeroValueID) Name() string { return "zero-value-id" }

// zeroIDPool is the fixed set of zero-value payloads; it is cycled when
// more records than len(pool) are requested.
var zeroIDPool = [][]byte{
	[]byte(`{"id":0,"str_id":"","chaos_type":"zero_value_id","variant":"numeric_zero"}`),
	[]byte(`{"id":0,"str_id":"","chaos_type":"zero_value_id","variant":"string_empty"}`),
	[]byte(`{"id":0,"str_id":"0","chaos_type":"zero_value_id","variant":"string_zero"}`),
	[]byte(`{"id":0,"str_id":null,"chaos_type":"zero_value_id","variant":"null_id"}`),
}

func (ZeroValueID) Generate(opts Options) ([][]byte, error) {
	n := opts.countOrDefault()
	out := make([][]byte, n)
	for i := 0; i < n; i++ {
		src := zeroIDPool[i%len(zeroIDPool)]
		dst := make([]byte, len(src))
		copy(dst, src)
		out[i] = dst
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// Generator: NegativeID
// ---------------------------------------------------------------------------

// NegativeID emits JSON records whose id field is -1 or decreasing negative
// integers.
type NegativeID struct{}

func (NegativeID) Name() string { return "negative-id" }

func (NegativeID) Generate(opts Options) ([][]byte, error) {
	n := opts.countOrDefault()
	out := make([][]byte, n)
	for i := 0; i < n; i++ {
		rec := map[string]any{
			"id":         -(i + 1), // -1, -2, -3, …
			"chaos_type": "negative_id",
		}
		b, err := json.Marshal(rec)
		if err != nil {
			return nil, fmt.Errorf("negative-id marshal: %w", err)
		}
		out[i] = b
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// Generator: ZeroUUID
// ---------------------------------------------------------------------------

// ZeroUUID emits JSON records whose uuid field is the all-zero UUID.
type ZeroUUID struct{}

func (ZeroUUID) Name() string { return "zero-uuid" }

const allZerosUUID = "00000000-0000-0000-0000-000000000000"

func (ZeroUUID) Generate(opts Options) ([][]byte, error) {
	n := opts.countOrDefault()
	out := make([][]byte, n)
	for i := 0; i < n; i++ {
		rec := map[string]any{
			"id":         i + 1,
			"uuid":       allZerosUUID,
			"chaos_type": "zero_uuid",
		}
		b, err := json.Marshal(rec)
		if err != nil {
			return nil, fmt.Errorf("zero-uuid marshal: %w", err)
		}
		out[i] = b
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// Generator: OverflowID
// ---------------------------------------------------------------------------

// OverflowID emits JSON records whose id field exceeds the int64 maximum
// (9223372036854775807), represented as a JSON string to preserve precision.
type OverflowID struct{}

func (OverflowID) Name() string { return "overflow-id" }

// overflowIDPool holds a variety of IDs that exceed int64 range.
var overflowIDPool = [][]byte{
	[]byte(`{"id":"9223372036854775808","chaos_type":"overflow_id","variant":"int64_max_plus_1"}`),
	[]byte(`{"id":"18446744073709551615","chaos_type":"overflow_id","variant":"uint64_max"}`),
	[]byte(`{"id":"99999999999999999999","chaos_type":"overflow_id","variant":"arbitrary_large"}`),
	[]byte(`{"id":"-9223372036854775809","chaos_type":"overflow_id","variant":"int64_min_minus_1"}`),
}

func (OverflowID) Generate(opts Options) ([][]byte, error) {
	n := opts.countOrDefault()
	out := make([][]byte, n)
	for i := 0; i < n; i++ {
		src := overflowIDPool[i%len(overflowIDPool)]
		dst := make([]byte, len(src))
		copy(dst, src)
		out[i] = dst
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// Generator: DuplicatePrimaryKey
// ---------------------------------------------------------------------------

// DuplicatePrimaryKey emits a JSON array batch in which the same primary key
// appears more than once.
type DuplicatePrimaryKey struct{}

func (DuplicatePrimaryKey) Name() string { return "duplicate-primary-key" }

func (DuplicatePrimaryKey) Generate(opts Options) ([][]byte, error) {
	n := opts.countOrDefault()
	// We produce n records where every other record reuses the previous id so
	// that collisions appear throughout the batch.
	out := make([][]byte, n)
	for i := 0; i < n; i++ {
		// Even indices get id = i+1; odd indices repeat the previous even id.
		id := i + 1
		if i%2 == 1 {
			id = i // same as the previous record
		}
		rec := map[string]any{
			"id":         id,
			"seq":        i,
			"chaos_type": "duplicate_primary_key",
		}
		b, err := json.Marshal(rec)
		if err != nil {
			return nil, fmt.Errorf("duplicate-primary-key marshal: %w", err)
		}
		out[i] = b
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// Generator: OrphanedChild
// ---------------------------------------------------------------------------

// OrphanedChild emits JSON records that represent child rows whose parent_id
// does not match any record in a co-emitted parent set. The payload carries an
// explicit "orphaned":true flag to make assertions straightforward.
type OrphanedChild struct{}

func (OrphanedChild) Name() string { return "orphaned-child" }

func (OrphanedChild) Generate(opts Options) ([][]byte, error) {
	n := opts.countOrDefault()
	out := make([][]byte, n)
	for i := 0; i < n; i++ {
		// parent_id is deliberately offset far beyond any real parent id.
		rec := map[string]any{
			"child_id":   i + 1,
			"parent_id":  100_000 + i,
			"orphaned":   true,
			"chaos_type": "orphaned_child",
		}
		b, err := json.Marshal(rec)
		if err != nil {
			return nil, fmt.Errorf("orphaned-child marshal: %w", err)
		}
		out[i] = b
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// Generator: CircularReference
// ---------------------------------------------------------------------------

// CircularReference emits JSON records that use $ref-style pointer fields to
// form cycles: record A points to B, B points to C, C points back to A.
type CircularReference struct{}

func (CircularReference) Name() string { return "circular-reference" }

func (CircularReference) Generate(opts Options) ([][]byte, error) {
	n := opts.countOrDefault()
	out := make([][]byte, n)
	for i := 0; i < n; i++ {
		// The $ref points to the *next* record (wrapping around), completing a
		// cycle across the batch.
		nextID := (i+1)%n + 1
		rec := map[string]any{
			"id":         i + 1,
			"$ref":       fmt.Sprintf("#/records/%d", nextID),
			"chaos_type": "circular_reference",
		}
		b, err := json.Marshal(rec)
		if err != nil {
			return nil, fmt.Errorf("circular-reference marshal: %w", err)
		}
		out[i] = b
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// init: self-registration
// ---------------------------------------------------------------------------

func init() {
	Register(DanglingForeignKey{})
	Register(SelfReferencingID{})
	Register(ZeroValueID{})
	Register(NegativeID{})
	Register(ZeroUUID{})
	Register(OverflowID{})
	Register(DuplicatePrimaryKey{})
	Register(OrphanedChild{})
	Register(CircularReference{})
}
