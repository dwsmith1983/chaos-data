// Package nulls provides chaos data generators for null/absence scenarios.
// Each generator produces raw []byte JSON payloads exercising different ways
// null or absence can manifest in structured data.
//
// All generators are registered in the package-level Registry via init().
package nulls

import (
	"encoding/json"
	"fmt"
	"sync"
)

// Generator is a function that produces a raw JSON payload as []byte.
type Generator func() []byte

// registry holds all named null-chaos generators.
type registry struct {
	mu         sync.RWMutex
	generators map[string]Generator
}

// Register adds a named generator to the registry.
// It panics on duplicate names to catch misconfiguration at startup.
func (r *registry) Register(name string, g Generator) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.generators[name]; exists {
		panic(fmt.Sprintf("nulls: generator already registered: %s", name))
	}
	r.generators[name] = g
}

// Get returns the named generator and whether it was found.
func (r *registry) Get(name string) (Generator, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	g, ok := r.generators[name]
	return g, ok
}

// Names returns all registered generator names in insertion order is not
// guaranteed; callers should sort if deterministic order is needed.
func (r *registry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.generators))
	for n := range r.generators {
		names = append(names, n)
	}
	return names
}

// Registry is the package-level registry populated by init().
var Registry = &registry{
	generators: make(map[string]Generator),
}

func init() {
	Registry.Register("null-variants", NullVariants)
	Registry.Register("nested-nulls", NestedNulls)
	Registry.Register("array-nulls", ArrayNulls)
}

// mustMarshal marshals v to JSON and panics on error.
// All inputs to mustMarshal are constructed internally and are always valid.
func mustMarshal(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(fmt.Sprintf("nulls: marshal failed: %v", err))
	}
	return b
}

// NullVariants returns a JSON array of objects that each exercise a distinct
// way null or absence can appear in a JSON payload:
//
//   - literal null value         {"field": null}
//   - empty string               {"field": ""}
//   - string "null"              {"field": "null"}
//   - string "NULL"              {"field": "NULL"}
//   - string "None"              {"field": "None"}
//   - string "nil"               {"field": "nil"}
//   - missing key                {}
//   - explicit null vs absent    {"present": null}  alongside  {"present": "value"}
func NullVariants() []byte {
	type record = map[string]any

	variants := []record{
		{"variant": "literal-null", "field": nil},
		{"variant": "empty-string", "field": ""},
		{"variant": "string-null", "field": "null"},
		{"variant": "string-NULL", "field": "NULL"},
		{"variant": "string-None", "field": "None"},
		{"variant": "string-nil", "field": "nil"},
		{"variant": "missing-key"},
		{"variant": "explicit-null", "present": nil},
		{"variant": "absent-field", "present": "value"},
	}
	return mustMarshal(variants)
}

// NestedNulls returns a JSON object with null values embedded at multiple
// depths of nesting.  The structure exercises:
//
//   - depth-1: top-level null field
//   - depth-2: null inside a nested object
//   - depth-3: null inside a doubly-nested object
//   - depth-4: null inside a triply-nested object
//   - mixed:   a branch that is itself null alongside a branch with a null leaf
func NestedNulls() []byte {
	payload := map[string]any{
		"depth1": nil,
		"depth2": map[string]any{
			"child": nil,
		},
		"depth3": map[string]any{
			"child": map[string]any{
				"grandchild": nil,
			},
		},
		"depth4": map[string]any{
			"child": map[string]any{
				"grandchild": map[string]any{
					"great_grandchild": nil,
				},
			},
		},
		"mixed": map[string]any{
			"null_branch":  nil,
			"value_branch": map[string]any{"leaf": nil},
		},
	}
	return mustMarshal(payload)
}

// ArrayNulls returns a JSON object containing multiple array fields that
// exercise null semantics inside arrays:
//
//   - null_elements:   array where some elements are null, some are not
//   - all_null:        array whose every element is null
//   - mixed_types:     array with string, number, bool, null, and object elements
//   - sparse_nulls:    a longer array with nulls scattered at various indices
//   - single_null:     array with exactly one element which is null
//   - empty:           empty array (absence of any element)
func ArrayNulls() []byte {
	payload := map[string]any{
		"null_elements": []any{1, nil, 2, nil, 3},
		"all_null":      []any{nil, nil, nil},
		"mixed_types":   []any{"string", 42, true, nil, map[string]any{"key": "value"}},
		"sparse_nulls":  buildSparseArray(10, 0, 3, 7, 9),
		"single_null":   []any{nil},
		"empty":         []any{},
	}
	return mustMarshal(payload)
}

// buildSparseArray creates a numeric array of length n and sets the positions
// listed in nullAt to nil.
func buildSparseArray(n int, nullAt ...int) []any {
	arr := make([]any, n)
	for i := range arr {
		arr[i] = i
	}
	nullSet := make(map[int]struct{}, len(nullAt))
	for _, idx := range nullAt {
		nullSet[idx] = struct{}{}
	}
	for _, idx := range nullAt {
		if idx >= 0 && idx < n {
			arr[idx] = nil
		}
	}
	_ = nullSet
	return arr
}
