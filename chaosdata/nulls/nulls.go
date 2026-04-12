package nulls

import (
	"encoding/json"
	"fmt"

	"github.com/dwsmith1983/chaos-data/chaosdata"
)

// NullsGenerator produces chaos data payloads for null/absence scenarios.
type NullsGenerator struct{}

func (NullsGenerator) Name() string {
	return "nulls"
}

func (NullsGenerator) Category() string {
	return "nulls"
}

func (g NullsGenerator) Generate(opts chaosdata.GenerateOpts) (chaosdata.Payload, error) {
	records := []map[string]interface{}{}

	// Basic null variants
	records = append(records, map[string]interface{}{"type": "nil", "value": nil})
	records = append(records, map[string]interface{}{"type": "empty string", "value": ""})
	records = append(records, map[string]interface{}{"type": "zero-length slice", "value": []interface{}{}})
	records = append(records, map[string]interface{}{"type": "zero-length map", "value": map[string]interface{}{}})
	records = append(records, map[string]interface{}{"type": "string literal null", "value": "null"})
	records = append(records, map[string]interface{}{"type": "string literal NULL", "value": "NULL"})
	records = append(records, map[string]interface{}{"type": "string literal nil", "value": "nil"})
	records = append(records, map[string]interface{}{"type": "string literal None", "value": "None"})
	records = append(records, map[string]interface{}{"type": "string literal undefined", "value": "undefined"})
	records = append(records, map[string]interface{}{"type": "Unicode null", "value": "\u0000"})
	records = append(records, map[string]interface{}{"type": "null byte in middle of string", "value": "a\x00b"})
	records = append(records, map[string]interface{}{
		"type":  "sql.NullString Valid=false",
		"value": map[string]interface{}{"String": "", "Valid": false},
	})

	// Nested null variants
	records = append(records, map[string]interface{}{
		"type":  "nested object with null field",
		"value": map[string]interface{}{"child": nil},
	})
	records = append(records, map[string]interface{}{
		"type":  "doubly-nested null",
		"value": map[string]interface{}{"child": map[string]interface{}{"grandchild": nil}},
	})
	records = append(records, map[string]interface{}{
		"type":  "triply-nested null",
		"value": map[string]interface{}{
			"child": map[string]interface{}{
				"grandchild": map[string]interface{}{"great_grandchild": nil},
			},
		},
	})
	records = append(records, map[string]interface{}{
		"type": "mixed null branches",
		"value": map[string]interface{}{
			"null_branch":  nil,
			"value_branch": map[string]interface{}{"leaf": nil},
		},
	})

	// Array null variants
	records = append(records, map[string]interface{}{
		"type":  "array with null elements",
		"value": []interface{}{1, nil, 2, nil, 3},
	})
	records = append(records, map[string]interface{}{
		"type":  "array of all nulls",
		"value": []interface{}{nil, nil, nil},
	})
	records = append(records, map[string]interface{}{
		"type":  "mixed types with null",
		"value": []interface{}{"string", 42, true, nil, map[string]interface{}{"key": "value"}},
	})
	records = append(records, map[string]interface{}{
		"type":  "single null element array",
		"value": []interface{}{nil},
	})
	records = append(records, map[string]interface{}{
		"type":  "empty array",
		"value": []interface{}{},
	})

	// Sparse null array
	sparseArray := []interface{}{0, 1, nil, 3, nil, 5, 6, nil, 8, nil}
	records = append(records, map[string]interface{}{
		"type":  "sparse nulls in array",
		"value": sparseArray,
	})

	count := opts.Count
	if count < 1 {
		count = 1
	}

	all := make([]map[string]interface{}, 0, len(records)*count)
	for i := 0; i < count; i++ {
		all = append(all, records...)
	}

	data, err := json.Marshal(all)
	if err != nil {
		return chaosdata.Payload{}, fmt.Errorf("nulls: marshal payload: %w", err)
	}

	return chaosdata.Payload{
		Data: data,
		Type: "application/json",
		Attributes: map[string]string{
			"generator": g.Name(),
			"category":  g.Category(),
			"records":   fmt.Sprintf("%d", len(all)),
		},
	}, nil
}

func init() {
	chaosdata.Register(NullsGenerator{})
}

// Helper functions for low-level testing

// mustMarshal marshals v to JSON and panics on error.
// All inputs are constructed internally and are always valid.
func mustMarshal(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(fmt.Sprintf("nulls: marshal failed: %v", err))
	}
	return b
}

// NullVariants returns a JSON array of objects that each exercise a distinct
// way null or absence can appear in a JSON payload.
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
// depths of nesting.
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
// exercise null semantics inside arrays.
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
	for _, idx := range nullAt {
		if idx >= 0 && idx < n {
			arr[idx] = nil
		}
	}
	return arr
}
