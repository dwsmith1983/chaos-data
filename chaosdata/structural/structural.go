// Package structural provides chaos data generators for JSON structural
// edge cases: deeply nested objects, extremely wide objects, empty
// collections, mixed-type arrays, degenerate single-element cases,
// duplicate keys, arrays of nulls, and intentionally invalid JSON
// (trailing commas).
//
// Intentionally-invalid JSON cases are clearly documented and returned
// with an "valid_json" attribute set to "false" so consumers can
// distinguish them from parseable payloads.
package structural

import (
	"fmt"
	"strings"

	"github.com/dwsmith1983/chaos-data/chaosdata"
)

// maxNestingDepth is the hard ceiling used by deepNestGenerator to prevent
// runaway allocations from a misconfigured opts.Size value.
const maxNestingDepth = 1000

// ---------------------------------------------------------------------------
// deepNestGenerator
// ---------------------------------------------------------------------------

// deepNestGenerator produces a JSON object nested opts.Size levels deep
// (capped at maxNestingDepth).  The nesting is built with a simple loop
// rather than recursion to keep stack usage constant regardless of depth.
//
// Example output (depth 3):
//
//	{"d0":{"d1":{"d2":{}}}}
type deepNestGenerator struct{}

func (deepNestGenerator) Name() string     { return "deep-nest" }
func (deepNestGenerator) Category() string { return "structural" }

func (deepNestGenerator) Generate(opts chaosdata.GenerateOpts) (chaosdata.Payload, error) {
	depth := opts.Size
	if depth <= 0 {
		depth = 100
	}
	if depth > maxNestingDepth {
		depth = maxNestingDepth
	}

	var sb strings.Builder
	// Open depth levels.
	for i := 0; i < depth; i++ {
		fmt.Fprintf(&sb, `{"d%d":`, i)
	}
	// Innermost empty object.
	sb.WriteString("{}")
	// Close depth levels.
	for i := 0; i < depth; i++ {
		sb.WriteByte('}')
	}

	return chaosdata.Payload{
		Data:     []byte(sb.String()),
		Type:     "application/json",
		Attributes: map[string]string{
			"valid_json": "true",
			"depth":      fmt.Sprintf("%d", depth),
		},
	}, nil
}

// ---------------------------------------------------------------------------
// emptyArrayGenerator
// ---------------------------------------------------------------------------

// emptyArrayGenerator produces an empty JSON array: [].
type emptyArrayGenerator struct{}

func (emptyArrayGenerator) Name() string     { return "empty-array" }
func (emptyArrayGenerator) Category() string { return "structural" }

func (emptyArrayGenerator) Generate(_ chaosdata.GenerateOpts) (chaosdata.Payload, error) {
	return chaosdata.Payload{
		Data: []byte(`[]`),
		Type: "application/json",
		Attributes: map[string]string{
			"valid_json": "true",
		},
	}, nil
}

// ---------------------------------------------------------------------------
// emptyObjectGenerator
// ---------------------------------------------------------------------------

// emptyObjectGenerator produces an empty JSON object: {}.
type emptyObjectGenerator struct{}

func (emptyObjectGenerator) Name() string     { return "empty-object" }
func (emptyObjectGenerator) Category() string { return "structural" }

func (emptyObjectGenerator) Generate(_ chaosdata.GenerateOpts) (chaosdata.Payload, error) {
	return chaosdata.Payload{
		Data: []byte(`{}`),
		Type: "application/json",
		Attributes: map[string]string{
			"valid_json": "true",
		},
	}, nil
}

// ---------------------------------------------------------------------------
// mixedTypeArrayGenerator
// ---------------------------------------------------------------------------

// mixedTypeArrayGenerator produces a JSON array whose elements span every
// JSON primitive type plus nested objects and arrays.
type mixedTypeArrayGenerator struct{}

func (mixedTypeArrayGenerator) Name() string     { return "mixed-type-array" }
func (mixedTypeArrayGenerator) Category() string { return "structural" }

func (mixedTypeArrayGenerator) Generate(_ chaosdata.GenerateOpts) (chaosdata.Payload, error) {
	data := []byte(`[null,true,false,0,-1,3.14,"string","",[],{},[1,"two",null],{"k":"v"}]`)
	return chaosdata.Payload{
		Data: data,
		Type: "application/json",
		Attributes: map[string]string{
			"valid_json": "true",
		},
	}, nil
}

// ---------------------------------------------------------------------------
// wideObjectGenerator
// ---------------------------------------------------------------------------

// wideObjectGenerator produces a JSON object with opts.Size keys
// (default 1000, minimum 1000).  All values are integer indices so the
// output is deterministic and easy to verify programmatically.
type wideObjectGenerator struct{}

func (wideObjectGenerator) Name() string     { return "wide-object" }
func (wideObjectGenerator) Category() string { return "structural" }

func (wideObjectGenerator) Generate(opts chaosdata.GenerateOpts) (chaosdata.Payload, error) {
	width := opts.Size
	if width < 1000 {
		width = 1000
	}

	var sb strings.Builder
	sb.Grow(width * 20) // rough pre-allocation
	sb.WriteByte('{')
	for i := 0; i < width; i++ {
		if i > 0 {
			sb.WriteByte(',')
		}
		fmt.Fprintf(&sb, `"k%d":%d`, i, i)
	}
	sb.WriteByte('}')

	return chaosdata.Payload{
		Data: []byte(sb.String()),
		Type: "application/json",
		Attributes: map[string]string{
			"valid_json": "true",
			"key_count":  fmt.Sprintf("%d", width),
		},
	}, nil
}

// ---------------------------------------------------------------------------
// singleElementGenerator
// ---------------------------------------------------------------------------

// singleElementGenerator produces degenerate single-element cases:
// a one-item array and a one-key object bundled together as a JSON array.
type singleElementGenerator struct{}

func (singleElementGenerator) Name() string     { return "single-element" }
func (singleElementGenerator) Category() string { return "structural" }

func (singleElementGenerator) Generate(_ chaosdata.GenerateOpts) (chaosdata.Payload, error) {
	// Array containing: single-item array, single-key object, single null array.
	data := []byte(`[[42],{"only":"value"},[null]]`)
	return chaosdata.Payload{
		Data: data,
		Type: "application/json",
		Attributes: map[string]string{
			"valid_json": "true",
		},
	}, nil
}

// ---------------------------------------------------------------------------
// trailingCommaGenerator  — INTENTIONALLY INVALID JSON
// ---------------------------------------------------------------------------

// trailingCommaGenerator produces JSON-like payloads that contain trailing
// commas after the last element in objects and arrays.  These are
// intentionally invalid per RFC 8259 and are documented as such via the
// "valid_json"="false" attribute.  They are useful for testing parsers that
// must reject or tolerate trailing commas.
type trailingCommaGenerator struct{}

func (trailingCommaGenerator) Name() string     { return "trailing-comma" }
func (trailingCommaGenerator) Category() string { return "structural" }

func (trailingCommaGenerator) Generate(_ chaosdata.GenerateOpts) (chaosdata.Payload, error) {
	// INVALID JSON: trailing comma in object and array.
	data := []byte(`{"a":1,"b":2,}` + "\n" + `[1,2,3,]`)
	return chaosdata.Payload{
		Data: data,
		Type: "application/json-invalid",
		// valid_json=false is the contract callers must check.
		Attributes: map[string]string{
			"valid_json": "false",
			"reason":     "trailing comma after last element (RFC 8259 §2)",
		},
	}, nil
}

// ---------------------------------------------------------------------------
// duplicateKeyGenerator  — INTENTIONALLY INVALID / AMBIGUOUS JSON
// ---------------------------------------------------------------------------

// duplicateKeyGenerator produces a JSON object containing duplicate keys.
// RFC 8259 §4 notes that names within an object SHOULD be unique; behaviour
// on duplicate keys is undefined by the spec.  Many parsers silently accept
// the last value, others reject the document entirely.  The attribute
// "valid_json" is set to "false" to flag this as a hazardous payload.
type duplicateKeyGenerator struct{}

func (duplicateKeyGenerator) Name() string     { return "duplicate-keys" }
func (duplicateKeyGenerator) Category() string { return "structural" }

func (duplicateKeyGenerator) Generate(_ chaosdata.GenerateOpts) (chaosdata.Payload, error) {
	// INVALID / AMBIGUOUS: duplicate key "id" with conflicting values.
	data := []byte(`{"id":1,"name":"Alice","id":2,"name":"Bob","id":null}`)
	return chaosdata.Payload{
		Data: data,
		Type: "application/json-invalid",
		Attributes: map[string]string{
			"valid_json": "false",
			"reason":     "duplicate object keys (RFC 8259 §4 — behaviour undefined)",
		},
	}, nil
}

// ---------------------------------------------------------------------------
// nullArrayGenerator
// ---------------------------------------------------------------------------

// nullArrayGenerator produces a JSON array populated entirely with null
// values.  opts.Size controls how many nulls (default 10).
type nullArrayGenerator struct{}

func (nullArrayGenerator) Name() string     { return "null-array" }
func (nullArrayGenerator) Category() string { return "structural" }

func (nullArrayGenerator) Generate(opts chaosdata.GenerateOpts) (chaosdata.Payload, error) {
	count := opts.Size
	if count <= 0 {
		count = 10
	}

	var sb strings.Builder
	sb.Grow(count*5 + 2)
	sb.WriteByte('[')
	for i := 0; i < count; i++ {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString("null")
	}
	sb.WriteByte(']')

	return chaosdata.Payload{
		Data: []byte(sb.String()),
		Type: "application/json",
		Attributes: map[string]string{
			"valid_json": "true",
			"null_count": fmt.Sprintf("%d", count),
		},
	}, nil
}

// ---------------------------------------------------------------------------
// init: self-registration
// ---------------------------------------------------------------------------

func init() {
	chaosdata.Register(deepNestGenerator{})
	chaosdata.Register(emptyArrayGenerator{})
	chaosdata.Register(emptyObjectGenerator{})
	chaosdata.Register(mixedTypeArrayGenerator{})
	chaosdata.Register(wideObjectGenerator{})
	chaosdata.Register(singleElementGenerator{})
	chaosdata.Register(trailingCommaGenerator{})
	chaosdata.Register(duplicateKeyGenerator{})
	chaosdata.Register(nullArrayGenerator{})
}
