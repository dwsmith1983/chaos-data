// Package numeric provides chaos data generators that produce raw []byte JSON
// payloads exercising numeric edge cases: boundary values, special/invalid
// values, scientific notation extremes, and type-ambiguous representations.
//
// Each generator is registered via init() using the package-level registry so
// that callers can enumerate or invoke generators by name without importing
// concrete types.
package numeric

import (
	"fmt"
	"math"
	"sync"
)

// Generator produces a deterministic raw JSON payload as a []byte slice.
// The payload may be valid or intentionally invalid JSON — callers should
// handle parse errors from json.Unmarshal gracefully.
type Generator interface {
	// Name returns the unique identifier for this generator.
	Name() string

	// Generate returns the raw JSON payload bytes.
	Generate() []byte
}

// registry holds all registered generators, keyed by name.
var (
	mu       sync.RWMutex
	registry = make(map[string]Generator)
)

// Register adds a Generator to the global registry. Panics on duplicate name.
func Register(g Generator) {
	mu.Lock()
	defer mu.Unlock()
	name := g.Name()
	if _, exists := registry[name]; exists {
		panic(fmt.Sprintf("numeric: duplicate generator registration: %q", name))
	}
	registry[name] = g
}

// Lookup returns the Generator registered under name and whether it exists.
func Lookup(name string) (Generator, bool) {
	mu.RLock()
	defer mu.RUnlock()
	g, ok := registry[name]
	return g, ok
}

// All returns a slice of every registered Generator in an unspecified order.
func All() []Generator {
	mu.RLock()
	defer mu.RUnlock()
	gs := make([]Generator, 0, len(registry))
	for _, g := range registry {
		gs = append(gs, g)
	}
	return gs
}

// ---------------------------------------------------------------------------
// 1. BoundaryValues
// ---------------------------------------------------------------------------

// boundaryValuesGenerator emits a JSON object containing numeric boundary
// literals that stress integer and floating-point precision:
//   - int64 max:  9223372036854775807
//   - 2^53+1:     9007199254740993  (first integer not exactly representable as float64)
//   - 2^53:       9007199254740992  (largest integer exactly representable as float64)
//   - MaxFloat64: 1.7976931348623157e+308
type boundaryValuesGenerator struct{}

func (boundaryValuesGenerator) Name() string { return "boundary_values" }

func (boundaryValuesGenerator) Generate() []byte {
	// Use raw literal construction so the exact token bytes are preserved.
	// json.Marshal would silently round 2^53+1 when encoding float64.
	const payload = `{` +
		`"int64_max":9223372036854775807,` +
		`"pow2_53_plus1":9007199254740993,` +
		`"pow2_53":9007199254740992,` +
		`"max_float64":1.7976931348623157e+308` +
		`}`
	return []byte(payload)
}

// ---------------------------------------------------------------------------
// 2. SpecialValues  (intentionally invalid JSON)
// ---------------------------------------------------------------------------

// specialValuesGenerator emits a JSON object that uses literal NaN, Infinity,
// -Infinity, and -0 as JSON number tokens — none of which are permitted by
// RFC 8259 — plus string-wrapped variants of each so callers can compare the
// two representations side by side.
type specialValuesGenerator struct{}

func (specialValuesGenerator) Name() string { return "special_values" }

func (specialValuesGenerator) Generate() []byte {
	// NaN / Infinity / -Infinity are rejected by json.Unmarshal.
	// -0 is accepted as 0 by most parsers but is semantically distinct.
	const payload = `{` +
		`"nan":NaN,` +
		`"pos_infinity":Infinity,` +
		`"neg_infinity":-Infinity,` +
		`"neg_zero":-0,` +
		`"nan_str":"NaN",` +
		`"pos_infinity_str":"Infinity",` +
		`"neg_infinity_str":"-Infinity",` +
		`"neg_zero_str":"-0"` +
		`}`
	return []byte(payload)
}

// ---------------------------------------------------------------------------
// 3. ScientificNotation
// ---------------------------------------------------------------------------

// scientificNotationGenerator emits a JSON object with four scientific-
// notation literals that probe float64 range limits:
//   - 1e308:  near MaxFloat64, representable
//   - 1e-324: near SmallestNonzeroFloat64 (sub-normal floor)
//   - 1e309:  overflow → +Inf when unmarshalled into float64
//   - 1e-325: underflow → 0 when unmarshalled into float64
type scientificNotationGenerator struct{}

func (scientificNotationGenerator) Name() string { return "scientific_notation" }

func (scientificNotationGenerator) Generate() []byte {
	// Verify the boundary values at init-time so we know they are correct.
	_ = math.MaxFloat64        // 1.7976931348623157e+308
	_ = math.SmallestNonzeroFloat64 // 5e-324

	const payload = `{` +
		`"near_max":1e308,` +
		`"near_min":1e-324,` +
		`"overflow":1e309,` +
		`"underflow":1e-325` +
		`}`
	return []byte(payload)
}

// ---------------------------------------------------------------------------
// 4. TypeAmbiguity
// ---------------------------------------------------------------------------

// typeAmbiguityGenerator emits a JSON object whose values illustrate common
// type-ambiguity pitfalls parsed into interface{}:
//   - 1.0 vs 1:           both become float64(1) via json.Unmarshal
//   - stringified numbers: "42", "3.14", "1e10"
//   - overflow string:     a decimal that exceeds float64 range as a string
type typeAmbiguityGenerator struct{}

func (typeAmbiguityGenerator) Name() string { return "type_ambiguity" }

func (typeAmbiguityGenerator) Generate() []byte {
	const payload = `{` +
		`"float_one":1.0,` +
		`"int_one":1,` +
		`"stringified_int":"42",` +
		`"stringified_float":"3.14",` +
		`"stringified_sci":"1e10",` +
		`"overflow_str":"1.7976931348623157e+309"` +
		`}`
	return []byte(payload)
}

// ---------------------------------------------------------------------------
// init — register all generators
// ---------------------------------------------------------------------------

func init() {
	Register(boundaryValuesGenerator{})
	Register(specialValuesGenerator{})
	Register(scientificNotationGenerator{})
	Register(typeAmbiguityGenerator{})
}
