package numeric

import (
	"encoding/json"
	"fmt"
	"math"

	"github.com/dwsmith1983/chaos-data/chaosdata"
)

// NumericGenerator produces chaos data payloads for numeric edge cases.
type NumericGenerator struct{}

func (NumericGenerator) Name() string {
	return "numeric"
}

func (NumericGenerator) Category() string {
	return "numeric"
}

func (g NumericGenerator) Generate(opts chaosdata.GenerateOpts) (chaosdata.Payload, error) {
	records := []map[string]interface{}{}

	// Basic numeric values
	records = append(records, map[string]interface{}{"type": "Zero", "value": 0})
	records = append(records, map[string]interface{}{"type": "Negative Zero", "value": math.Copysign(0, -1)})
	records = append(records, map[string]interface{}{"type": "MaxInt64", "value": int64(math.MaxInt64)})
	records = append(records, map[string]interface{}{"type": "MinInt64", "value": int64(math.MinInt64)})
	records = append(records, map[string]interface{}{"type": "MaxFloat64", "value": math.MaxFloat64})
	records = append(records, map[string]interface{}{"type": "SmallestNonzeroFloat64", "value": math.SmallestNonzeroFloat64})

	// Special values (as strings since JSON doesn't support them)
	records = append(records, map[string]interface{}{"type": "NaN", "value": "NaN"})
	records = append(records, map[string]interface{}{"type": "+Inf", "value": "+Inf"})
	records = append(records, map[string]interface{}{"type": "-Inf", "value": "-Inf"})

	// Overflow/underflow
	records = append(records, map[string]interface{}{"type": "MaxInt32+1", "value": int64(math.MaxInt32) + 1})
	records = append(records, map[string]interface{}{"type": "High-precision float", "value": 0.1234567890123456789})

	// Boundary values
	records = append(records, map[string]interface{}{"type": "2^53 (int exact as float64)", "value": float64(1 << 53)})
	records = append(records, map[string]interface{}{"type": "2^53+1 (loses precision)", "value": float64(math.MaxInt64)})

	// Scientific notation extremes
	records = append(records, map[string]interface{}{"type": "1e308 (near max)", "value": 1e308})
	records = append(records, map[string]interface{}{"type": "1e-324 (near min)", "value": 1e-324})

	// Type ambiguity
	records = append(records, map[string]interface{}{"type": "1.0 vs 1", "value": map[string]interface{}{"float": 1.0, "int": 1}})
	records = append(records, map[string]interface{}{"type": "stringified int", "value": "42"})
	records = append(records, map[string]interface{}{"type": "stringified float", "value": "3.14"})
	records = append(records, map[string]interface{}{"type": "stringified scientific", "value": "1e10"})

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
		return chaosdata.Payload{}, fmt.Errorf("numeric: marshal payload: %w", err)
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

// Low-level generator interface and implementations

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
var registry = make(map[string]Generator)

// Register adds a Generator to the global registry. Panics on duplicate name.
func Register(g Generator) {
	name := g.Name()
	if _, exists := registry[name]; exists {
		panic(fmt.Sprintf("numeric: duplicate generator registration: %q", name))
	}
	registry[name] = g
}

// Lookup returns the Generator registered under name and whether it exists.
func Lookup(name string) (Generator, bool) {
	g, ok := registry[name]
	return g, ok
}

// All returns a slice of every registered Generator in an unspecified order.
func All() []Generator {
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
// literals that stress integer and floating-point precision.
type boundaryValuesGenerator struct{}

func (boundaryValuesGenerator) Name() string { return "boundary_values" }

func (boundaryValuesGenerator) Generate() []byte {
	const payload = `{` +
		`"int64_max":9223372036854775807,` +
		`"pow2_53_plus1":9007199254740993,` +
		`"pow2_53":9007199254740992,` +
		`"max_float64":1.7976931348623157e+308` +
		`}`
	return []byte(payload)
}

// ---------------------------------------------------------------------------
// 2. SpecialValues (intentionally invalid JSON)
// ---------------------------------------------------------------------------

// specialValuesGenerator emits a JSON object that uses literal NaN, Infinity,
// -Infinity, and -0 as JSON number tokens — none of which are permitted by
// RFC 8259.
type specialValuesGenerator struct{}

func (specialValuesGenerator) Name() string { return "special_values" }

func (specialValuesGenerator) Generate() []byte {
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

// scientificNotationGenerator emits a JSON object with scientific notation
// literals that probe float64 range limits.
type scientificNotationGenerator struct{}

func (scientificNotationGenerator) Name() string { return "scientific_notation" }

func (scientificNotationGenerator) Generate() []byte {
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
// type-ambiguity pitfalls when parsed into interface{}.
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
	chaosdata.Register(NumericGenerator{})
}
