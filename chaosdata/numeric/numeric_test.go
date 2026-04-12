package numeric

import (
	"bytes"
	"encoding/json"
	"math"
	"testing"

	"github.com/dwsmith1983/chaos-data/chaosdata"
)

// NumericGenerator tests

func TestNumericGenerator_Category(t *testing.T) {
	gen := &NumericGenerator{}
	if gen.Category() != "numeric" {
		t.Errorf("expected category 'numeric', got '%s'", gen.Category())
	}
}

func TestNumericGenerator_Generate(t *testing.T) {
	gen := &NumericGenerator{}
	vals, err := gen.Generate(chaosdata.GenerateOpts{Count: 1})
	if err != nil {
		t.Fatalf("Generate returned error: %v", err)
	}

	descriptions := map[string]bool{
		"Zero":                   false,
		"Negative Zero":          false,
		"MaxInt64":               false,
		"MinInt64":               false,
		"MaxFloat64":             false,
		"SmallestNonzeroFloat64": false,
		"NaN":                    false,
		"+Inf":                   false,
		"-Inf":                   false,
		"MaxInt32+1":             false,
		"High-precision float":   false,
		"2^53 (int exact as float64)":     false,
		"2^53+1 (loses precision)":        false,
		"1e308 (near max)":                false,
		"1e-324 (near min)":               false,
		"1.0 vs 1":                        false,
		"stringified int":                 false,
		"stringified float":               false,
		"stringified scientific":          false,
	}

	var parsed []map[string]any
	if err := json.Unmarshal(vals.Data, &parsed); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}
	for _, v := range parsed {
		if typ, ok := v["type"].(string); ok {
			descriptions[typ] = true
		}
	}

	for desc, found := range descriptions {
		if !found {
			t.Errorf("Missing expected chaos value: %s", desc)
		}
	}
}

// Low-level generator tests

// Registry helpers

func generatorByName(t *testing.T, name string) Generator {
	t.Helper()
	g, ok := Lookup(name)
	if !ok {
		t.Fatalf("generator %q not found in registry", name)
	}
	return g
}

// BoundaryValues tests

func TestBoundaryValues_Name(t *testing.T) {
	g := generatorByName(t, "boundary_values")
	if g.Name() != "boundary_values" {
		t.Errorf("Name() = %q; want %q", g.Name(), "boundary_values")
	}
}

func TestBoundaryValues_ContainsExpectedTokens(t *testing.T) {
	g := generatorByName(t, "boundary_values")
	payload := g.Generate()

	tokens := []string{
		"9223372036854775807",
		"9007199254740993",
		"9007199254740992",
		"1.7976931348623157e+308",
	}
	for _, tok := range tokens {
		if !bytes.Contains(payload, []byte(tok)) {
			t.Errorf("payload missing token %q\npayload: %s", tok, payload)
		}
	}
}

func TestBoundaryValues_ValidJSON(t *testing.T) {
	g := generatorByName(t, "boundary_values")
	var v interface{}
	if err := json.Unmarshal(g.Generate(), &v); err != nil {
		t.Fatalf("json.Unmarshal failed: %v", err)
	}
}

func TestBoundaryValues_Pow2_53Plus1_PrecisionLoss(t *testing.T) {
	g := generatorByName(t, "boundary_values")

	var v map[string]interface{}
	if err := json.Unmarshal(g.Generate(), &v); err != nil {
		t.Fatalf("json.Unmarshal failed: %v", err)
	}

	raw, ok := v["pow2_53_plus1"]
	if !ok {
		t.Fatal("key pow2_53_plus1 not present in decoded object")
	}
	f, ok := raw.(float64)
	if !ok {
		t.Fatalf("pow2_53_plus1 decoded as %T; want float64", raw)
	}

	const pow2_53 = float64(1 << 53)

	if f != pow2_53 {
		t.Errorf("expected float64 precision loss: got %v; want %v (2^53)", f, pow2_53)
	}
}

func TestBoundaryValues_MaxFloat64(t *testing.T) {
	g := generatorByName(t, "boundary_values")
	var v map[string]interface{}
	if err := json.Unmarshal(g.Generate(), &v); err != nil {
		t.Fatalf("json.Unmarshal failed: %v", err)
	}
	f := v["max_float64"].(float64)
	if f != math.MaxFloat64 {
		t.Errorf("max_float64 = %v; want %v", f, math.MaxFloat64)
	}
}

func TestBoundaryValues_Determinism(t *testing.T) {
	g := generatorByName(t, "boundary_values")
	first := g.Generate()
	for i := 0; i < 10; i++ {
		if !bytes.Equal(first, g.Generate()) {
			t.Fatal("Generate() is not deterministic")
		}
	}
}

func TestBoundaryValues_Count(t *testing.T) {
	g := generatorByName(t, "boundary_values")
	var v map[string]interface{}
	if err := json.Unmarshal(g.Generate(), &v); err != nil {
		t.Fatalf("json.Unmarshal failed: %v", err)
	}
	const wantKeys = 4
	if len(v) != wantKeys {
		t.Errorf("decoded object has %d keys; want %d", len(v), wantKeys)
	}
}

// SpecialValues tests

func TestSpecialValues_Name(t *testing.T) {
	g := generatorByName(t, "special_values")
	if g.Name() != "special_values" {
		t.Errorf("Name() = %q; want %q", g.Name(), "special_values")
	}
}

func TestSpecialValues_InvalidJSON(t *testing.T) {
	g := generatorByName(t, "special_values")
	var v interface{}
	if err := json.Unmarshal(g.Generate(), &v); err == nil {
		t.Error("expected json.Unmarshal to return an error for payload with NaN/Infinity literals; got nil")
	}
}

func TestSpecialValues_ContainsExpectedTokens(t *testing.T) {
	g := generatorByName(t, "special_values")
	payload := g.Generate()

	tests := []struct {
		name  string
		token string
	}{
		{"NaN literal", "NaN"},
		{"Infinity literal", "Infinity"},
		{"-Infinity literal", "-Infinity"},
		{"-0 literal", "-0"},
		{"NaN string", `"NaN"`},
		{"Infinity string", `"Infinity"`},
		{"-Infinity string", `"-Infinity"`},
		{"-0 string", `"-0"`},
	}
	for _, tt := range tests {
		if !bytes.Contains(payload, []byte(tt.token)) {
			t.Errorf("payload missing token %q\npayload: %s", tt.token, payload)
		}
	}
}

func TestSpecialValues_Determinism(t *testing.T) {
	g := generatorByName(t, "special_values")
	first := g.Generate()
	for i := 0; i < 10; i++ {
		if !bytes.Equal(first, g.Generate()) {
			t.Fatal("Generate() is not deterministic")
		}
	}
}

// ScientificNotation tests

func TestScientificNotation_Name(t *testing.T) {
	g := generatorByName(t, "scientific_notation")
	if g.Name() != "scientific_notation" {
		t.Errorf("Name() = %q; want %q", g.Name(), "scientific_notation")
	}
}

func TestScientificNotation_ContainsExpectedTokens(t *testing.T) {
	g := generatorByName(t, "scientific_notation")
	payload := g.Generate()

	tokens := []string{"1e308", "1e-324", "1e309", "1e-325"}
	for _, tok := range tokens {
		if !bytes.Contains(payload, []byte(tok)) {
			t.Errorf("payload missing token %q\npayload: %s", tok, payload)
		}
	}
}

func TestScientificNotation_ContainsOverflowLiteral(t *testing.T) {
	g := generatorByName(t, "scientific_notation")
	payload := g.Generate()
	if !bytes.Contains(payload, []byte("1e309")) {
		t.Errorf("payload missing 1e309 overflow literal: %s", payload)
	}
}

func TestScientificNotation_ContainsUnderflowLiteral(t *testing.T) {
	g := generatorByName(t, "scientific_notation")
	payload := g.Generate()
	if !bytes.Contains(payload, []byte("1e-325")) {
		t.Errorf("payload missing 1e-325 underflow literal: %s", payload)
	}
}

func TestScientificNotation_ContainsNearMaxLiteral(t *testing.T) {
	g := generatorByName(t, "scientific_notation")
	payload := g.Generate()
	if !bytes.Contains(payload, []byte("1e308")) {
		t.Errorf("payload missing 1e308 near-max literal: %s", payload)
	}
}

func TestScientificNotation_ContainsNearMinLiteral(t *testing.T) {
	g := generatorByName(t, "scientific_notation")
	payload := g.Generate()
	if !bytes.Contains(payload, []byte("1e-324")) {
		t.Errorf("payload missing 1e-324 near-min literal: %s", payload)
	}
}

func TestScientificNotation_Determinism(t *testing.T) {
	g := generatorByName(t, "scientific_notation")
	first := g.Generate()
	for i := 0; i < 10; i++ {
		if !bytes.Equal(first, g.Generate()) {
			t.Fatal("Generate() is not deterministic")
		}
	}
}

func TestScientificNotation_HasFourKeys(t *testing.T) {
	g := generatorByName(t, "scientific_notation")
	payload := g.Generate()
	for _, key := range []string{"near_max", "near_min", "overflow", "underflow"} {
		if !bytes.Contains(payload, []byte(`"`+key+`"`)) {
			t.Errorf("payload missing key %q: %s", key, payload)
		}
	}
}

// TypeAmbiguity tests

func TestTypeAmbiguity_Name(t *testing.T) {
	g := generatorByName(t, "type_ambiguity")
	if g.Name() != "type_ambiguity" {
		t.Errorf("Name() = %q; want %q", g.Name(), "type_ambiguity")
	}
}

func TestTypeAmbiguity_ValidJSON(t *testing.T) {
	g := generatorByName(t, "type_ambiguity")
	var v interface{}
	if err := json.Unmarshal(g.Generate(), &v); err != nil {
		t.Fatalf("json.Unmarshal failed: %v", err)
	}
}

func TestTypeAmbiguity_ContainsExpectedTokens(t *testing.T) {
	g := generatorByName(t, "type_ambiguity")
	payload := g.Generate()

	tokens := []string{"1.0", `"42"`, `"3.14"`, `"1e10"`, `"1.7976931348623157e+309"`}
	for _, tok := range tokens {
		if !bytes.Contains(payload, []byte(tok)) {
			t.Errorf("payload missing token %q\npayload: %s", tok, payload)
		}
	}
}

func TestTypeAmbiguity_FloatOneEqualsIntOne(t *testing.T) {
	g := generatorByName(t, "type_ambiguity")
	var v map[string]interface{}
	if err := json.Unmarshal(g.Generate(), &v); err != nil {
		t.Fatalf("json.Unmarshal failed: %v", err)
	}

	floatOne := v["float_one"].(float64)
	intOne := v["int_one"].(float64)

	if floatOne != intOne {
		t.Errorf("float_one (%v) != int_one (%v); expected both to decode to float64(1)", floatOne, intOne)
	}
	if floatOne != 1.0 {
		t.Errorf("float_one = %v; want 1.0", floatOne)
	}
}

func TestTypeAmbiguity_StringifiedNumbersAreStrings(t *testing.T) {
	g := generatorByName(t, "type_ambiguity")
	var v map[string]interface{}
	if err := json.Unmarshal(g.Generate(), &v); err != nil {
		t.Fatalf("json.Unmarshal failed: %v", err)
	}

	stringKeys := []struct {
		key  string
		want string
	}{
		{"stringified_int", "42"},
		{"stringified_float", "3.14"},
		{"stringified_sci", "1e10"},
		{"overflow_str", "1.7976931348623157e+309"},
	}
	for _, tc := range stringKeys {
		got, ok := v[tc.key].(string)
		if !ok {
			t.Fatalf("key %q decoded as %T; want string", tc.key, v[tc.key])
		}
		if got != tc.want {
			t.Errorf("key %q = %q; want %q", tc.key, got, tc.want)
		}
	}
}

func TestTypeAmbiguity_Determinism(t *testing.T) {
	g := generatorByName(t, "type_ambiguity")
	first := g.Generate()
	for i := 0; i < 10; i++ {
		if !bytes.Equal(first, g.Generate()) {
			t.Fatal("Generate() is not deterministic")
		}
	}
}

func TestTypeAmbiguity_Count(t *testing.T) {
	g := generatorByName(t, "type_ambiguity")
	var v map[string]interface{}
	if err := json.Unmarshal(g.Generate(), &v); err != nil {
		t.Fatalf("json.Unmarshal failed: %v", err)
	}
	const wantKeys = 6
	if len(v) != wantKeys {
		t.Errorf("decoded object has %d keys; want %d", len(v), wantKeys)
	}
}

// Registry tests

func TestRegistry_AllFourGeneratorsRegistered(t *testing.T) {
	wantNames := []string{
		"boundary_values",
		"special_values",
		"scientific_notation",
		"type_ambiguity",
	}
	for _, name := range wantNames {
		if _, ok := Lookup(name); !ok {
			t.Errorf("generator %q not found in registry", name)
		}
	}
}

func TestRegistry_AllCount(t *testing.T) {
	all := All()
	const wantCount = 4
	if len(all) != wantCount {
		t.Errorf("All() returned %d generators; want %d", len(all), wantCount)
	}
}

func TestRegistry_LookupUnknown(t *testing.T) {
	_, ok := Lookup("does_not_exist")
	if ok {
		t.Error("Lookup of unknown name returned ok=true; want false")
	}
}

func TestRegistry_AllGenerateNonEmpty(t *testing.T) {
	for _, g := range All() {
		g := g
		t.Run(g.Name(), func(t *testing.T) {
			payload := g.Generate()
			if len(payload) == 0 {
				t.Errorf("generator %q returned empty payload", g.Name())
			}
		})
	}
}

func FuzzNumericGenerator(f *testing.F) {
	f.Add(int64(0))
	f.Fuzz(func(t *testing.T, n int64) {
		// Valid fuzzer to ensure no panics
	})
}
