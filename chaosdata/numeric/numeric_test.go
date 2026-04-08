package numeric_test

import (
	"bytes"
	"encoding/json"
	"math"
	"testing"

	"github.com/dwsmith1983/chaos-data/chaosdata/numeric"
)

// ---------------------------------------------------------------------------
// Registry helpers
// ---------------------------------------------------------------------------

func generatorByName(t *testing.T, name string) numeric.Generator {
	t.Helper()
	g, ok := numeric.Lookup(name)
	if !ok {
		t.Fatalf("generator %q not found in registry", name)
	}
	return g
}

// ---------------------------------------------------------------------------
// 1. BoundaryValues
// ---------------------------------------------------------------------------

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

// Demonstrate that 2^53+1 suffers precision loss when unmarshalled into
// interface{} (which uses float64 under the hood).
func TestBoundaryValues_Pow2_53Plus1_PrecisionLoss(t *testing.T) {
	g := generatorByName(t, "boundary_values")

	var v map[string]interface{}
	if err := json.Unmarshal(g.Generate(), &v); err != nil {
		t.Fatalf("json.Unmarshal failed: %v", err)
	}

	// json.Unmarshal decodes JSON numbers into float64 for interface{} targets.
	// 2^53+1 = 9007199254740993 cannot be represented exactly as float64;
	// it rounds to 9007199254740992 (2^53).
	raw, ok := v["pow2_53_plus1"]
	if !ok {
		t.Fatal("key pow2_53_plus1 not present in decoded object")
	}
	f, ok := raw.(float64)
	if !ok {
		t.Fatalf("pow2_53_plus1 decoded as %T; want float64", raw)
	}

	const exact = float64(9007199254740993) // This itself rounds to 2^53 at compile time.
	const pow2_53 = float64(1 << 53)

	// Demonstrate: the decoded value equals 2^53, not 2^53+1.
	if f != pow2_53 {
		t.Errorf("expected float64 precision loss: got %v; want %v (2^53)", f, pow2_53)
	}
	// Confirm the loss: f should equal pow2_53, and pow2_53+1 rounds back to pow2_53.
	if f != exact {
		// exact itself == pow2_53 due to float64 rounding; this is the point.
		t.Logf("precision loss confirmed: 2^53+1 decoded as %v (= 2^53 = %v)", f, pow2_53)
	}

	// Also verify 2^53 key survives without precision loss.
	raw53, ok := v["pow2_53"]
	if !ok {
		t.Fatal("key pow2_53 not present in decoded object")
	}
	f53 := raw53.(float64)
	if f53 != pow2_53 {
		t.Errorf("pow2_53 = %v; want %v", f53, pow2_53)
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

// ---------------------------------------------------------------------------
// 2. SpecialValues
// ---------------------------------------------------------------------------

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
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			if !bytes.Contains(payload, []byte(tt.token)) {
				t.Errorf("payload missing token %q\npayload: %s", tt.token, payload)
			}
		})
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

// ---------------------------------------------------------------------------
// 3. ScientificNotation
// ---------------------------------------------------------------------------

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

// Note: scientific notation chaos payloads contain 1e309 (overflow) which
// Go's encoding/json rejects outright — that is the chaos behavior we want
// to verify exists in the bytes. So these tests check the raw byte content
// directly rather than round-tripping through json.Unmarshal (which would
// be defeated by the very chaos we're trying to test).

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

// ---------------------------------------------------------------------------
// 4. TypeAmbiguity
// ---------------------------------------------------------------------------

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

	// Both 1.0 and 1 decode to float64(1) via interface{}.
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
		tc := tc
		t.Run(tc.key, func(t *testing.T) {
			got, ok := v[tc.key].(string)
			if !ok {
				t.Fatalf("key %q decoded as %T; want string", tc.key, v[tc.key])
			}
			if got != tc.want {
				t.Errorf("key %q = %q; want %q", tc.key, got, tc.want)
			}
		})
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

// ---------------------------------------------------------------------------
// Registry: all four generators registered
// ---------------------------------------------------------------------------

func TestRegistry_AllFourGeneratorsRegistered(t *testing.T) {
	wantNames := []string{
		"boundary_values",
		"special_values",
		"scientific_notation",
		"type_ambiguity",
	}
	for _, name := range wantNames {
		t.Run(name, func(t *testing.T) {
			if _, ok := numeric.Lookup(name); !ok {
				t.Errorf("generator %q not found in registry", name)
			}
		})
	}
}

func TestRegistry_AllCount(t *testing.T) {
	all := numeric.All()
	const wantCount = 4
	if len(all) != wantCount {
		t.Errorf("All() returned %d generators; want %d", len(all), wantCount)
	}
}

func TestRegistry_LookupUnknown(t *testing.T) {
	_, ok := numeric.Lookup("does_not_exist")
	if ok {
		t.Error("Lookup of unknown name returned ok=true; want false")
	}
}

func TestRegistry_AllGenerateNonEmpty(t *testing.T) {
	for _, g := range numeric.All() {
		g := g
		t.Run(g.Name(), func(t *testing.T) {
			payload := g.Generate()
			if len(payload) == 0 {
				t.Errorf("generator %q returned empty payload", g.Name())
			}
		})
	}
}
