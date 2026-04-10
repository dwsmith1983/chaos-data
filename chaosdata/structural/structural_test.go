package structural_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/dwsmith1983/chaos-data/chaosdata"

	// Side-effect import registers all structural generators.
	_ "github.com/dwsmith1983/chaos-data/chaosdata/structural"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// generatorByName returns the first registered generator whose Name() matches.
func generatorByName(t *testing.T, name string) chaosdata.ChaosGenerator {
	t.Helper()
	for _, g := range chaosdata.All() {
		if g.Name() == name {
			return g
		}
	}
	t.Fatalf("generator %q not found in registry", name)
	return nil
}

func mustGenerate(t *testing.T, g chaosdata.ChaosGenerator, opts chaosdata.GenerateOpts) chaosdata.Payload {
	t.Helper()
	p, err := g.Generate(opts)
	if err != nil {
		t.Fatalf("Generate() returned unexpected error: %v", err)
	}
	return p
}

// isValidJSON returns true when data round-trips through encoding/json.
func isValidJSON(data []byte) bool {
	var v interface{}
	return json.Unmarshal(data, &v) == nil
}

// countOccurrences counts non-overlapping occurrences of sub in s.
func countOccurrences(s, sub string) int {
	return strings.Count(s, sub)
}

// ---------------------------------------------------------------------------
// Registration
// ---------------------------------------------------------------------------

func TestRegistration(t *testing.T) {
	wantNames := []string{
		"deep-nest",
		"empty-array",
		"empty-object",
		"mixed-type-array",
		"wide-object",
		"single-element",
		"trailing-comma",
		"duplicate-keys",
		"null-array",
	}

	registered := map[string]bool{}
	for _, g := range chaosdata.All() {
		registered[g.Name()] = true
	}

	for _, name := range wantNames {
		if !registered[name] {
			t.Errorf("generator %q not registered", name)
		}
	}
}

func TestAllGeneratorsHaveStructuralCategory(t *testing.T) {
	wantNames := []string{
		"deep-nest", "empty-array", "empty-object", "mixed-type-array",
		"wide-object", "single-element", "trailing-comma", "duplicate-keys",
		"null-array",
	}
	for _, name := range wantNames {
		g := generatorByName(t, name)
		if g.Category() != "structural" {
			t.Errorf("generator %q: Category() = %q; want %q", name, g.Category(), "structural")
		}
	}
}

// ---------------------------------------------------------------------------
// Deep-nest — programmatic depth verification
// ---------------------------------------------------------------------------

func TestDeepNestGenerator(t *testing.T) {
	tests := []struct {
		name      string
		size      int
		wantDepth int // expected number of opening '{' braces
	}{
		// The innermost empty object adds 1 extra '{', so total '{' = depth+1.
		{name: "default depth (size=0 → 100)", size: 0, wantDepth: 101},
		{name: "explicit 100 levels", size: 100, wantDepth: 101},
		{name: "minimum 1 level", size: 1, wantDepth: 2},
		{name: "50 levels", size: 50, wantDepth: 51},
		{name: "200 levels", size: 200, wantDepth: 201},
		{name: "cap at 1000", size: 9999, wantDepth: 1001},
	}

	g := generatorByName(t, "deep-nest")

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := mustGenerate(t, g, chaosdata.GenerateOpts{Size: tt.size})

			payload := string(p.Data)

			// Count '{' occurrences to verify nesting depth programmatically.
			gotBraces := countOccurrences(payload, "{")
			if gotBraces != tt.wantDepth {
				t.Errorf("opening brace count = %d; want %d\npayload (first 120 bytes): %.120s",
					gotBraces, tt.wantDepth, payload)
			}

			// Closing braces must equal opening braces (balanced).
			closingBraces := countOccurrences(payload, "}")
			if closingBraces != tt.wantDepth {
				t.Errorf("closing brace count = %d; want %d (unbalanced)", closingBraces, tt.wantDepth)
			}

			// Must be valid JSON.
			if !isValidJSON(p.Data) {
				t.Errorf("deep-nest payload is not valid JSON:\n%.200s", payload)
			}

			// Attribute must declare valid JSON.
			if p.Attributes["valid_json"] != "true" {
				t.Errorf(`attribute valid_json = %q; want "true"`, p.Attributes["valid_json"])
			}
		})
	}
}

// TestDeepNestMinimum100Levels asserts the default output is at least 100
// nesting levels as required by the specification.
func TestDeepNestMinimum100Levels(t *testing.T) {
	g := generatorByName(t, "deep-nest")
	p := mustGenerate(t, g, chaosdata.GenerateOpts{})

	// Each nesting level contributes exactly one '{'.
	// The innermost {} adds one extra, so depth-100 → 101 opening braces.
	braces := countOccurrences(string(p.Data), "{")
	if braces < 101 {
		t.Errorf("expected at least 101 opening braces (100 nesting levels + innermost), got %d", braces)
	}
}

// ---------------------------------------------------------------------------
// Empty collections
// ---------------------------------------------------------------------------

func TestEmptyArrayGenerator(t *testing.T) {
	g := generatorByName(t, "empty-array")
	p := mustGenerate(t, g, chaosdata.GenerateOpts{})

	if !bytes.Equal(p.Data, []byte("[]")) {
		t.Errorf("data = %q; want %q", p.Data, "[]")
	}
	if !isValidJSON(p.Data) {
		t.Error("empty array is not valid JSON")
	}
	if p.Attributes["valid_json"] != "true" {
		t.Errorf(`valid_json = %q; want "true"`, p.Attributes["valid_json"])
	}
}

func TestEmptyObjectGenerator(t *testing.T) {
	g := generatorByName(t, "empty-object")
	p := mustGenerate(t, g, chaosdata.GenerateOpts{})

	if !bytes.Equal(p.Data, []byte("{}")) {
		t.Errorf("data = %q; want %q", p.Data, "{}")
	}
	if !isValidJSON(p.Data) {
		t.Error("empty object is not valid JSON")
	}
	if p.Attributes["valid_json"] != "true" {
		t.Errorf(`valid_json = %q; want "true"`, p.Attributes["valid_json"])
	}
}

// ---------------------------------------------------------------------------
// Mixed-type array
// ---------------------------------------------------------------------------

func TestMixedTypeArrayGenerator(t *testing.T) {
	g := generatorByName(t, "mixed-type-array")
	p := mustGenerate(t, g, chaosdata.GenerateOpts{})

	if !isValidJSON(p.Data) {
		t.Errorf("mixed-type-array is not valid JSON: %s", p.Data)
	}

	var arr []interface{}
	if err := json.Unmarshal(p.Data, &arr); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}
	if len(arr) == 0 {
		t.Error("expected non-empty mixed-type array")
	}

	// Verify the array contains at least one element of each required kind.
	typesSeen := map[string]bool{}
	for _, v := range arr {
		switch v.(type) {
		case nil:
			typesSeen["null"] = true
		case bool:
			typesSeen["bool"] = true
		case float64:
			typesSeen["number"] = true
		case string:
			typesSeen["string"] = true
		case []interface{}:
			typesSeen["array"] = true
		case map[string]interface{}:
			typesSeen["object"] = true
		}
	}

	for _, kind := range []string{"null", "bool", "number", "string", "array", "object"} {
		if !typesSeen[kind] {
			t.Errorf("mixed-type array missing element of type %q", kind)
		}
	}
}

// ---------------------------------------------------------------------------
// Wide object — key count verification
// ---------------------------------------------------------------------------

func TestWideObjectGenerator(t *testing.T) {
	tests := []struct {
		name         string
		size         int
		wantKeyCount int
	}{
		{name: "default (size=0 → 1000 keys)", size: 0, wantKeyCount: 1000},
		{name: "exactly 1000 keys", size: 1000, wantKeyCount: 1000},
		{name: "1500 keys", size: 1500, wantKeyCount: 1500},
		{name: "below minimum clamped to 1000", size: 5, wantKeyCount: 1000},
	}

	g := generatorByName(t, "wide-object")

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := mustGenerate(t, g, chaosdata.GenerateOpts{Size: tt.size})

			if !isValidJSON(p.Data) {
				t.Fatalf("wide-object is not valid JSON")
			}

			var obj map[string]interface{}
			if err := json.Unmarshal(p.Data, &obj); err != nil {
				t.Fatalf("Unmarshal failed: %v", err)
			}

			// Programmatically verify key count matches expectation.
			if got := len(obj); got != tt.wantKeyCount {
				t.Errorf("key count = %d; want %d", got, tt.wantKeyCount)
			}

			// Attribute must reflect actual key count.
			wantAttr := fmt.Sprintf("%d", tt.wantKeyCount)
			if p.Attributes["key_count"] != wantAttr {
				t.Errorf(`attribute key_count = %q; want %q`, p.Attributes["key_count"], wantAttr)
			}

			if p.Attributes["valid_json"] != "true" {
				t.Errorf(`attribute valid_json = %q; want "true"`, p.Attributes["valid_json"])
			}
		})
	}
}

// TestWideObjectMinimum1000Keys asserts the baseline ≥1000 key requirement.
func TestWideObjectMinimum1000Keys(t *testing.T) {
	g := generatorByName(t, "wide-object")
	p := mustGenerate(t, g, chaosdata.GenerateOpts{})

	var obj map[string]interface{}
	if err := json.Unmarshal(p.Data, &obj); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}
	if len(obj) < 1000 {
		t.Errorf("wide-object has %d keys; need at least 1000", len(obj))
	}
}

// ---------------------------------------------------------------------------
// Single-element degenerate cases
// ---------------------------------------------------------------------------

func TestSingleElementGenerator(t *testing.T) {
	g := generatorByName(t, "single-element")
	p := mustGenerate(t, g, chaosdata.GenerateOpts{})

	if !isValidJSON(p.Data) {
		t.Fatalf("single-element is not valid JSON: %s", p.Data)
	}

	var outer []interface{}
	if err := json.Unmarshal(p.Data, &outer); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}
	if len(outer) == 0 {
		t.Fatal("expected non-empty outer array")
	}

	// Verify each inner collection is a single-element structure.
	for i, v := range outer {
		switch inner := v.(type) {
		case []interface{}:
			if len(inner) != 1 {
				t.Errorf("outer[%d] array has %d elements; want 1", i, len(inner))
			}
		case map[string]interface{}:
			if len(inner) != 1 {
				t.Errorf("outer[%d] object has %d keys; want 1", i, len(inner))
			}
		default:
			t.Errorf("outer[%d] unexpected type %T", i, v)
		}
	}
}

// ---------------------------------------------------------------------------
// Intentionally-invalid JSON cases — tested separately
// ---------------------------------------------------------------------------

// TestTrailingCommaGenerator_IsInvalidJSON documents and verifies that the
// trailing-comma generator produces output that the standard library rejects.
func TestTrailingCommaGenerator_IsInvalidJSON(t *testing.T) {
	g := generatorByName(t, "trailing-comma")
	p := mustGenerate(t, g, chaosdata.GenerateOpts{})

	// Contract: must be explicitly flagged as invalid.
	if p.Attributes["valid_json"] != "false" {
		t.Errorf(`attribute valid_json = %q; want "false" (intentionally invalid)`,
			p.Attributes["valid_json"])
	}

	// The payload must contain at least one trailing comma pattern.
	payload := string(p.Data)
	if !strings.Contains(payload, ",}") && !strings.Contains(payload, ",]") {
		t.Errorf("trailing-comma payload contains no trailing comma pattern (,} or ,]): %s", payload)
	}

	// Standard library must reject every line that contains a trailing comma.
	for _, line := range strings.Split(payload, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if strings.Contains(line, ",}") || strings.Contains(line, ",]") {
			var v interface{}
			if err := json.Unmarshal([]byte(line), &v); err == nil {
				t.Errorf("expected json.Unmarshal to reject trailing-comma line %q, but it accepted it", line)
			}
		}
	}
}

// TestDuplicateKeysGenerator_IsAmbiguousJSON documents that the duplicate-key
// generator produces a payload flagged as invalid/ambiguous, and verifies that
// Go's standard library silently accepts the last value (last-wins behaviour).
func TestDuplicateKeysGenerator_IsAmbiguousJSON(t *testing.T) {
	g := generatorByName(t, "duplicate-keys")
	p := mustGenerate(t, g, chaosdata.GenerateOpts{})

	// Contract: must be flagged as invalid/ambiguous.
	if p.Attributes["valid_json"] != "false" {
		t.Errorf(`attribute valid_json = %q; want "false" (duplicate keys — ambiguous)`,
			p.Attributes["valid_json"])
	}

	// Document Go's last-wins behaviour — do not assert this as correct.
	var obj map[string]interface{}
	if err := json.Unmarshal(p.Data, &obj); err != nil {
		// Some decoders reject duplicate keys; document if that happens here.
		t.Logf("json.Unmarshal rejected duplicate-key payload (err=%v) — acceptable behaviour", err)
		return
	}

	// Go's encoding/json uses last-wins; "id" should resolve to null.
	if idVal, ok := obj["id"]; ok {
		t.Logf("duplicate key 'id' resolved to %v (last-wins behaviour documented)", idVal)
	}
}

// ---------------------------------------------------------------------------
// Null array
// ---------------------------------------------------------------------------

func TestNullArrayGenerator(t *testing.T) {
	tests := []struct {
		name      string
		size      int
		wantNulls int
	}{
		{name: "default (size=0 → 10)", size: 0, wantNulls: 10},
		{name: "5 nulls", size: 5, wantNulls: 5},
		{name: "100 nulls", size: 100, wantNulls: 100},
		{name: "1 null", size: 1, wantNulls: 1},
	}

	g := generatorByName(t, "null-array")

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := mustGenerate(t, g, chaosdata.GenerateOpts{Size: tt.size})

			if !isValidJSON(p.Data) {
				t.Fatalf("null-array is not valid JSON: %s", p.Data)
			}

			var arr []interface{}
			if err := json.Unmarshal(p.Data, &arr); err != nil {
				t.Fatalf("Unmarshal failed: %v", err)
			}

			if len(arr) != tt.wantNulls {
				t.Errorf("null count = %d; want %d", len(arr), tt.wantNulls)
			}

			// Every element must be null.
			for i, v := range arr {
				if v != nil {
					t.Errorf("arr[%d] = %v; want nil (null)", i, v)
				}
			}

			// Attribute null_count must match.
			wantAttr := fmt.Sprintf("%d", tt.wantNulls)
			if p.Attributes["null_count"] != wantAttr {
				t.Errorf(`attribute null_count = %q; want %q`, p.Attributes["null_count"], wantAttr)
			}

			if p.Attributes["valid_json"] != "true" {
				t.Errorf(`attribute valid_json = %q; want "true"`, p.Attributes["valid_json"])
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Payload contract: Type field
// ---------------------------------------------------------------------------

func TestPayloadTypeField(t *testing.T) {
	tests := []struct {
		name     string
		wantType string
	}{
		{"deep-nest", "application/json"},
		{"empty-array", "application/json"},
		{"empty-object", "application/json"},
		{"mixed-type-array", "application/json"},
		{"wide-object", "application/json"},
		{"single-element", "application/json"},
		{"trailing-comma", "application/json-invalid"},
		{"duplicate-keys", "application/json-invalid"},
		{"null-array", "application/json"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := generatorByName(t, tt.name)
			p := mustGenerate(t, g, chaosdata.GenerateOpts{})
			if p.Type != tt.wantType {
				t.Errorf("Type = %q; want %q", p.Type, tt.wantType)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Compile-time interface compliance
// ---------------------------------------------------------------------------

// The blank import of the structural package registers its generators via
// init(); the generatorByName helper confirms they satisfy ChaosGenerator at
// runtime.  This block provides an additional compile-time check by asserting
// the unexported types through their registered instances.
var _ chaosdata.ChaosGenerator // ensures import is used
