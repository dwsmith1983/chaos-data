package nulls

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/dwsmith1983/chaos-data/chaosdata"
)

// NullsGenerator tests

func TestNullsGenerator_Category(t *testing.T) {
	gen := &NullsGenerator{}
	if gen.Category() != "nulls" {
		t.Errorf("expected 'nulls', got '%s'", gen.Category())
	}
}

func TestNullsGenerator_Generate(t *testing.T) {
	gen := &NullsGenerator{}
	vals, err := gen.Generate(chaosdata.GenerateOpts{Count: 1})
	if err != nil {
		t.Fatalf("Generate() err = %v", err)
	}

	expectedDesc := []string{
		"nil",
		"empty string",
		"zero-length slice",
		"zero-length map",
		"string literal null",
		"string literal NULL",
		"string literal nil",
		"string literal None",
		"string literal undefined",
		"Unicode null",
		"null byte in middle of string",
		"sql.NullString Valid=false",
		"nested object with null field",
		"doubly-nested null",
		"triply-nested null",
		"mixed null branches",
		"array with null elements",
		"array of all nulls",
		"mixed types with null",
		"single null element array",
		"empty array",
		"sparse nulls in array",
	}

	found := make(map[string]bool)
	var parsed []map[string]any
	if err := json.Unmarshal(vals.Data, &parsed); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}
	for _, v := range parsed {
		if typ, ok := v["type"].(string); ok {
			found[typ] = true
		}
	}

	for _, desc := range expectedDesc {
		if !found[desc] {
			t.Errorf("Missing expected chaos value with description: %s", desc)
		}
	}
}

// Low-level generator tests

func TestNullVariants_ReturnsValidJSON(t *testing.T) {
	payload := NullVariants()
	if !json.Valid(payload) {
		t.Fatalf("NullVariants() produced invalid JSON: %s", payload)
	}
}

func TestNullVariants_IsArray(t *testing.T) {
	payload := NullVariants()
	var arr []json.RawMessage
	if err := json.Unmarshal(payload, &arr); err != nil {
		t.Fatalf("NullVariants() is not a JSON array: %v", err)
	}
}

func TestNullVariants_LiteralStringsPresent(t *testing.T) {
	payload := NullVariants()

	cases := []struct {
		name    string
		literal string
	}{
		{"literal null value", `"field":null`},
		{"empty string", `"field":""`},
		{"string null", `"field":"null"`},
		{"string NULL", `"field":"NULL"`},
		{"string None", `"field":"None"`},
		{"string nil", `"field":"nil"`},
		{"missing key sentinel", `"missing-key"`},
		{"explicit null variant", `"explicit-null"`},
		{"absent field variant", `"absent-field"`},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			compact := compactJSON(t, payload)
			if !bytes.Contains(compact, []byte(tc.literal)) {
				t.Errorf("expected literal %q in payload\ngot: %s", tc.literal, compact)
			}
		})
	}
}

func TestNullVariants_RecordCount(t *testing.T) {
	payload := NullVariants()
	var arr []json.RawMessage
	if err := json.Unmarshal(payload, &arr); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	const want = 9
	if len(arr) != want {
		t.Errorf("NullVariants() record count = %d, want %d", len(arr), want)
	}
}

func TestNullVariants_Determinism(t *testing.T) {
	a := NullVariants()
	b := NullVariants()
	if !bytes.Equal(a, b) {
		t.Error("NullVariants() is not deterministic across successive calls")
	}
}

func TestNestedNulls_ReturnsValidJSON(t *testing.T) {
	payload := NestedNulls()
	if !json.Valid(payload) {
		t.Fatalf("NestedNulls() produced invalid JSON: %s", payload)
	}
}

func TestNestedNulls_IsObject(t *testing.T) {
	payload := NestedNulls()
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(payload, &obj); err != nil {
		t.Fatalf("NestedNulls() is not a JSON object: %v", err)
	}
}

func TestNestedNulls_LiteralStringsPresent(t *testing.T) {
	payload := NestedNulls()
	compact := compactJSON(t, payload)

	cases := []struct {
		name    string
		literal string
	}{
		{"depth1 null", `"depth1":null`},
		{"depth2 child null", `"child":null`},
		{"depth3 key present", `"depth3"`},
		{"depth4 key present", `"depth4"`},
		{"great_grandchild null", `"great_grandchild":null`},
		{"mixed null_branch", `"null_branch":null`},
		{"mixed value_branch leaf null", `"leaf":null`},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if !bytes.Contains(compact, []byte(tc.literal)) {
				t.Errorf("expected literal %q in payload\ngot: %s", tc.literal, compact)
			}
		})
	}
}

func TestNestedNulls_TopLevelKeyCount(t *testing.T) {
	payload := NestedNulls()
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(payload, &obj); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	const want = 5
	if len(obj) != want {
		t.Errorf("NestedNulls() top-level key count = %d, want %d", len(obj), want)
	}
}

func TestNestedNulls_Determinism(t *testing.T) {
	a := NestedNulls()
	b := NestedNulls()

	var objA, objB any
	if err := json.Unmarshal(a, &objA); err != nil {
		t.Fatalf("unmarshal a: %v", err)
	}
	if err := json.Unmarshal(b, &objB); err != nil {
		t.Fatalf("unmarshal b: %v", err)
	}

	ra, err := json.Marshal(objA)
	if err != nil {
		t.Fatalf("re-marshal a: %v", err)
	}
	rb, err := json.Marshal(objB)
	if err != nil {
		t.Fatalf("re-marshal b: %v", err)
	}

	if !bytes.Equal(ra, rb) {
		t.Error("NestedNulls() semantic content is not deterministic across successive calls")
	}
}

func TestArrayNulls_ReturnsValidJSON(t *testing.T) {
	payload := ArrayNulls()
	if !json.Valid(payload) {
		t.Fatalf("ArrayNulls() produced invalid JSON: %s", payload)
	}
}

func TestArrayNulls_IsObject(t *testing.T) {
	payload := ArrayNulls()
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(payload, &obj); err != nil {
		t.Fatalf("ArrayNulls() is not a JSON object: %v", err)
	}
}

func TestArrayNulls_LiteralStringsPresent(t *testing.T) {
	payload := ArrayNulls()
	compact := compactJSON(t, payload)

	cases := []struct {
		name    string
		literal string
	}{
		{"null_elements key", `"null_elements"`},
		{"all_null key", `"all_null"`},
		{"mixed_types key", `"mixed_types"`},
		{"sparse_nulls key", `"sparse_nulls"`},
		{"single_null key", `"single_null"`},
		{"empty key", `"empty"`},
		{"null in null_elements", `1,null,2`},
		{"all_null content", `[null,null,null]`},
		{"single_null content", `[null]`},
		{"empty array content", `[]`},
		{"mixed_types has null", `true,null,`},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if !bytes.Contains(compact, []byte(tc.literal)) {
				t.Errorf("expected literal %q in payload\ngot: %s", tc.literal, compact)
			}
		})
	}
}

func TestArrayNulls_NullElementsHasTwoNulls(t *testing.T) {
	payload := ArrayNulls()
	var obj map[string][]any
	if err := json.Unmarshal(payload, &obj); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	ne, ok := obj["null_elements"]
	if !ok {
		t.Fatal("null_elements key missing")
	}
	count := 0
	for _, v := range ne {
		if v == nil {
			count++
		}
	}
	if count != 2 {
		t.Errorf("null_elements null count = %d, want 2", count)
	}
}

func TestArrayNulls_AllNullEveryElementIsNull(t *testing.T) {
	payload := ArrayNulls()
	var obj map[string][]any
	if err := json.Unmarshal(payload, &obj); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	an, ok := obj["all_null"]
	if !ok {
		t.Fatal("all_null key missing")
	}
	if len(an) == 0 {
		t.Fatal("all_null must not be empty")
	}
	for i, v := range an {
		if v != nil {
			t.Errorf("all_null[%d] = %v, want nil", i, v)
		}
	}
}

func TestArrayNulls_Determinism(t *testing.T) {
	a := ArrayNulls()
	b := ArrayNulls()

	var objA, objB any
	if err := json.Unmarshal(a, &objA); err != nil {
		t.Fatalf("unmarshal a: %v", err)
	}
	if err := json.Unmarshal(b, &objB); err != nil {
		t.Fatalf("unmarshal b: %v", err)
	}

	ra, _ := json.Marshal(objA)
	rb, _ := json.Marshal(objB)

	if !bytes.Equal(ra, rb) {
		t.Error("ArrayNulls() semantic content is not deterministic across successive calls")
	}
}

// Helper function

func compactJSON(t *testing.T, b []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := json.Compact(&buf, b); err != nil {
		t.Fatalf("compactJSON: %v", err)
	}
	return buf.Bytes()
}
