package nulls_test

import (
	"bytes"
	"encoding/json"
	"sort"
	"testing"

	"github.com/dwsmith1983/chaos-data/chaosdata/nulls"
)

// ── NullVariants ─────────────────────────────────────────────────────────────

func TestNullVariants_ReturnsValidJSON(t *testing.T) {
	payload := nulls.NullVariants()
	if !json.Valid(payload) {
		t.Fatalf("NullVariants() produced invalid JSON: %s", payload)
	}
}

func TestNullVariants_IsArray(t *testing.T) {
	payload := nulls.NullVariants()
	var arr []json.RawMessage
	if err := json.Unmarshal(payload, &arr); err != nil {
		t.Fatalf("NullVariants() is not a JSON array: %v", err)
	}
}

func TestNullVariants_LiteralStringsPresent(t *testing.T) {
	payload := nulls.NullVariants()

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
			// Strip all whitespace-sensitive matching by looking for the raw
			// literal anywhere in the compact JSON.
			compact := compactJSON(t, payload)
			if !bytes.Contains(compact, []byte(tc.literal)) {
				t.Errorf("expected literal %q in payload\ngot: %s", tc.literal, compact)
			}
		})
	}
}

func TestNullVariants_RecordCount(t *testing.T) {
	payload := nulls.NullVariants()
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
	a := nulls.NullVariants()
	b := nulls.NullVariants()
	if !bytes.Equal(a, b) {
		t.Error("NullVariants() is not deterministic across successive calls")
	}
}

// ── NestedNulls ───────────────────────────────────────────────────────────────

func TestNestedNulls_ReturnsValidJSON(t *testing.T) {
	payload := nulls.NestedNulls()
	if !json.Valid(payload) {
		t.Fatalf("NestedNulls() produced invalid JSON: %s", payload)
	}
}

func TestNestedNulls_IsObject(t *testing.T) {
	payload := nulls.NestedNulls()
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(payload, &obj); err != nil {
		t.Fatalf("NestedNulls() is not a JSON object: %v", err)
	}
}

func TestNestedNulls_LiteralStringsPresent(t *testing.T) {
	payload := nulls.NestedNulls()
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
	payload := nulls.NestedNulls()
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(payload, &obj); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	const want = 5 // depth1, depth2, depth3, depth4, mixed
	if len(obj) != want {
		t.Errorf("NestedNulls() top-level key count = %d, want %d", len(obj), want)
	}
}

func TestNestedNulls_Determinism(t *testing.T) {
	// JSON object key ordering is not guaranteed by encoding/json for maps,
	// but the semantic content must be identical: unmarshal both and compare.
	a := nulls.NestedNulls()
	b := nulls.NestedNulls()

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

	// Compare re-marshalled forms; encoding/json is deterministic for the
	// same Go value passed twice.
	if !bytes.Equal(ra, rb) {
		t.Error("NestedNulls() semantic content is not deterministic across successive calls")
	}
}

// ── ArrayNulls ────────────────────────────────────────────────────────────────

func TestArrayNulls_ReturnsValidJSON(t *testing.T) {
	payload := nulls.ArrayNulls()
	if !json.Valid(payload) {
		t.Fatalf("ArrayNulls() produced invalid JSON: %s", payload)
	}
}

func TestArrayNulls_IsObject(t *testing.T) {
	payload := nulls.ArrayNulls()
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(payload, &obj); err != nil {
		t.Fatalf("ArrayNulls() is not a JSON object: %v", err)
	}
}

func TestArrayNulls_LiteralStringsPresent(t *testing.T) {
	payload := nulls.ArrayNulls()
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
	payload := nulls.ArrayNulls()
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
	payload := nulls.ArrayNulls()
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
	a := nulls.ArrayNulls()
	b := nulls.ArrayNulls()

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

// ── Registry ──────────────────────────────────────────────────────────────────

func TestRegistry_AllGeneratorsRegistered(t *testing.T) {
	want := []string{"array-nulls", "nested-nulls", "null-variants"}
	sort.Strings(want)

	got := nulls.Registry.Names()
	sort.Strings(got)

	if len(got) != len(want) {
		t.Fatalf("Registry.Names() = %v, want %v", got, want)
	}
	for i, name := range want {
		if got[i] != name {
			t.Errorf("Registry.Names()[%d] = %q, want %q", i, got[i], name)
		}
	}
}

func TestRegistry_GetKnownGenerator(t *testing.T) {
	cases := []string{"null-variants", "nested-nulls", "array-nulls"}
	for _, name := range cases {
		name := name
		t.Run(name, func(t *testing.T) {
			g, ok := nulls.Registry.Get(name)
			if !ok {
				t.Fatalf("Registry.Get(%q) not found", name)
			}
			payload := g()
			if len(payload) == 0 {
				t.Errorf("generator %q returned empty payload", name)
			}
			if !json.Valid(payload) {
				t.Errorf("generator %q returned invalid JSON: %s", name, payload)
			}
		})
	}
}

func TestRegistry_GetUnknownGeneratorReturnsFalse(t *testing.T) {
	_, ok := nulls.Registry.Get("does-not-exist")
	if ok {
		t.Error("expected ok=false for unknown generator name")
	}
}

// ── helpers ───────────────────────────────────────────────────────────────────

// compactJSON re-encodes b through json.Compact so literal-string matching is
// not sensitive to the whitespace choices of the encoder.
func compactJSON(t *testing.T, b []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := json.Compact(&buf, b); err != nil {
		t.Fatalf("compactJSON: %v", err)
	}
	return buf.Bytes()
}
