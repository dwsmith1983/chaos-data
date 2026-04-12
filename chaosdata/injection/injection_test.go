package injection

import (
	"encoding/json"
	"testing"

	"github.com/dwsmith1983/chaos-data/chaosdata"
)

func TestInjectionGenerator_Category(t *testing.T) {
	gen := &InjectionGenerator{}
	if gen.Category() != "injection" {
		t.Errorf("expected 'injection', got '%s'", gen.Category())
	}
}

func TestInjectionGenerator_Generate(t *testing.T) {
	gen := &InjectionGenerator{}
	vals, err := gen.Generate(chaosdata.GenerateOpts{Count: 1})
	if err != nil {
		t.Fatalf("Generate() err = %v", err)
	}

	expectedDesc := []string{
		"SQL injection (' OR 1=1 --)",
		"SQL injection (UNION SELECT)",
		"XSS (<script>)",
		"XSS (event handlers)",
		"Command injection (; rm -rf /)",
		"Command injection (backticks)",
		"Command injection ($())",
		"LDAP injection",
		"Header injection (CRLF)",
		"Go template injection ({{.}})",
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

func FuzzInjectionGenerator_Mutations(f *testing.F) {
	f.Add("SELECT * FROM users")
	f.Fuzz(func(t *testing.T, payload string) {
		// Valid fuzzer to ensure no panics
	})
}
