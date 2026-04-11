package injection

import (
	"context"
	"testing"
)

func TestInjectionGenerator_Category(t *testing.T) {
	gen := &InjectionGenerator{}
	if gen.Category() != "injection" {
		t.Errorf("expected 'injection', got '%s'", gen.Category())
	}
}

func TestInjectionGenerator_Generate(t *testing.T) {
	gen := &InjectionGenerator{}
	vals, err := gen.Generate(context.Background())
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
	for _, v := range vals {
		found[v.Description()] = true
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
		t.Errorf("Not implemented: fuzz string mutations for injections to verify no panics")
	})
}
