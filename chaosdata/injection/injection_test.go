package injection_test

import (
	"strings"
	"testing"

	"github.com/dwsmith1983/chaos-data/chaosdata"
	"github.com/dwsmith1983/chaos-data/chaosdata/injection"
)

// ---------------------------------------------------------------------------
// Helper
// ---------------------------------------------------------------------------

func requireNoError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func generate(t *testing.T, count int) chaosdata.Payload {
	t.Helper()
	g := injection.InjectionGenerator{}
	p, err := g.Generate(chaosdata.GenerateOpts{Count: count})
	requireNoError(t, err)
	return p
}

// ---------------------------------------------------------------------------
// Metadata tests
// ---------------------------------------------------------------------------

func TestName(t *testing.T) {
	g := injection.InjectionGenerator{}
	if got := g.Name(); got != "injection" {
		t.Errorf("Name() = %q; want %q", got, "injection")
	}
}

func TestCategory(t *testing.T) {
	g := injection.InjectionGenerator{}
	if got := g.Category(); got != "security" {
		t.Errorf("Category() = %q; want %q", got, "security")
	}
}

// ---------------------------------------------------------------------------
// Payload type and structure
// ---------------------------------------------------------------------------

func TestPayloadType(t *testing.T) {
	p := generate(t, 0)
	if p.Type != "injection" {
		t.Errorf("Payload.Type = %q; want %q", p.Type, "injection")
	}
}

func TestPayloadDataNonEmpty(t *testing.T) {
	p := generate(t, 0)
	if len(p.Data) == 0 {
		t.Error("Payload.Data must not be empty")
	}
}

func TestPayloadAttributesPopulated(t *testing.T) {
	p := generate(t, 3)
	if len(p.Attributes) == 0 {
		t.Error("Payload.Attributes must not be empty")
	}
}

// ---------------------------------------------------------------------------
// Count behaviour
// ---------------------------------------------------------------------------

func TestCountZeroReturnsFullCatalogue(t *testing.T) {
	p0 := generate(t, 0)
	pFull := generate(t, -1)
	if string(p0.Data) != string(pFull.Data) {
		t.Error("Count=0 and Count=-1 should both return the full catalogue")
	}
}

func TestCountRespected(t *testing.T) {
	for _, count := range []int{1, 3, 5} {
		count := count
		t.Run("count="+string(rune('0'+count)), func(t *testing.T) {
			p := generate(t, count)
			lines := strings.Split(strings.TrimRight(string(p.Data), "\n"), "\n")
			if len(lines) != count {
				t.Errorf("expected %d lines, got %d", count, len(lines))
			}
			if len(p.Attributes) != count {
				t.Errorf("expected %d attributes, got %d", count, len(p.Attributes))
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Dangerous substring assertions (table-driven)
// ---------------------------------------------------------------------------

func TestDangerousSubstrings(t *testing.T) {
	tests := []struct {
		name     string
		category string
		needle   string
	}{
		// SQL injection
		{"sql_drop_table", "SQL injection", "DROP TABLE"},
		{"sql_or_one_equals_one", "SQL injection", "1 OR 1=1"},
		{"sql_comment", "SQL injection", "--"},

		// XSS
		{"xss_script_tag", "XSS", "<script>alert(1)</script>"},
		{"xss_img_onerror", "XSS", "onerror=alert(1)"},
		{"xss_svg_onload", "XSS", "onload=alert(1)"},
		{"xss_javascript_uri", "XSS", "javascript:alert"},

		// Template injection
		{"tpl_jinja", "template injection", "{{7*7}}"},
		{"tpl_el", "template injection", "${7*7}"},

		// LDAP injection
		{"ldap_wildcard", "LDAP injection", "*)(objectClass=*"},

		// Path traversal
		{"path_traversal_unix", "path traversal", "../../etc/passwd"},
		{"path_traversal_win", "path traversal", `..\..\windows`},

		// Command injection
		{"cmd_rm_rf", "command injection", "rm -rf /"},
		{"cmd_subshell_passwd", "command injection", "$(cat /etc/passwd)"},
		{"cmd_backtick", "command injection", "`id`"},

		// JSON injection
		{"json_unescaped_quote", "JSON injection", `"key": "va"lue"`},
		{"json_break_out", "JSON injection", `"admin":true`},

		// Log injection
		{"log_newline_lf", "log injection", "\nINFO: injected log line"},
		{"log_newline_crlf", "log injection", "\r\nINFO: injected log line"},
		{"log_ansi_escape", "log injection", "\x1b[31m"},

		// Header injection (CRLF)
		{"header_crlf_set_cookie", "header injection", "Set-Cookie: session=evil"},
		{"header_crlf_location", "header injection", "Location: https://evil.example.com"},
		{"header_crlf_header", "header injection", "X-Injected: bar"},
	}

	// Generate the full catalogue once.
	p := generate(t, 0)
	body := string(p.Data)

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			if !strings.Contains(body, tt.needle) {
				t.Errorf("[%s] payload does not contain %q", tt.category, tt.needle)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Determinism
// ---------------------------------------------------------------------------

func TestGenerateIsDeterministic(t *testing.T) {
	g := injection.InjectionGenerator{}
	opts := chaosdata.GenerateOpts{Count: 10}

	p1, err := g.Generate(opts)
	requireNoError(t, err)

	p2, err := g.Generate(opts)
	requireNoError(t, err)

	if string(p1.Data) != string(p2.Data) {
		t.Error("Generate is not deterministic: two calls with the same opts returned different data")
	}
}

// ---------------------------------------------------------------------------
// Registry self-registration
// ---------------------------------------------------------------------------

func TestSelfRegistration(t *testing.T) {
	// The init() in the injection package registers InjectionGenerator.
	// Importing the package is sufficient; we verify it appears in the registry.
	found := false
	for _, g := range chaosdata.All() {
		if g.Name() == "injection" && g.Category() == "security" {
			found = true
			break
		}
	}
	if !found {
		t.Error("InjectionGenerator was not found in the chaosdata registry after import")
	}
}

// ---------------------------------------------------------------------------
// Interface compliance (compile-time)
// ---------------------------------------------------------------------------

var _ chaosdata.ChaosGenerator = injection.InjectionGenerator{}
