package dagster_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/dwsmith1983/chaos-data/adapters/dagster"
)

func TestConfig_Validate_EmptyURL(t *testing.T) {
	t.Parallel()

	cfg := dagster.Config{}
	cfg.Defaults()

	err := cfg.Validate()
	if err == nil {
		t.Fatal("Validate() = nil, want error for empty URL")
	}
	if !strings.Contains(err.Error(), "URL is required") {
		t.Errorf("Validate() error = %q, want message containing %q", err.Error(), "URL is required")
	}
}

func TestConfig_Validate_RepositoryNameWithoutLocationName(t *testing.T) {
	t.Parallel()

	cfg := dagster.Config{
		URL:            "http://localhost:3000/graphql",
		RepositoryName: "my_repo",
		// RepositoryLocationName intentionally left empty
	}
	cfg.Defaults()

	err := cfg.Validate()
	if err == nil {
		t.Fatal("Validate() = nil, want error when RepositoryName set without RepositoryLocationName")
	}
	if !strings.Contains(err.Error(), "RepositoryLocationName is required") {
		t.Errorf("Validate() error = %q, want message containing %q", err.Error(), "RepositoryLocationName is required")
	}
}

func TestConfig_Validate_InvalidURL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		url  string
	}{
		{name: "no scheme", url: "not-a-url"},
		{name: "ftp scheme", url: "ftp://host"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := dagster.Config{URL: tt.url}
			cfg.Defaults()

			err := cfg.Validate()
			if err == nil {
				t.Fatalf("Validate() = nil, want error for URL %q", tt.url)
			}
			if !strings.Contains(err.Error(), "valid http or https URL") {
				t.Errorf("Validate() error = %q, want message containing %q", err.Error(), "valid http or https URL")
			}
		})
	}
}

func TestConfig_Validate_URLOnly(t *testing.T) {
	t.Parallel()

	cfg := dagster.Config{
		URL: "http://localhost:3000/graphql",
	}
	cfg.Defaults()

	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() = %v, want nil for URL-only config", err)
	}
}

func TestConfig_Validate_URLWithBothRepoFields(t *testing.T) {
	t.Parallel()

	cfg := dagster.Config{
		URL:                    "http://localhost:3000/graphql",
		RepositoryLocationName: "my_location",
		RepositoryName:         "my_repo",
	}
	cfg.Defaults()

	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() = %v, want nil when both repo fields are set", err)
	}
}

func TestConfig_Validate_URLWithHeadersAndBothRepoFields(t *testing.T) {
	t.Parallel()

	cfg := dagster.Config{
		URL: "http://dagster.example.com/graphql",
		Headers: map[string]string{
			"Dagster-Cloud-Api-Token": "secret-token",
			"X-Custom-Header":         "value",
		},
		RepositoryLocationName: "prod_location",
		RepositoryName:         "prod_repo",
	}
	cfg.Defaults()

	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() = %v, want nil with URL, headers, and both repo fields", err)
	}
}

func TestConfig_Defaults_SetsStateDSN(t *testing.T) {
	t.Parallel()

	cfg := dagster.Config{}
	cfg.Defaults()

	if cfg.StateDSN != ":memory:" {
		t.Errorf("Defaults() StateDSN = %q, want %q", cfg.StateDSN, ":memory:")
	}
}

func TestConfig_Defaults_PreservesExplicitStateDSN(t *testing.T) {
	t.Parallel()

	cfg := dagster.Config{StateDSN: "/var/dagster/chaos-state.db"}
	cfg.Defaults()

	if cfg.StateDSN != "/var/dagster/chaos-state.db" {
		t.Errorf("Defaults() StateDSN = %q, want %q", cfg.StateDSN, "/var/dagster/chaos-state.db")
	}
}

func TestConfig_Defaults_PreservesExplicitFields(t *testing.T) {
	t.Parallel()

	// Defaults() must not panic and must not overwrite explicitly set fields.
	tests := []struct {
		name string
		cfg  dagster.Config
	}{
		{
			name: "zero value config",
			cfg:  dagster.Config{},
		},
		{
			name: "fully populated config",
			cfg: dagster.Config{
				URL: "http://localhost:3000/graphql",
				Headers: map[string]string{
					"Authorization": "Bearer token",
				},
				RepositoryLocationName: "loc",
				RepositoryName:         "repo",
				StateDSN:               "/tmp/state.db",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Capture values before Defaults().
			before := tt.cfg

			tt.cfg.Defaults()

			// Verify Defaults() did not overwrite any fields.
			if tt.cfg.URL != before.URL {
				t.Errorf("Defaults() changed URL: got %q, want %q", tt.cfg.URL, before.URL)
			}
			if tt.cfg.RepositoryLocationName != before.RepositoryLocationName {
				t.Errorf("Defaults() changed RepositoryLocationName: got %q, want %q",
					tt.cfg.RepositoryLocationName, before.RepositoryLocationName)
			}
			if tt.cfg.RepositoryName != before.RepositoryName {
				t.Errorf("Defaults() changed RepositoryName: got %q, want %q",
					tt.cfg.RepositoryName, before.RepositoryName)
			}
			if tt.cfg.StateDSN != before.StateDSN && before.StateDSN != "" {
				t.Errorf("Defaults() changed StateDSN: got %q, want %q",
					tt.cfg.StateDSN, before.StateDSN)
			}
		})
	}
}

func TestConfig_Validate_HTTPWarning(t *testing.T) {
	var buf bytes.Buffer
	restore := dagster.SetWarnWriter(&buf)
	defer restore()

	cfg := dagster.Config{URL: "http://localhost:3000/graphql"}
	cfg.Defaults()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() unexpected error: %v", err)
	}

	output := buf.String()
	if output == "" {
		t.Error("expected HTTP warning, got nothing")
	}
	if !strings.Contains(output, "http://") {
		t.Errorf("expected warning to mention http://, got: %q", output)
	}
}

func TestConfig_Validate_HTTPSNoWarning(t *testing.T) {
	var buf bytes.Buffer
	restore := dagster.SetWarnWriter(&buf)
	defer restore()

	cfg := dagster.Config{URL: "https://dagster.example.com/graphql"}
	cfg.Defaults()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() unexpected error: %v", err)
	}

	output := buf.String()
	if output != "" {
		t.Errorf("expected no warning for HTTPS URL, got: %q", output)
	}
}

func TestConfig_Redacted_MasksToken(t *testing.T) {
	t.Parallel()

	cfg := dagster.Config{
		URL: "https://dagster.example.com/graphql",
		Headers: map[string]string{
			"Dagster-Cloud-Api-Token": "secret-token-123",
			"X-Custom":               "keep-me",
		},
		RepositoryLocationName: "loc",
		RepositoryName:         "repo",
	}
	redacted := cfg.Redacted()

	if redacted.Headers["Dagster-Cloud-Api-Token"] != "[REDACTED]" {
		t.Errorf("Redacted().Headers[Dagster-Cloud-Api-Token] = %q, want %q",
			redacted.Headers["Dagster-Cloud-Api-Token"], "[REDACTED]")
	}
	if redacted.Headers["X-Custom"] != "keep-me" {
		t.Errorf("Redacted().Headers[X-Custom] = %q, want %q",
			redacted.Headers["X-Custom"], "keep-me")
	}
	// Original must not be modified.
	if cfg.Headers["Dagster-Cloud-Api-Token"] != "secret-token-123" {
		t.Errorf("original token header mutated to %q", cfg.Headers["Dagster-Cloud-Api-Token"])
	}
	// Other fields preserved.
	if redacted.URL != cfg.URL {
		t.Errorf("Redacted().URL = %q, want %q", redacted.URL, cfg.URL)
	}
	if redacted.RepositoryLocationName != cfg.RepositoryLocationName {
		t.Errorf("Redacted().RepositoryLocationName = %q, want %q",
			redacted.RepositoryLocationName, cfg.RepositoryLocationName)
	}
	if redacted.RepositoryName != cfg.RepositoryName {
		t.Errorf("Redacted().RepositoryName = %q, want %q",
			redacted.RepositoryName, cfg.RepositoryName)
	}
}

func TestConfig_Redacted_NilHeaders(t *testing.T) {
	t.Parallel()

	cfg := dagster.Config{
		URL: "https://dagster.example.com/graphql",
	}
	redacted := cfg.Redacted()

	if redacted.Headers != nil {
		t.Errorf("Redacted().Headers = %v, want nil when original is nil", redacted.Headers)
	}
}

func TestConfig_Redacted_IncludesStateDSN(t *testing.T) {
	t.Parallel()

	cfg := dagster.Config{
		URL:      "https://dagster.example.com/graphql",
		StateDSN: "/var/dagster/chaos-state.db",
	}
	redacted := cfg.Redacted()

	if redacted.StateDSN != cfg.StateDSN {
		t.Errorf("Redacted().StateDSN = %q, want %q", redacted.StateDSN, cfg.StateDSN)
	}
}

func TestConfig_Redacted_NoTokenHeader(t *testing.T) {
	t.Parallel()

	cfg := dagster.Config{
		URL: "https://dagster.example.com/graphql",
		Headers: map[string]string{
			"X-Custom": "value",
		},
	}
	redacted := cfg.Redacted()

	if redacted.Headers["X-Custom"] != "value" {
		t.Errorf("Redacted().Headers[X-Custom] = %q, want %q",
			redacted.Headers["X-Custom"], "value")
	}
	if _, ok := redacted.Headers["Dagster-Cloud-Api-Token"]; ok {
		t.Error("Redacted() should not add Dagster-Cloud-Api-Token if not present")
	}
}
