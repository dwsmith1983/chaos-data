package airflow_test

import (
	"bytes"
	"testing"

	"github.com/dwsmith1983/chaos-data/adapters/airflow"
)

func TestConfig_Validate_EmptyURL(t *testing.T) {
	t.Parallel()
	cfg := airflow.Config{}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for empty URL")
	}
}

func TestConfig_Validate_InvalidURL(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		url  string
	}{
		{"no scheme", "not-a-url"},
		{"ftp scheme", "ftp://host"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := airflow.Config{URL: tt.url}
			if err := cfg.Validate(); err == nil {
				t.Errorf("expected error for URL %q", tt.url)
			}
		})
	}
}

func TestConfig_Validate_ValidURL(t *testing.T) {
	t.Parallel()
	cfg := airflow.Config{URL: "http://localhost:8080/api/v1"}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestConfig_Validate_ValidURLWithHeaders(t *testing.T) {
	t.Parallel()
	cfg := airflow.Config{
		URL:     "https://airflow.example.com/api/v1",
		Headers: map[string]string{"Authorization": "Basic dXNlcjpwYXNz"},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestConfig_Defaults_IsNoOp(t *testing.T) {
	t.Parallel()
	cfg := airflow.Config{URL: "http://localhost:8080/api/v1"}
	cfg.Defaults()
	if cfg.URL != "http://localhost:8080/api/v1" {
		t.Error("Defaults() mutated URL")
	}
}

func TestConfig_Validate_V2_WithCredentials(t *testing.T) {
	t.Parallel()
	cfg := airflow.Config{
		URL:      "http://localhost:8080/api/v2",
		Version:  "v2",
		Username: "admin",
		Password: "secret",
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestConfig_Validate_V2_WithAuthHeader(t *testing.T) {
	t.Parallel()
	cfg := airflow.Config{
		URL:     "http://localhost:8080/api/v2",
		Version: "v2",
		Headers: map[string]string{"Authorization": "Bearer token123"},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestConfig_Validate_V2_NoCredentials_Error(t *testing.T) {
	t.Parallel()
	cfg := airflow.Config{
		URL:     "http://localhost:8080/api/v2",
		Version: "v2",
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for v2 without credentials or Authorization header")
	}
}

func TestConfig_Validate_UnknownVersion_Error(t *testing.T) {
	t.Parallel()
	cfg := airflow.Config{
		URL:     "http://localhost:8080/api/v3",
		Version: "v3",
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for unknown version v3")
	}
}

func TestConfig_Defaults_V1(t *testing.T) {
	t.Parallel()
	cfg := airflow.Config{URL: "http://localhost:8080/api/v1"}
	cfg.Defaults()
	if cfg.Version != "v1" {
		t.Errorf("expected Version %q after Defaults(), got %q", "v1", cfg.Version)
	}
}

func TestConfig_Validate_HTTPWarning(t *testing.T) {
	var buf bytes.Buffer
	restore := airflow.SetWarnWriter(&buf)
	defer restore()

	cfg := airflow.Config{URL: "http://localhost:8080/api/v1"}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() unexpected error: %v", err)
	}

	output := buf.String()
	if output == "" {
		t.Error("expected HTTP warning, got nothing")
	}
	if !bytes.Contains([]byte(output), []byte("http://")) {
		t.Errorf("expected warning to mention http://, got: %q", output)
	}
}

func TestConfig_Validate_HTTPSNoWarning(t *testing.T) {
	var buf bytes.Buffer
	restore := airflow.SetWarnWriter(&buf)
	defer restore()

	cfg := airflow.Config{URL: "https://airflow.example.com/api/v1"}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() unexpected error: %v", err)
	}

	output := buf.String()
	if output != "" {
		t.Errorf("expected no warning for HTTPS URL, got: %q", output)
	}
}

func TestConfig_Redacted_MasksPassword(t *testing.T) {
	t.Parallel()

	cfg := airflow.Config{
		URL:      "https://airflow.example.com/api/v2",
		Version:  "v2",
		Username: "admin",
		Password: "super-secret",
	}
	redacted := cfg.Redacted()

	if redacted.Password != "[REDACTED]" {
		t.Errorf("Redacted().Password = %q, want %q", redacted.Password, "[REDACTED]")
	}
	// Original must not be modified.
	if cfg.Password != "super-secret" {
		t.Errorf("original Password mutated to %q", cfg.Password)
	}
	// Other fields preserved.
	if redacted.URL != cfg.URL {
		t.Errorf("Redacted().URL = %q, want %q", redacted.URL, cfg.URL)
	}
	if redacted.Username != cfg.Username {
		t.Errorf("Redacted().Username = %q, want %q", redacted.Username, cfg.Username)
	}
	if redacted.Version != cfg.Version {
		t.Errorf("Redacted().Version = %q, want %q", redacted.Version, cfg.Version)
	}
}

func TestConfig_Redacted_MasksAuthorizationHeader(t *testing.T) {
	t.Parallel()

	cfg := airflow.Config{
		URL: "https://airflow.example.com/api/v1",
		Headers: map[string]string{
			"Authorization": "Bearer token123",
			"X-Custom":      "keep-me",
		},
	}
	redacted := cfg.Redacted()

	if redacted.Headers["Authorization"] != "[REDACTED]" {
		t.Errorf("Redacted().Headers[Authorization] = %q, want %q",
			redacted.Headers["Authorization"], "[REDACTED]")
	}
	if redacted.Headers["X-Custom"] != "keep-me" {
		t.Errorf("Redacted().Headers[X-Custom] = %q, want %q",
			redacted.Headers["X-Custom"], "keep-me")
	}
	// Original must not be modified.
	if cfg.Headers["Authorization"] != "Bearer token123" {
		t.Errorf("original Authorization header mutated to %q", cfg.Headers["Authorization"])
	}
}

func TestConfig_Redacted_EmptyPassword_StaysEmpty(t *testing.T) {
	t.Parallel()

	cfg := airflow.Config{
		URL: "https://airflow.example.com/api/v1",
	}
	redacted := cfg.Redacted()

	if redacted.Password != "" {
		t.Errorf("Redacted().Password = %q, want empty for no password", redacted.Password)
	}
}

func TestConfig_Redacted_NilHeaders(t *testing.T) {
	t.Parallel()

	cfg := airflow.Config{
		URL:      "https://airflow.example.com/api/v1",
		Password: "secret",
	}
	redacted := cfg.Redacted()

	if redacted.Headers != nil {
		t.Errorf("Redacted().Headers = %v, want nil when original is nil", redacted.Headers)
	}
	if redacted.Password != "[REDACTED]" {
		t.Errorf("Redacted().Password = %q, want %q", redacted.Password, "[REDACTED]")
	}
}
