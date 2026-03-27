package airflow_test

import (
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
