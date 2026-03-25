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
