package dagster_test

import (
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

func TestConfig_Defaults_IsNoOp(t *testing.T) {
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
		})
	}
}
