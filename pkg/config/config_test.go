package config_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/dwsmith1983/chaos-data/pkg/config"
)

func TestLoad_DagsterAdapter(t *testing.T) {
	t.Parallel()
	yaml := `
adapters:
  dagster:
    url: "http://localhost:3000/graphql"
    headers:
      Dagster-Cloud-Api-Token: "my-token"
    repository_location_name: "my_loc"
    repository_name: "my_repo"
`
	path := writeTempFile(t, yaml)
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Adapters.Dagster.URL != "http://localhost:3000/graphql" {
		t.Errorf("Dagster.URL = %q", cfg.Adapters.Dagster.URL)
	}
	if cfg.Adapters.Dagster.Headers["Dagster-Cloud-Api-Token"] != "my-token" {
		t.Error("Dagster headers not parsed")
	}
	if cfg.Adapters.Dagster.RepositoryLocationName != "my_loc" {
		t.Errorf("RepositoryLocationName = %q", cfg.Adapters.Dagster.RepositoryLocationName)
	}
	if cfg.Adapters.Dagster.RepositoryName != "my_repo" {
		t.Errorf("RepositoryName = %q", cfg.Adapters.Dagster.RepositoryName)
	}
}

func TestLoad_AirflowAdapter(t *testing.T) {
	t.Parallel()
	yaml := `
adapters:
  airflow:
    url: "http://localhost:8080/api/v1"
    headers:
      Authorization: "Basic dXNlcjpwYXNz"
`
	path := writeTempFile(t, yaml)
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Adapters.Airflow.URL != "http://localhost:8080/api/v1" {
		t.Errorf("Airflow.URL = %q", cfg.Adapters.Airflow.URL)
	}
	if cfg.Adapters.Airflow.Headers["Authorization"] != "Basic dXNlcjpwYXNz" {
		t.Error("Airflow headers not parsed")
	}
}

func TestValidate_BothAdaptersConfigured_Error(t *testing.T) {
	t.Parallel()
	yaml := `
adapters:
  dagster:
    url: "http://dagster:3000/graphql"
  airflow:
    url: "http://airflow:8080/api/v1"
`
	path := writeTempFile(t, yaml)
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error when both adapters have URLs")
	}
}

func TestValidate_NeitherAdapter_Passes(t *testing.T) {
	t.Parallel()
	yaml := "adapters: {}\n"
	path := writeTempFile(t, yaml)
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestValidate_EmptyFile_Passes(t *testing.T) {
	t.Parallel()
	path := writeTempFile(t, "")
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestLoad_NonexistentFile_Error(t *testing.T) {
	t.Parallel()
	_, err := config.Load("/nonexistent/chaos.yaml")
	if err == nil {
		t.Fatal("expected error for nonexistent file")
	}
}

func TestLoad_InvalidYAML_Error(t *testing.T) {
	t.Parallel()
	path := writeTempFile(t, "{{invalid yaml")
	_, err := config.Load(path)
	if err == nil {
		t.Fatal("expected error for invalid YAML")
	}
}

func TestBuildAsserter_DagsterURL_ReturnsAsserter(t *testing.T) {
	t.Parallel()
	yaml := `
adapters:
  dagster:
    url: "http://localhost:3000/graphql"
`
	path := writeTempFile(t, yaml)
	cfg, _ := config.Load(path)
	asserter, err := cfg.BuildAsserter()
	if err != nil {
		t.Fatalf("BuildAsserter() error = %v", err)
	}
	if asserter == nil {
		t.Fatal("expected non-nil asserter")
	}
}

func TestBuildAsserter_AirflowURL_ReturnsAsserter(t *testing.T) {
	t.Parallel()
	yaml := `
adapters:
  airflow:
    url: "http://localhost:8080/api/v1"
`
	path := writeTempFile(t, yaml)
	cfg, _ := config.Load(path)
	asserter, err := cfg.BuildAsserter()
	if err != nil {
		t.Fatalf("BuildAsserter() error = %v", err)
	}
	if asserter == nil {
		t.Fatal("expected non-nil asserter")
	}
}

func TestBuildAsserter_NeitherURL_ReturnsNil(t *testing.T) {
	t.Parallel()
	yaml := "adapters: {}\n"
	path := writeTempFile(t, yaml)
	cfg, _ := config.Load(path)
	asserter, err := cfg.BuildAsserter()
	if err != nil {
		t.Fatalf("BuildAsserter() error = %v", err)
	}
	if asserter != nil {
		t.Fatal("expected nil asserter when no adapter configured")
	}
}

func TestBuildAsserter_InvalidDagsterURL_Error(t *testing.T) {
	t.Parallel()
	yaml := `
adapters:
  dagster:
    url: "ftp://invalid"
`
	path := writeTempFile(t, yaml)
	cfg, _ := config.Load(path)
	_, err := cfg.BuildAsserter()
	if err == nil {
		t.Fatal("expected error for invalid dagster URL")
	}
}

func TestBuildAsserter_InvalidAirflowURL_Error(t *testing.T) {
	t.Parallel()
	yaml := `
adapters:
  airflow:
    url: "ftp://invalid"
`
	path := writeTempFile(t, yaml)
	cfg, _ := config.Load(path)
	_, err := cfg.BuildAsserter()
	if err == nil {
		t.Fatal("expected error for invalid airflow URL")
	}
}

func TestLoadFromBytes_ValidYAML(t *testing.T) {
	t.Parallel()
	data := []byte(`
adapters:
  airflow:
    url: "http://localhost:8080/api/v1"
`)
	cfg, err := config.LoadFromBytes(data)
	if err != nil {
		t.Fatalf("LoadFromBytes() error = %v", err)
	}
	if cfg.Adapters.Airflow.URL != "http://localhost:8080/api/v1" {
		t.Errorf("Airflow.URL = %q", cfg.Adapters.Airflow.URL)
	}
}

func TestLoadFromBytes_InvalidYAML(t *testing.T) {
	t.Parallel()
	_, err := config.LoadFromBytes([]byte("{{invalid"))
	if err == nil {
		t.Fatal("expected error for invalid YAML")
	}
}

func TestLoadFromBytes_EmptyBytes(t *testing.T) {
	t.Parallel()
	cfg, err := config.LoadFromBytes([]byte(""))
	if err != nil {
		t.Fatalf("LoadFromBytes() error = %v", err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("empty bytes should produce valid config: %v", err)
	}
}

func TestBuildAsserter_AirflowV2_ReturnsAsserter(t *testing.T) {
	t.Parallel()
	yaml := `
adapters:
  airflow:
    url: "http://localhost:8080"
    version: "v2"
    username: "admin"
    password: "admin"
`
	path := writeTempFile(t, yaml)
	cfg, _ := config.Load(path)
	asserter, err := cfg.BuildAsserter()
	if err != nil {
		t.Fatalf("BuildAsserter() error = %v", err)
	}
	if asserter == nil {
		t.Fatal("expected non-nil asserter for v2")
	}
}

func TestBuildAsserter_AirflowDefaultVersion_UsesV1(t *testing.T) {
	t.Parallel()
	yaml := `
adapters:
  airflow:
    url: "http://localhost:8080/api/v1"
`
	path := writeTempFile(t, yaml)
	cfg, _ := config.Load(path)
	asserter, err := cfg.BuildAsserter()
	if err != nil {
		t.Fatalf("BuildAsserter() error = %v", err)
	}
	if asserter == nil {
		t.Fatal("expected non-nil asserter")
	}
}

func TestBuildAsserter_AirflowV2_NoCredentials_Error(t *testing.T) {
	t.Parallel()
	yaml := `
adapters:
  airflow:
    url: "http://localhost:8080"
    version: "v2"
`
	path := writeTempFile(t, yaml)
	cfg, _ := config.Load(path)
	_, err := cfg.BuildAsserter()
	if err == nil {
		t.Fatal("expected error for v2 without credentials")
	}
}

func writeTempFile(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "chaos.yaml")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}
