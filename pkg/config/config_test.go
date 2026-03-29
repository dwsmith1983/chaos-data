package config_test

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
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

func TestBuildTransport_S3(t *testing.T) {
	t.Parallel()
	yaml := `
transport:
  type: "s3"
  staging_bucket: "my-staging"
  pipeline_bucket: "my-pipeline"
`
	path := writeTempFile(t, yaml)
	cfg, _ := config.Load(path)
	transport, err := cfg.BuildTransport(context.Background())
	if err != nil {
		t.Fatalf("BuildTransport() error = %v", err)
	}
	if transport == nil {
		t.Fatal("expected non-nil transport for type=s3")
	}
}

func TestBuildTransport_S3_MissingStagingBucket(t *testing.T) {
	t.Parallel()
	yaml := `
transport:
  type: "s3"
  pipeline_bucket: "my-pipeline"
`
	path := writeTempFile(t, yaml)
	cfg, _ := config.Load(path)
	_, err := cfg.BuildTransport(context.Background())
	if err == nil {
		t.Fatal("expected error for missing staging_bucket")
	}
}

func TestBuildTransport_FS(t *testing.T) {
	t.Parallel()
	yaml := `
transport:
  type: "fs"
  input_dir: "/tmp/in"
  output_dir: "/tmp/out"
`
	path := writeTempFile(t, yaml)
	cfg, _ := config.Load(path)
	transport, err := cfg.BuildTransport(context.Background())
	if err != nil {
		t.Fatalf("BuildTransport() error = %v", err)
	}
	if transport == nil {
		t.Fatal("expected non-nil transport for type=fs")
	}
}

func TestBuildTransport_Empty(t *testing.T) {
	t.Parallel()
	yaml := "adapters: {}\n"
	path := writeTempFile(t, yaml)
	cfg, _ := config.Load(path)
	transport, err := cfg.BuildTransport(context.Background())
	if err != nil {
		t.Fatalf("BuildTransport() error = %v", err)
	}
	if transport != nil {
		t.Fatal("expected nil transport when not configured")
	}
}

func TestBuildTransport_UnknownType(t *testing.T) {
	t.Parallel()
	yaml := `
transport:
  type: "gcs"
`
	path := writeTempFile(t, yaml)
	cfg, _ := config.Load(path)
	_, err := cfg.BuildTransport(context.Background())
	if err == nil {
		t.Fatal("expected error for unknown transport type")
	}
}

func TestBuildSafety_DynamoDB(t *testing.T) {
	t.Parallel()
	yaml := `
safety:
  type: "dynamodb"
  table_name: "chaos-data"
`
	path := writeTempFile(t, yaml)
	cfg, _ := config.Load(path)
	safety, err := cfg.BuildSafety(context.Background())
	if err != nil {
		t.Fatalf("BuildSafety() error = %v", err)
	}
	if safety == nil {
		t.Fatal("expected non-nil safety for type=dynamodb")
	}
}

func TestBuildSafety_DynamoDB_MissingTable(t *testing.T) {
	t.Parallel()
	yaml := `
safety:
  type: "dynamodb"
`
	path := writeTempFile(t, yaml)
	cfg, _ := config.Load(path)
	_, err := cfg.BuildSafety(context.Background())
	if err == nil {
		t.Fatal("expected error for missing table_name")
	}
}

func TestBuildSafety_Config(t *testing.T) {
	t.Parallel()
	yaml := `
safety:
  type: "config"
`
	path := writeTempFile(t, yaml)
	cfg, _ := config.Load(path)
	safety, err := cfg.BuildSafety(context.Background())
	if err != nil {
		t.Fatalf("BuildSafety() error = %v", err)
	}
	if safety == nil {
		t.Fatal("expected non-nil safety for type=config")
	}
}

func TestBuildSafety_Empty_DefaultsToConfig(t *testing.T) {
	t.Parallel()
	yaml := "adapters: {}\n"
	path := writeTempFile(t, yaml)
	cfg, _ := config.Load(path)
	safety, err := cfg.BuildSafety(context.Background())
	if err != nil {
		t.Fatalf("BuildSafety() error = %v", err)
	}
	if safety == nil {
		t.Fatal("expected non-nil safety (defaults to config)")
	}
}

func TestBuildSafety_None(t *testing.T) {
	t.Parallel()
	yaml := `
safety:
  type: "none"
`
	path := writeTempFile(t, yaml)
	cfg, _ := config.Load(path)
	safety, err := cfg.BuildSafety(context.Background())
	if err != nil {
		t.Fatalf("BuildSafety() error = %v", err)
	}
	if safety != nil {
		t.Fatal("expected nil safety for type=none")
	}
}

func TestBuildEmitter_EventBridge(t *testing.T) {
	t.Parallel()
	yaml := `
emitter:
  type: "eventbridge"
`
	path := writeTempFile(t, yaml)
	cfg, _ := config.Load(path)
	emitter, err := cfg.BuildEmitter(context.Background(), io.Discard)
	if err != nil {
		t.Fatalf("BuildEmitter() error = %v", err)
	}
	if emitter == nil {
		t.Fatal("expected non-nil emitter for type=eventbridge")
	}
}

func TestBuildEmitter_Stdout(t *testing.T) {
	t.Parallel()
	yaml := `
emitter:
  type: "stdout"
`
	path := writeTempFile(t, yaml)
	cfg, _ := config.Load(path)
	emitter, err := cfg.BuildEmitter(context.Background(), io.Discard)
	if err != nil {
		t.Fatalf("BuildEmitter() error = %v", err)
	}
	if emitter == nil {
		t.Fatal("expected non-nil emitter for type=stdout")
	}
}

func TestBuildEmitter_Empty_DefaultsToStdout(t *testing.T) {
	t.Parallel()
	yaml := "adapters: {}\n"
	path := writeTempFile(t, yaml)
	cfg, _ := config.Load(path)
	emitter, err := cfg.BuildEmitter(context.Background(), io.Discard)
	if err != nil {
		t.Fatalf("BuildEmitter() error = %v", err)
	}
	if emitter == nil {
		t.Fatal("expected non-nil emitter (defaults to stdout)")
	}
}

func TestBuildEmitter_None(t *testing.T) {
	t.Parallel()
	yaml := `
emitter:
  type: "none"
`
	path := writeTempFile(t, yaml)
	cfg, _ := config.Load(path)
	emitter, err := cfg.BuildEmitter(context.Background(), io.Discard)
	if err != nil {
		t.Fatalf("BuildEmitter() error = %v", err)
	}
	if emitter != nil {
		t.Fatal("expected nil emitter for type=none")
	}
}

func TestLoad_OversizedFile_Error(t *testing.T) {
	t.Parallel()

	// Create a file just over 1MB.
	dir := t.TempDir()
	path := filepath.Join(dir, "huge.yaml")
	data := make([]byte, 1<<20+1)
	for i := range data {
		data[i] = 'a'
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}

	_, err := config.Load(path)
	if err == nil {
		t.Fatal("expected error for oversized config file")
	}
	if !strings.Contains(err.Error(), "exceeds maximum") {
		t.Errorf("error = %q, want message containing %q", err.Error(), "exceeds maximum")
	}
}

func TestLoadFromBytes_OversizedData_Error(t *testing.T) {
	t.Parallel()

	data := make([]byte, 1<<20+1)
	for i := range data {
		data[i] = 'a'
	}

	_, err := config.LoadFromBytes(data)
	if err == nil {
		t.Fatal("expected error for oversized data")
	}
	if !strings.Contains(err.Error(), "exceeds maximum") {
		t.Errorf("error = %q, want message containing %q", err.Error(), "exceeds maximum")
	}
}

func TestLoad_ExactlyMaxSize_OK(t *testing.T) {
	t.Parallel()

	// Exactly 1MB should be accepted (it's valid YAML content that won't parse,
	// but the size check should pass).
	dir := t.TempDir()
	path := filepath.Join(dir, "max.yaml")
	// Use valid empty YAML padded with comments to reach exactly 1MB.
	header := []byte("# padding\n")
	data := make([]byte, 1<<20)
	copy(data, header)
	for i := len(header); i < len(data); i++ {
		data[i] = '#'
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}

	// Should not fail on size check (may fail on parse, but that's fine).
	_, err := config.Load(path)
	if err != nil && strings.Contains(err.Error(), "exceeds maximum") {
		t.Fatalf("file at exactly 1MB should not be rejected for size: %v", err)
	}
}
