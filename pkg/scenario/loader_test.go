package scenario_test

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/dwsmith1983/chaos-data/pkg/scenario"
)

// validYAML returns a minimal valid scenario YAML string.
func validYAML(name string) string {
	return `name: ` + name + `
description: Test scenario
category: data-arrival
severity: moderate
version: 1
target:
  layer: data
  transport: s3
  filter:
    prefix: "ingest/"
    match: "*.parquet"
mutation:
  type: delay
  params:
    duration: "30m"
probability: 0.5
safety:
  max_affected_pct: 25
  cooldown: 5m
  sla_aware: true
`
}

func TestLoadFile(t *testing.T) {
	tests := []struct {
		name      string
		setup     func(t *testing.T) string // returns file path
		wantName  string
		wantErr   bool
		errTarget error
	}{
		{
			name: "valid YAML file",
			setup: func(t *testing.T) string {
				t.Helper()
				dir := t.TempDir()
				path := filepath.Join(dir, "scenario.yaml")
				if err := os.WriteFile(path, []byte(validYAML("test-scenario")), 0o644); err != nil {
					t.Fatal(err)
				}
				return path
			},
			wantName: "test-scenario",
			wantErr:  false,
		},
		{
			name: "missing file returns error",
			setup: func(t *testing.T) string {
				t.Helper()
				return filepath.Join(t.TempDir(), "nonexistent.yaml")
			},
			wantErr: true,
		},
		{
			name: "invalid YAML returns error",
			setup: func(t *testing.T) string {
				t.Helper()
				dir := t.TempDir()
				path := filepath.Join(dir, "bad.yaml")
				content := []byte("{{not: valid: yaml: [}")
				if err := os.WriteFile(path, content, 0o644); err != nil {
					t.Fatal(err)
				}
				return path
			},
			wantErr: true,
		},
		{
			name: "YAML failing validation returns error",
			setup: func(t *testing.T) string {
				t.Helper()
				dir := t.TempDir()
				path := filepath.Join(dir, "invalid.yaml")
				// Missing name field triggers validation failure
				content := []byte(`category: data-arrival
severity: moderate
version: 1
mutation:
  type: delay
probability: 0.5
`)
				if err := os.WriteFile(path, content, 0o644); err != nil {
					t.Fatal(err)
				}
				return path
			},
			wantErr:   true,
			errTarget: scenario.ErrInvalidScenario,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := tt.setup(t)
			s, err := scenario.LoadFile(path)

			if tt.wantErr {
				if err == nil {
					t.Fatal("LoadFile() expected error, got nil")
				}
				if tt.errTarget != nil && !errors.Is(err, tt.errTarget) {
					t.Errorf("LoadFile() error = %v, want wrapping %v", err, tt.errTarget)
				}
				return
			}

			if err != nil {
				t.Fatalf("LoadFile() unexpected error: %v", err)
			}
			if s.Name != tt.wantName {
				t.Errorf("LoadFile() Name = %q, want %q", s.Name, tt.wantName)
			}
		})
	}
}

func TestLoadDir(t *testing.T) {
	tests := []struct {
		name       string
		setup      func(t *testing.T) string // returns directory path
		wantCount  int
		wantNames  []string
		wantErr    bool
	}{
		{
			name: "directory with multiple valid YAML files",
			setup: func(t *testing.T) string {
				t.Helper()
				dir := t.TempDir()
				if err := os.WriteFile(filepath.Join(dir, "a.yaml"), []byte(validYAML("scenario-a")), 0o644); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(dir, "b.yml"), []byte(validYAML("scenario-b")), 0o644); err != nil {
					t.Fatal(err)
				}
				return dir
			},
			wantCount: 2,
			wantNames: []string{"scenario-a", "scenario-b"},
			wantErr:   false,
		},
		{
			name: "empty directory returns empty slice",
			setup: func(t *testing.T) string {
				t.Helper()
				return t.TempDir()
			},
			wantCount: 0,
			wantErr:   false,
		},
		{
			name: "missing directory returns error",
			setup: func(t *testing.T) string {
				t.Helper()
				return filepath.Join(t.TempDir(), "nonexistent")
			},
			wantErr: true,
		},
		{
			name: "non-YAML files are ignored",
			setup: func(t *testing.T) string {
				t.Helper()
				dir := t.TempDir()
				if err := os.WriteFile(filepath.Join(dir, "valid.yaml"), []byte(validYAML("only-yaml")), 0o644); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(dir, "readme.txt"), []byte("not a scenario"), 0o644); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(dir, "data.json"), []byte(`{"key":"value"}`), 0o644); err != nil {
					t.Fatal(err)
				}
				return dir
			},
			wantCount: 1,
			wantNames: []string{"only-yaml"},
			wantErr:   false,
		},
		{
			name: "invalid YAML file in directory returns error",
			setup: func(t *testing.T) string {
				t.Helper()
				dir := t.TempDir()
				if err := os.WriteFile(filepath.Join(dir, "good.yaml"), []byte(validYAML("good-one")), 0o644); err != nil {
					t.Fatal(err)
				}
				// This file has valid YAML but fails Validate (missing name)
				badContent := []byte(`category: data-arrival
severity: moderate
version: 1
mutation:
  type: delay
`)
				if err := os.WriteFile(filepath.Join(dir, "bad.yaml"), badContent, 0o644); err != nil {
					t.Fatal(err)
				}
				return dir
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := tt.setup(t)
			scenarios, err := scenario.LoadDir(dir)

			if tt.wantErr {
				if err == nil {
					t.Fatal("LoadDir() expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Fatalf("LoadDir() unexpected error: %v", err)
			}
			if len(scenarios) != tt.wantCount {
				t.Fatalf("LoadDir() returned %d scenarios, want %d", len(scenarios), tt.wantCount)
			}

			if tt.wantNames != nil {
				gotNames := make(map[string]bool)
				for _, s := range scenarios {
					gotNames[s.Name] = true
				}
				for _, want := range tt.wantNames {
					if !gotNames[want] {
						t.Errorf("LoadDir() missing scenario with name %q", want)
					}
				}
			}
		})
	}
}
