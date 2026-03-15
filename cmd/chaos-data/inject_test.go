package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestInjectCmd_RequiresScenarioFlag(t *testing.T) {
	t.Parallel()

	cmd := rootCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{
		"inject",
		"--input", "/tmp/in",
		"--output", "/tmp/out",
	})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error when --scenario is missing")
	}
}

func TestInjectCmd_RequiresInputFlag(t *testing.T) {
	t.Parallel()

	cmd := rootCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{
		"inject",
		"--scenario", "phantom-sensor",
		"--output", "/tmp/out",
	})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error when --input is missing")
	}
}

func TestInjectCmd_RequiresOutputFlag(t *testing.T) {
	t.Parallel()

	cmd := rootCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{
		"inject",
		"--scenario", "phantom-sensor",
		"--input", "/tmp/in",
	})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error when --output is missing")
	}
}

func TestInjectCmd_EndToEnd(t *testing.T) {
	t.Parallel()

	inputDir := t.TempDir()
	outputDir := t.TempDir()
	scenarioDir := t.TempDir()

	// Write a phantom-sensor scenario YAML targeting the state layer so it
	// doesn't require real data files.
	scenarioYAML := `name: inject-test-phantom
description: Inject a phantom sensor for CLI testing
category: state-consistency
severity: low
version: 1
target:
  layer: state
mutation:
  type: phantom-sensor
  params:
    pipeline: cli-test-pipe
    sensor_key: health
probability: 1.0
safety:
  max_affected_pct: 100
  cooldown: 1m
  sla_aware: false
`
	scenarioFile := filepath.Join(scenarioDir, "inject-test.yaml")
	if err := os.WriteFile(scenarioFile, []byte(scenarioYAML), 0o644); err != nil {
		t.Fatalf("write scenario file: %v", err)
	}

	cmd := rootCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{
		"inject",
		"--scenario", scenarioFile,
		"--input", inputDir,
		"--output", outputDir,
		"--state-db", ":memory:",
	})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("inject command failed: %v\noutput: %s", err, buf.String())
	}

	output := buf.String()
	// The output should mention either the mutation type or that it was applied.
	if !bytes.Contains([]byte(output), []byte("phantom-sensor")) {
		t.Errorf("expected output to mention 'phantom-sensor', got:\n%s", output)
	}
}

func TestInjectCmd_DryRun(t *testing.T) {
	t.Parallel()

	inputDir := t.TempDir()
	outputDir := t.TempDir()
	scenarioDir := t.TempDir()

	scenarioYAML := `name: inject-dryrun-test
description: Dry-run inject test
category: state-consistency
severity: low
version: 1
target:
  layer: state
mutation:
  type: phantom-sensor
  params:
    pipeline: dryrun-pipe
    sensor_key: status
probability: 1.0
safety:
  max_affected_pct: 100
  cooldown: 1m
  sla_aware: false
`
	scenarioFile := filepath.Join(scenarioDir, "dryrun.yaml")
	if err := os.WriteFile(scenarioFile, []byte(scenarioYAML), 0o644); err != nil {
		t.Fatalf("write scenario file: %v", err)
	}

	cmd := rootCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{
		"inject",
		"--scenario", scenarioFile,
		"--input", inputDir,
		"--output", outputDir,
		"--state-db", ":memory:",
		"--dry-run",
	})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("inject --dry-run command failed: %v\noutput: %s", err, buf.String())
	}

	output := buf.String()
	// In dry-run mode the record error should be "dry-run".
	if !bytes.Contains([]byte(output), []byte("dry-run")) {
		t.Errorf("expected output to contain 'dry-run', got:\n%s", output)
	}
}

func TestInjectCmd_InvalidScenario(t *testing.T) {
	t.Parallel()

	cmd := rootCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{
		"inject",
		"--scenario", "/nonexistent/path/scenario.yaml",
		"--input", "/tmp/in",
		"--output", "/tmp/out",
	})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error for nonexistent scenario file")
	}
}
