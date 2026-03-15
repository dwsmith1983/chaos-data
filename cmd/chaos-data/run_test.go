package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestRunCmd_EndToEnd(t *testing.T) {
	t.Parallel()

	// Create temp input and output directories.
	inputDir := t.TempDir()
	outputDir := t.TempDir()

	// Write a test JSONL file to input.
	testData := `{"id":1,"name":"alice","score":100}
{"id":2,"name":"bob","score":200}
{"id":3,"name":"charlie","score":300}
`
	testFile := filepath.Join(inputDir, "test-data.jsonl")
	if err := os.WriteFile(testFile, []byte(testData), 0o644); err != nil {
		t.Fatalf("write test file: %v", err)
	}

	cmd := rootCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{
		"run",
		"--scenario", "late-data",
		"--input", inputDir,
		"--output", outputDir,
	})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("run command failed: %v\noutput: %s", err, buf.String())
	}

	output := buf.String()

	// Verify the output mentions applied mutation(s).
	if !bytes.Contains([]byte(output), []byte("delay")) {
		t.Errorf("expected output to mention mutation type %q, got:\n%s", "delay", output)
	}

	// Verify it mentions the object key.
	if !bytes.Contains([]byte(output), []byte("test-data.jsonl")) {
		t.Errorf("expected output to mention object key %q, got:\n%s", "test-data.jsonl", output)
	}
}

func TestRunCmd_RequiresScenarioFlag(t *testing.T) {
	t.Parallel()

	cmd := rootCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{
		"run",
		"--input", "/tmp/in",
		"--output", "/tmp/out",
	})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error when --scenario is missing")
	}
}

func TestRunCmd_RequiresInputFlag(t *testing.T) {
	t.Parallel()

	cmd := rootCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{
		"run",
		"--scenario", "late-data",
		"--output", "/tmp/out",
	})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error when --input is missing")
	}
}

func TestRunCmd_RequiresOutputFlag(t *testing.T) {
	t.Parallel()

	cmd := rootCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{
		"run",
		"--scenario", "late-data",
		"--input", "/tmp/in",
	})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error when --output is missing")
	}
}

func TestRunCmd_ScenarioFromFile(t *testing.T) {
	t.Parallel()

	// Create temp directories.
	inputDir := t.TempDir()
	outputDir := t.TempDir()
	scenarioDir := t.TempDir()

	// Write a test JSONL file to input.
	testData := `{"id":1,"value":"hello"}
`
	testFile := filepath.Join(inputDir, "data.jsonl")
	if err := os.WriteFile(testFile, []byte(testData), 0o644); err != nil {
		t.Fatalf("write test file: %v", err)
	}

	// Write a custom scenario YAML file.
	scenarioYAML := `name: custom-drop
description: Drop all files for testing
category: data-arrival
severity: low
version: 1
target:
  layer: data
  filter:
    prefix: ""
    match: ""
mutation:
  type: drop
  params:
    scope: object
probability: 1.0
safety:
  max_affected_pct: 100
  cooldown: 1m
  sla_aware: false
`
	scenarioFile := filepath.Join(scenarioDir, "custom-drop.yaml")
	if err := os.WriteFile(scenarioFile, []byte(scenarioYAML), 0o644); err != nil {
		t.Fatalf("write scenario file: %v", err)
	}

	cmd := rootCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{
		"run",
		"--scenario", scenarioFile,
		"--input", inputDir,
		"--output", outputDir,
	})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("run command failed: %v\noutput: %s", err, buf.String())
	}

	output := buf.String()

	// Verify the output mentions the drop mutation.
	if !bytes.Contains([]byte(output), []byte("drop")) {
		t.Errorf("expected output to mention mutation type %q, got:\n%s", "drop", output)
	}
}

func TestRunCmd_InvalidScenario(t *testing.T) {
	t.Parallel()

	cmd := rootCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{
		"run",
		"--scenario", "nonexistent-scenario",
		"--input", "/tmp/in",
		"--output", "/tmp/out",
	})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error for nonexistent scenario")
	}
}

func TestRunCmd_AssertWait(t *testing.T) {
	t.Parallel()

	inputDir := t.TempDir()
	outputDir := t.TempDir()

	testData := `{"id":1,"name":"alice"}
`
	testFile := filepath.Join(inputDir, "test-data.jsonl")
	if err := os.WriteFile(testFile, []byte(testData), 0o644); err != nil {
		t.Fatalf("write test file: %v", err)
	}

	cmd := rootCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{
		"run",
		"--scenario", "late-data",
		"--input", inputDir,
		"--output", outputDir,
		"--assert-wait",
	})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("run --assert-wait failed: %v\noutput: %s", err, buf.String())
	}
}

func TestDefaultRegistry_AllMutations(t *testing.T) {
	t.Parallel()

	reg := defaultRegistry()
	for _, name := range []string{"delay", "drop", "corrupt"} {
		if _, err := reg.Get(name); err != nil {
			t.Errorf("defaultRegistry missing mutation %q: %v", name, err)
		}
	}
}
