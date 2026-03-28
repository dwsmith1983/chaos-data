package interlocksuite

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadSuiteScenario(t *testing.T) {
	t.Parallel()
	ss, err := LoadSuiteScenario("testdata/test-scenario.yaml")
	if err != nil {
		t.Fatal(err)
	}

	if ss.Name != "test-validation-equals" {
		t.Errorf("name = %q, want %q", ss.Name, "test-validation-equals")
	}
	if ss.Capability != "validation/equals" {
		t.Errorf("capability = %q, want %q", ss.Capability, "validation/equals")
	}
	if ss.Setup == nil {
		t.Fatal("setup should not be nil")
	}
	if ss.Setup.Pipeline != "bronze-cdr" {
		t.Errorf("setup.pipeline = %q, want %q", ss.Setup.Pipeline, "bronze-cdr")
	}
	if ss.Setup.TriggerStatus != "COMPLETED" {
		t.Errorf("setup.trigger_status = %q, want %q", ss.Setup.TriggerStatus, "COMPLETED")
	}
	if ss.Setup.Sensors == nil {
		t.Fatal("setup.sensors should not be nil")
	}
	sensorData, ok := ss.Setup.Sensors["hourly-status"]
	if !ok {
		t.Fatal("setup.sensors missing hourly-status")
	}
	if sensorData["status"] != "COMPLETE" {
		t.Errorf("sensor status = %v, want %q", sensorData["status"], "COMPLETE")
	}

	// Also verify the embedded Scenario fields are parsed.
	if ss.Category != "state-consistency" {
		t.Errorf("category = %q, want %q", ss.Category, "state-consistency")
	}
	if ss.Mutation.Type != "interlock-phantom-sensor" {
		t.Errorf("mutation.type = %q, want %q", ss.Mutation.Type, "interlock-phantom-sensor")
	}
	if ss.Probability != 1.0 {
		t.Errorf("probability = %v, want 1.0", ss.Probability)
	}
}

func TestLoadSuiteScenario_NoSetup(t *testing.T) {
	t.Parallel()

	content := []byte(`name: test-no-setup
description: Scenario without setup block
category: data-arrival
severity: low
version: 1
target:
  layer: data
  filter: { prefix: "", match: "" }
mutation:
  type: drop-file
  params:
    bucket: test-bucket
probability: 0.5
safety:
  max_affected_pct: 50
  cooldown: 1m
  sla_aware: false
`)
	dir := t.TempDir()
	path := filepath.Join(dir, "no-setup.yaml")
	if err := os.WriteFile(path, content, 0o644); err != nil {
		t.Fatal(err)
	}

	ss, err := LoadSuiteScenario(path)
	if err != nil {
		t.Fatal(err)
	}

	if ss.Setup != nil {
		t.Errorf("setup should be nil, got %+v", ss.Setup)
	}
	if ss.Capability != "" {
		t.Errorf("capability should be empty, got %q", ss.Capability)
	}
	if ss.Name != "test-no-setup" {
		t.Errorf("name = %q, want %q", ss.Name, "test-no-setup")
	}
}

func TestLoadSuiteScenario_FileNotFound(t *testing.T) {
	t.Parallel()
	_, err := LoadSuiteScenario("testdata/nonexistent.yaml")
	if err == nil {
		t.Error("expected error for missing file")
	}
}

func TestLoadSuiteScenario_InvalidYAML(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "bad.yaml")
	if err := os.WriteFile(path, []byte(":::invalid yaml{{{"), 0o644); err != nil {
		t.Fatal(err)
	}

	_, err := LoadSuiteScenario(path)
	if err == nil {
		t.Error("expected error for invalid YAML")
	}
}
