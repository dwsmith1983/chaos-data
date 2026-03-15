package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestRunAPI_Catalog(t *testing.T) {
	t.Parallel()

	input := bytes.NewBufferString(`{"action":"catalog","params":{}}`)
	output := &bytes.Buffer{}

	err := runAPI(input, output)
	if err != nil {
		t.Fatalf("runAPI() error = %v", err)
	}

	var resp apiResponse
	if err := json.NewDecoder(output).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !resp.Success {
		t.Errorf("expected success=true, got error: %s", resp.Error)
	}
	if resp.Data == nil {
		t.Error("expected data to be non-nil")
	}
}

func TestRunAPI_CatalogContainsEntries(t *testing.T) {
	t.Parallel()

	input := bytes.NewBufferString(`{"action":"catalog","params":{}}`)
	output := &bytes.Buffer{}

	err := runAPI(input, output)
	if err != nil {
		t.Fatalf("runAPI() error = %v", err)
	}

	var resp apiResponse
	if err := json.NewDecoder(output).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !resp.Success {
		t.Fatalf("expected success=true, got error: %s", resp.Error)
	}

	// Data should be a list with at least one entry.
	entries, ok := resp.Data.([]interface{})
	if !ok {
		t.Fatalf("expected data to be a list, got %T", resp.Data)
	}
	if len(entries) == 0 {
		t.Fatal("expected at least one catalog entry")
	}

	// First entry should have expected fields.
	first, ok := entries[0].(map[string]interface{})
	if !ok {
		t.Fatalf("expected entry to be a map, got %T", entries[0])
	}
	for _, field := range []string{"name", "description", "category", "severity", "probability"} {
		if _, exists := first[field]; !exists {
			t.Errorf("expected catalog entry to have field %q", field)
		}
	}
}

func TestRunAPI_UnknownAction(t *testing.T) {
	t.Parallel()

	input := bytes.NewBufferString(`{"action":"bogus","params":{}}`)
	output := &bytes.Buffer{}

	err := runAPI(input, output)
	if err != nil {
		t.Fatalf("runAPI() error = %v", err)
	}

	var resp apiResponse
	if err := json.NewDecoder(output).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Success {
		t.Error("expected success=false for unknown action")
	}
	if resp.Error == "" {
		t.Error("expected non-empty error message for unknown action")
	}
}

func TestRunAPI_InvalidJSON(t *testing.T) {
	t.Parallel()

	input := bytes.NewBufferString(`not json`)
	output := &bytes.Buffer{}

	err := runAPI(input, output)
	if err != nil {
		t.Fatalf("runAPI() error = %v", err)
	}

	var resp apiResponse
	if err := json.NewDecoder(output).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Success {
		t.Error("expected success=false for invalid JSON")
	}
	if resp.Error == "" {
		t.Error("expected non-empty error message for invalid JSON")
	}
}

func TestRunAPI_RunMissingParams(t *testing.T) {
	t.Parallel()

	input := bytes.NewBufferString(`{"action":"run","params":{}}`)
	output := &bytes.Buffer{}

	err := runAPI(input, output)
	if err != nil {
		t.Fatalf("runAPI() error = %v", err)
	}

	var resp apiResponse
	if err := json.NewDecoder(output).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Success {
		t.Error("expected success=false for missing params")
	}
	if resp.Error == "" {
		t.Error("expected non-empty error message for missing params")
	}
}

func TestRunAPI_RunMissingScenario(t *testing.T) {
	t.Parallel()

	input := bytes.NewBufferString(`{"action":"run","params":{"input":"/tmp/in","output":"/tmp/out"}}`)
	output := &bytes.Buffer{}

	err := runAPI(input, output)
	if err != nil {
		t.Fatalf("runAPI() error = %v", err)
	}

	var resp apiResponse
	if err := json.NewDecoder(output).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Success {
		t.Error("expected success=false when scenario param is missing")
	}
}

func TestRunAPI_RunMissingInput(t *testing.T) {
	t.Parallel()

	input := bytes.NewBufferString(`{"action":"run","params":{"scenario":"late-data","output":"/tmp/out"}}`)
	output := &bytes.Buffer{}

	err := runAPI(input, output)
	if err != nil {
		t.Fatalf("runAPI() error = %v", err)
	}

	var resp apiResponse
	if err := json.NewDecoder(output).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Success {
		t.Error("expected success=false when input param is missing")
	}
}

func TestRunAPI_RunEndToEnd(t *testing.T) {
	t.Parallel()

	// Create temp input and output directories.
	inputDir := t.TempDir()
	outputDir := t.TempDir()

	// Write a test JSONL file to input.
	testData := `{"id":1,"name":"alice","score":100}
{"id":2,"name":"bob","score":200}
`
	testFile := filepath.Join(inputDir, "test-data.jsonl")
	if err := os.WriteFile(testFile, []byte(testData), 0o644); err != nil {
		t.Fatalf("write test file: %v", err)
	}

	reqJSON, err := json.Marshal(apiRequest{
		Action: "run",
		Params: map[string]string{
			"scenario": "late-data",
			"input":    inputDir,
			"output":   outputDir,
		},
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}

	in := bytes.NewBuffer(reqJSON)
	out := &bytes.Buffer{}

	if err := runAPI(in, out); err != nil {
		t.Fatalf("runAPI() error = %v", err)
	}

	var resp apiResponse
	if err := json.NewDecoder(out).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !resp.Success {
		t.Fatalf("expected success=true, got error: %s", resp.Error)
	}
	if resp.Data == nil {
		t.Error("expected data to be non-nil for successful run")
	}
}

func TestRunAPI_RunInvalidScenario(t *testing.T) {
	t.Parallel()

	input := bytes.NewBufferString(`{"action":"run","params":{"scenario":"nonexistent","input":"/tmp/in","output":"/tmp/out"}}`)
	output := &bytes.Buffer{}

	err := runAPI(input, output)
	if err != nil {
		t.Fatalf("runAPI() error = %v", err)
	}

	var resp apiResponse
	if err := json.NewDecoder(output).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Success {
		t.Error("expected success=false for invalid scenario name")
	}
}

func TestAPICmd_Help(t *testing.T) {
	t.Parallel()

	cmd := rootCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{"api", "--help"})

	err := cmd.Execute()
	if err != nil {
		t.Fatalf("api --help failed: %v", err)
	}

	output := buf.String()
	if !bytes.Contains([]byte(output), []byte("JSON stdin/stdout API")) {
		t.Errorf("expected help to mention 'JSON stdin/stdout API', got:\n%s", output)
	}
}

func TestRunAPI_StatusAction(t *testing.T) {
	t.Parallel()

	inputDir := t.TempDir()
	outputDir := t.TempDir()

	reqJSON, err := json.Marshal(apiRequest{
		Action: "status",
		Params: map[string]string{
			"input":  inputDir,
			"output": outputDir,
		},
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}

	in := bytes.NewBuffer(reqJSON)
	out := &bytes.Buffer{}

	if err := runAPI(in, out); err != nil {
		t.Fatalf("runAPI() error = %v", err)
	}

	var resp apiResponse
	if err := json.NewDecoder(out).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !resp.Success {
		t.Errorf("expected success=true, got error: %s", resp.Error)
	}
}

func TestRunAPI_StatusAction_MissingParams(t *testing.T) {
	t.Parallel()

	input := bytes.NewBufferString(`{"action":"status","params":{}}`)
	output := &bytes.Buffer{}

	if err := runAPI(input, output); err != nil {
		t.Fatalf("runAPI() error = %v", err)
	}

	var resp apiResponse
	if err := json.NewDecoder(output).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Success {
		t.Error("expected success=false for missing params")
	}
}

func TestRunAPI_ReleaseAction(t *testing.T) {
	t.Parallel()

	inputDir := t.TempDir()
	outputDir := t.TempDir()

	reqJSON, err := json.Marshal(apiRequest{
		Action: "release",
		Params: map[string]string{
			"input":  inputDir,
			"output": outputDir,
		},
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}

	in := bytes.NewBuffer(reqJSON)
	out := &bytes.Buffer{}

	if err := runAPI(in, out); err != nil {
		t.Fatalf("runAPI() error = %v", err)
	}

	var resp apiResponse
	if err := json.NewDecoder(out).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !resp.Success {
		t.Errorf("expected success=true, got error: %s", resp.Error)
	}
}

func TestRunAPI_ReleaseAction_MissingParams(t *testing.T) {
	t.Parallel()

	input := bytes.NewBufferString(`{"action":"release","params":{}}`)
	output := &bytes.Buffer{}

	if err := runAPI(input, output); err != nil {
		t.Fatalf("runAPI() error = %v", err)
	}

	var resp apiResponse
	if err := json.NewDecoder(output).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Success {
		t.Error("expected success=false for missing params")
	}
}

func TestRunAPI_InjectAction(t *testing.T) {
	t.Parallel()

	inputDir := t.TempDir()
	outputDir := t.TempDir()
	scenarioDir := t.TempDir()

	scenarioYAML := `name: api-inject-test
description: API inject test
category: state-consistency
severity: low
version: 1
target:
  layer: state
mutation:
  type: phantom-sensor
  params:
    pipeline: api-pipe
    sensor_key: health
probability: 1.0
safety:
  max_affected_pct: 100
  cooldown: 1m
  sla_aware: false
`
	scenarioFile := filepath.Join(scenarioDir, "api-inject.yaml")
	if err := os.WriteFile(scenarioFile, []byte(scenarioYAML), 0o644); err != nil {
		t.Fatalf("write scenario file: %v", err)
	}

	reqJSON, err := json.Marshal(apiRequest{
		Action: "inject",
		Params: map[string]string{
			"scenario": scenarioFile,
			"input":    inputDir,
			"output":   outputDir,
			"state_db": ":memory:",
		},
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}

	in := bytes.NewBuffer(reqJSON)
	out := &bytes.Buffer{}

	if err := runAPI(in, out); err != nil {
		t.Fatalf("runAPI() error = %v", err)
	}

	var resp apiResponse
	if err := json.NewDecoder(out).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !resp.Success {
		t.Errorf("expected success=true, got error: %s", resp.Error)
	}
	if resp.Data == nil {
		t.Error("expected data to be non-nil for successful inject")
	}
}

func TestRunAPI_InjectAction_MissingParams(t *testing.T) {
	t.Parallel()

	input := bytes.NewBufferString(`{"action":"inject","params":{}}`)
	output := &bytes.Buffer{}

	if err := runAPI(input, output); err != nil {
		t.Fatalf("runAPI() error = %v", err)
	}

	var resp apiResponse
	if err := json.NewDecoder(output).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Success {
		t.Error("expected success=false for missing params")
	}
}
