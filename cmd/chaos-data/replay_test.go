package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// --- Step 14.3: replay command tests ---

func TestReplayCmd_Creation(t *testing.T) {
	t.Parallel()

	cmd := rootCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{"replay", "--help"})

	err := cmd.Execute()
	if err != nil {
		t.Fatalf("replay --help failed: %v", err)
	}

	output := buf.String()
	if !bytes.Contains([]byte(output), []byte("Replay a chaos experiment")) {
		t.Errorf("expected help to mention 'Replay a chaos experiment', got:\n%s", output)
	}
}

func TestReplayCmd_RequiresManifestFlag(t *testing.T) {
	t.Parallel()

	cmd := rootCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{
		"replay",
		"--input", "/tmp/in",
		"--output", "/tmp/out",
	})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error when --manifest is missing")
	}
}

func TestReplayCmd_RequiresInputFlag(t *testing.T) {
	t.Parallel()

	cmd := rootCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{
		"replay",
		"--manifest", "/tmp/manifest.jsonl",
		"--output", "/tmp/out",
	})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error when --input is missing")
	}
}

func TestReplayCmd_RequiresOutputFlag(t *testing.T) {
	t.Parallel()

	cmd := rootCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{
		"replay",
		"--manifest", "/tmp/manifest.jsonl",
		"--input", "/tmp/in",
	})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error when --output is missing")
	}
}

func TestReplayCmd_FlagParsing(t *testing.T) {
	t.Parallel()

	cmd := rootCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{"replay", "--help"})

	err := cmd.Execute()
	if err != nil {
		t.Fatalf("replay --help failed: %v", err)
	}

	output := buf.String()
	for _, flag := range []string{"--manifest", "--input", "--output"} {
		if !bytes.Contains([]byte(output), []byte(flag)) {
			t.Errorf("expected help to mention flag %q, got:\n%s", flag, output)
		}
	}
}

func TestReplayCmd_EndToEnd(t *testing.T) {
	t.Parallel()

	// Create temp directories.
	inputDir := t.TempDir()
	outputDir := t.TempDir()
	manifestDir := t.TempDir()

	// Write a test data file to input.
	testData := `{"id":1,"name":"alice"}
`
	testFile := filepath.Join(inputDir, "data.jsonl")
	if err := os.WriteFile(testFile, []byte(testData), 0o644); err != nil {
		t.Fatalf("write test file: %v", err)
	}

	// Create a manifest with a delay event targeting data.jsonl.
	event := types.ChaosEvent{
		ID:        "evt-replay-1",
		Scenario:  "late-data",
		Category:  "data-arrival",
		Severity:  types.SeverityLow,
		Target:    "data.jsonl",
		Mutation:  "delay",
		Params:    map[string]string{"duration": "10m", "release": "true"},
		Timestamp: time.Now(),
		Mode:      "replay",
	}

	eventData, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}

	manifestFile := filepath.Join(manifestDir, "manifest.jsonl")
	if err := os.WriteFile(manifestFile, append(eventData, '\n'), 0o644); err != nil {
		t.Fatalf("write manifest: %v", err)
	}

	cmd := rootCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{
		"replay",
		"--manifest", manifestFile,
		"--input", inputDir,
		"--output", outputDir,
	})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("replay command failed: %v\noutput: %s", err, buf.String())
	}

	output := buf.String()

	// Should mention the applied mutation.
	if !bytes.Contains([]byte(output), []byte("delay")) {
		t.Errorf("expected output to mention mutation type %q, got:\n%s", "delay", output)
	}

	// Should mention the object key.
	if !bytes.Contains([]byte(output), []byte("data.jsonl")) {
		t.Errorf("expected output to mention object key %q, got:\n%s", "data.jsonl", output)
	}
}
