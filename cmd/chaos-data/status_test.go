package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestStatusCmd_NoHeldObjects(t *testing.T) {
	t.Parallel()

	inputDir := t.TempDir()
	outputDir := t.TempDir()

	cmd := rootCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{
		"status",
		"--input", inputDir,
		"--output", outputDir,
	})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("status command failed: %v\noutput: %s", err, buf.String())
	}

	output := buf.String()
	if !bytes.Contains([]byte(output), []byte("No held objects")) {
		t.Errorf("expected output to contain 'No held objects', got:\n%s", output)
	}
}

func TestStatusCmd_WithHeldObjects(t *testing.T) {
	t.Parallel()

	inputDir := t.TempDir()
	outputDir := t.TempDir()

	// Pre-create the hold directory and a file + .meta sidecar to simulate
	// what FSTransport.Hold creates.
	holdDir := filepath.Join(inputDir, ".chaos-hold")
	if err := os.MkdirAll(holdDir, 0o755); err != nil {
		t.Fatalf("create hold dir: %v", err)
	}

	// Write the held data file.
	heldFile := filepath.Join(holdDir, "sensor-data.jsonl")
	if err := os.WriteFile(heldFile, []byte(`{"id":1}`), 0o644); err != nil {
		t.Fatalf("write held file: %v", err)
	}

	// Write the .meta sidecar (matches holdMeta struct in fs_transport.go).
	type holdMeta struct {
		ReleaseAt time.Time `json:"release_at"`
	}
	meta := holdMeta{ReleaseAt: time.Now().Add(30 * time.Minute)}
	metaBytes, err := json.Marshal(meta)
	if err != nil {
		t.Fatalf("marshal meta: %v", err)
	}
	metaFile := filepath.Join(holdDir, "sensor-data.jsonl.meta")
	if err := os.WriteFile(metaFile, metaBytes, 0o644); err != nil {
		t.Fatalf("write meta file: %v", err)
	}

	cmd := rootCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{
		"status",
		"--input", inputDir,
		"--output", outputDir,
	})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("status command failed: %v\noutput: %s", err, buf.String())
	}

	output := buf.String()
	if !bytes.Contains([]byte(output), []byte("sensor-data.jsonl")) {
		t.Errorf("expected output to contain held key 'sensor-data.jsonl', got:\n%s", output)
	}
}

func TestStatusCmd_RequiresInputFlag(t *testing.T) {
	t.Parallel()

	cmd := rootCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{
		"status",
		"--output", "/tmp/out",
	})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error when --input is missing")
	}
}

func TestStatusCmd_RequiresOutputFlag(t *testing.T) {
	t.Parallel()

	cmd := rootCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{
		"status",
		"--input", "/tmp/in",
	})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error when --output is missing")
	}
}

func TestStatusCmd_Help(t *testing.T) {
	t.Parallel()

	cmd := rootCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{"status", "--help"})

	err := cmd.Execute()
	if err != nil {
		t.Fatalf("status --help failed: %v", err)
	}

	output := buf.String()
	for _, flag := range []string{"--input", "--output"} {
		if !bytes.Contains([]byte(output), []byte(flag)) {
			t.Errorf("expected help to mention flag %q, got:\n%s", flag, output)
		}
	}
}
