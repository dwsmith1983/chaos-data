package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

// --- Step 14.2: serve command tests ---

func TestServeCmd_Creation(t *testing.T) {
	t.Parallel()

	cmd := rootCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{"serve", "--help"})

	err := cmd.Execute()
	if err != nil {
		t.Fatalf("serve --help failed: %v", err)
	}

	output := buf.String()
	if !bytes.Contains([]byte(output), []byte("continuous probabilistic mode")) {
		t.Errorf("expected help to mention 'continuous probabilistic mode', got:\n%s", output)
	}
}

func TestServeCmd_RequiresInputFlag(t *testing.T) {
	t.Parallel()

	cmd := rootCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{
		"serve",
		"--output", "/tmp/out",
	})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error when --input is missing")
	}
}

func TestServeCmd_RequiresOutputFlag(t *testing.T) {
	t.Parallel()

	cmd := rootCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{
		"serve",
		"--input", "/tmp/in",
	})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error when --output is missing")
	}
}

func TestServeCmd_FlagParsing(t *testing.T) {
	t.Parallel()

	cmd := rootCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{
		"serve",
		"--help",
	})

	err := cmd.Execute()
	if err != nil {
		t.Fatalf("serve --help failed: %v", err)
	}

	output := buf.String()
	// Verify flags are registered.
	for _, flag := range []string{"--input", "--output", "--interval", "--duration"} {
		if !bytes.Contains([]byte(output), []byte(flag)) {
			t.Errorf("expected help to mention flag %q, got:\n%s", flag, output)
		}
	}
}

func TestServeCmd_EndToEnd(t *testing.T) {
	t.Parallel()

	// Create temp directories.
	inputDir := t.TempDir()
	outputDir := t.TempDir()

	// Write a test data file to input.
	testData := `{"id":1,"name":"alice"}
`
	testFile := filepath.Join(inputDir, "data.jsonl")
	if err := os.WriteFile(testFile, []byte(testData), 0o644); err != nil {
		t.Fatalf("write test file: %v", err)
	}

	cmd := rootCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{
		"serve",
		"--input", inputDir,
		"--output", outputDir,
		"--interval", "50ms",
		"--duration", "200ms",
	})

	err := cmd.Execute()
	if err != nil {
		t.Fatalf("serve command failed: %v\noutput: %s", err, buf.String())
	}

	output := buf.String()
	// Should print a summary with mutation count.
	if !bytes.Contains([]byte(output), []byte("mutation")) {
		t.Errorf("expected output to mention mutations, got:\n%s", output)
	}
}
