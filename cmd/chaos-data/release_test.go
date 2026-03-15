package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dwsmith1983/chaos-data/adapters/local"
)

func TestReleaseCmd_AllHeld(t *testing.T) {
	t.Parallel()

	inputDir := t.TempDir()
	outputDir := t.TempDir()

	// Write a file to input and hold it via FSTransport so the hold dir is
	// created with proper .meta sidecars.
	dataFile := filepath.Join(inputDir, "arrivals.jsonl")
	if err := os.WriteFile(dataFile, []byte(`{"id":1}`), 0o644); err != nil {
		t.Fatalf("write input file: %v", err)
	}

	transport := local.NewFSTransport(inputDir, outputDir)
	ctx := context.Background()
	if err := transport.Hold(ctx, "arrivals.jsonl", time.Now().Add(1*time.Hour)); err != nil {
		t.Fatalf("Hold: %v", err)
	}

	cmd := rootCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{
		"release",
		"--input", inputDir,
		"--output", outputDir,
	})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("release command failed: %v\noutput: %s", err, buf.String())
	}

	// The file should have been moved to outputDir.
	released := filepath.Join(outputDir, "arrivals.jsonl")
	if _, err := os.Stat(released); os.IsNotExist(err) {
		t.Errorf("expected released file at %s, but it does not exist", released)
	}
}

func TestReleaseCmd_SpecificKey(t *testing.T) {
	t.Parallel()

	inputDir := t.TempDir()
	outputDir := t.TempDir()

	// Create two files and hold both.
	for _, name := range []string{"file-a.jsonl", "file-b.jsonl"} {
		p := filepath.Join(inputDir, name)
		if err := os.WriteFile(p, []byte(`{"id":1}`), 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}

	transport := local.NewFSTransport(inputDir, outputDir)
	ctx := context.Background()
	for _, name := range []string{"file-a.jsonl", "file-b.jsonl"} {
		if err := transport.Hold(ctx, name, time.Now().Add(1*time.Hour)); err != nil {
			t.Fatalf("Hold %s: %v", name, err)
		}
	}

	// Release only file-a.jsonl.
	cmd := rootCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{
		"release",
		"--input", inputDir,
		"--output", outputDir,
		"--key", "file-a.jsonl",
	})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("release --key command failed: %v\noutput: %s", err, buf.String())
	}

	// file-a.jsonl must be in outputDir.
	if _, err := os.Stat(filepath.Join(outputDir, "file-a.jsonl")); os.IsNotExist(err) {
		t.Error("file-a.jsonl should have been released to outputDir")
	}

	// file-b.jsonl must still be held (not in outputDir).
	if _, err := os.Stat(filepath.Join(outputDir, "file-b.jsonl")); err == nil {
		t.Error("file-b.jsonl should still be held, not released")
	}

	// file-b.jsonl must still be in the hold directory.
	holdDir := filepath.Join(inputDir, ".chaos-hold")
	if _, err := os.Stat(filepath.Join(holdDir, "file-b.jsonl")); os.IsNotExist(err) {
		t.Error("file-b.jsonl should still be in .chaos-hold/")
	}
}

func TestReleaseCmd_NoHeldObjects(t *testing.T) {
	t.Parallel()

	inputDir := t.TempDir()
	outputDir := t.TempDir()

	cmd := rootCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{
		"release",
		"--input", inputDir,
		"--output", outputDir,
	})

	// Should succeed gracefully (no error) when nothing is held.
	if err := cmd.Execute(); err != nil {
		t.Fatalf("release with no held objects should not error: %v\noutput: %s", err, buf.String())
	}
}

func TestReleaseCmd_RequiresInputFlag(t *testing.T) {
	t.Parallel()

	cmd := rootCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{
		"release",
		"--output", "/tmp/out",
	})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error when --input is missing")
	}
}

func TestReleaseCmd_RequiresOutputFlag(t *testing.T) {
	t.Parallel()

	cmd := rootCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{
		"release",
		"--input", "/tmp/in",
	})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error when --output is missing")
	}
}
