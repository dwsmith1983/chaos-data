package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestWatchCmd_ReleasesExpiredObject(t *testing.T) {
	t.Parallel()

	staging := t.TempDir()
	output := t.TempDir()
	holdDir := filepath.Join(staging, ".chaos-hold")
	if err := os.MkdirAll(holdDir, 0o755); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(holdDir, "test.jsonl"), []byte(`{"id":1}`), 0o644); err != nil {
		t.Fatal(err)
	}

	meta := struct {
		ReleaseAt time.Time `json:"release_at"`
	}{ReleaseAt: time.Now().Add(-time.Minute)}
	metaBytes, _ := json.Marshal(meta)
	if err := os.WriteFile(filepath.Join(holdDir, "test.jsonl.meta"), metaBytes, 0o644); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	cmd := rootCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{
		"watch",
		"--input", staging,
		"--output", output,
		"--poll-interval", "50ms",
	})
	cmd.SetContext(ctx)
	_ = cmd.Execute()

	outputPath := filepath.Join(output, "test.jsonl")
	if _, err := os.Stat(outputPath); err != nil {
		t.Errorf("expected released file at %s: %v", outputPath, err)
	}

	if _, err := os.Stat(filepath.Join(holdDir, "test.jsonl")); !os.IsNotExist(err) {
		t.Error("expected held file to be removed after release")
	}
}

func TestWatchCmd_DoesNotReleaseUnexpired(t *testing.T) {
	t.Parallel()

	staging := t.TempDir()
	output := t.TempDir()
	holdDir := filepath.Join(staging, ".chaos-hold")
	if err := os.MkdirAll(holdDir, 0o755); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(holdDir, "future.jsonl"), []byte(`{"id":1}`), 0o644); err != nil {
		t.Fatal(err)
	}

	meta := struct {
		ReleaseAt time.Time `json:"release_at"`
	}{ReleaseAt: time.Now().Add(time.Hour)}
	metaBytes, _ := json.Marshal(meta)
	if err := os.WriteFile(filepath.Join(holdDir, "future.jsonl.meta"), metaBytes, 0o644); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	cmd := rootCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{
		"watch",
		"--input", staging,
		"--output", output,
		"--poll-interval", "50ms",
	})
	cmd.SetContext(ctx)
	_ = cmd.Execute()

	if _, err := os.Stat(filepath.Join(holdDir, "future.jsonl")); err != nil {
		t.Error("held file should still be in hold dir")
	}
	if _, err := os.Stat(filepath.Join(output, "future.jsonl")); !os.IsNotExist(err) {
		t.Error("file should not be in output yet")
	}
}

func TestWatchCmd_SkipsZeroHeldUntil(t *testing.T) {
	t.Parallel()

	staging := t.TempDir()
	output := t.TempDir()
	holdDir := filepath.Join(staging, ".chaos-hold")
	if err := os.MkdirAll(holdDir, 0o755); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(holdDir, "nosidecar.jsonl"), []byte(`{"id":1}`), 0o644); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	cmd := rootCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{
		"watch",
		"--input", staging,
		"--output", output,
		"--poll-interval", "50ms",
	})
	cmd.SetContext(ctx)
	_ = cmd.Execute()

	if _, err := os.Stat(filepath.Join(holdDir, "nosidecar.jsonl")); err != nil {
		t.Error("file with zero HeldUntil should remain in hold dir")
	}
}

func TestWatchCmd_EmptyHoldDir(t *testing.T) {
	t.Parallel()

	staging := t.TempDir()
	output := t.TempDir()

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	cmd := rootCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{
		"watch",
		"--input", staging,
		"--output", output,
		"--poll-interval", "50ms",
	})
	cmd.SetContext(ctx)

	err := cmd.Execute()
	if err != nil {
		t.Fatalf("watch with empty hold dir should not error: %v", err)
	}
}

func TestWatchCmd_EmitterStdout_EmitsEvent(t *testing.T) {
	t.Parallel()

	staging := t.TempDir()
	output := t.TempDir()
	holdDir := filepath.Join(staging, ".chaos-hold")
	if err := os.MkdirAll(holdDir, 0o755); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(holdDir, "emit.jsonl"), []byte(`{"id":1}`), 0o644); err != nil {
		t.Fatal(err)
	}

	meta := struct {
		ReleaseAt time.Time `json:"release_at"`
	}{ReleaseAt: time.Now().Add(-time.Minute)}
	metaBytes, _ := json.Marshal(meta)
	if err := os.WriteFile(filepath.Join(holdDir, "emit.jsonl.meta"), metaBytes, 0o644); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	cmd := rootCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{
		"watch",
		"--input", staging,
		"--output", output,
		"--poll-interval", "50ms",
		"--emitter", "stdout",
	})
	cmd.SetContext(ctx)
	_ = cmd.Execute()

	got := buf.String()
	if !strings.Contains(got, "object-released") {
		t.Errorf("expected stdout emitter output to contain 'object-released', got:\n%s", got)
	}
}

func TestWatchCmd_EmitterNone_NoEvents(t *testing.T) {
	t.Parallel()

	staging := t.TempDir()
	output := t.TempDir()
	holdDir := filepath.Join(staging, ".chaos-hold")
	if err := os.MkdirAll(holdDir, 0o755); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(holdDir, "noemit.jsonl"), []byte(`{"id":1}`), 0o644); err != nil {
		t.Fatal(err)
	}

	meta := struct {
		ReleaseAt time.Time `json:"release_at"`
	}{ReleaseAt: time.Now().Add(-time.Minute)}
	metaBytes, _ := json.Marshal(meta)
	if err := os.WriteFile(filepath.Join(holdDir, "noemit.jsonl.meta"), metaBytes, 0o644); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	cmd := rootCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{
		"watch",
		"--input", staging,
		"--output", output,
		"--poll-interval", "50ms",
		"--emitter", "none",
	})
	cmd.SetContext(ctx)
	_ = cmd.Execute()

	got := buf.String()
	if strings.Contains(got, "object-released") {
		t.Errorf("expected no event emission with --emitter=none, but got:\n%s", got)
	}
	if !strings.Contains(got, "released") {
		t.Errorf("expected human-readable release log line, got:\n%s", got)
	}
}

func TestWatchCmd_EmitterEventBridge_NoRegion_Fails(t *testing.T) {
	t.Parallel()

	staging := t.TempDir()
	output := t.TempDir()

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	cmd := rootCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{
		"watch",
		"--input", staging,
		"--output", output,
		"--poll-interval", "50ms",
		"--emitter", "eventbridge",
	})
	cmd.SetContext(ctx)

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error when --emitter=eventbridge without --region")
	}
	if !strings.Contains(err.Error(), "region") {
		t.Errorf("expected error to mention 'region', got: %v", err)
	}
}

func TestWatchCmd_EmitterUnknown_Fails(t *testing.T) {
	t.Parallel()

	staging := t.TempDir()
	output := t.TempDir()

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	cmd := rootCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{
		"watch",
		"--input", staging,
		"--output", output,
		"--poll-interval", "50ms",
		"--emitter", "kafka",
	})
	cmd.SetContext(ctx)

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error for unknown emitter 'kafka'")
	}
	if !strings.Contains(err.Error(), "kafka") {
		t.Errorf("expected error to mention 'kafka', got: %v", err)
	}
}
