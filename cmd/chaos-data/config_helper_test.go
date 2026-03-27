package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestResolveConfigPath_ExplicitReturnsAsIs(t *testing.T) {
	t.Parallel()

	got := resolveConfigPath("/some/explicit/path.yaml")
	if got != "/some/explicit/path.yaml" {
		t.Errorf("expected explicit path returned as-is, got %q", got)
	}
}

func TestResolveConfigPath_FindsChaosYamlInCwd(t *testing.T) {
	// Cannot be parallel: uses os.Chdir.
	dir := t.TempDir()
	chaosFile := filepath.Join(dir, "chaos.yaml")
	if err := os.WriteFile(chaosFile, []byte("test: true\n"), 0o644); err != nil {
		t.Fatalf("write chaos.yaml: %v", err)
	}

	orig, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatalf("chdir: %v", err)
	}
	t.Cleanup(func() { _ = os.Chdir(orig) })

	got := resolveConfigPath("")
	if got != "chaos.yaml" {
		t.Errorf("expected 'chaos.yaml', got %q", got)
	}
}

func TestResolveConfigPath_FindsXDGConfig(t *testing.T) {
	// Cannot be parallel: uses t.Setenv and os.Chdir.
	fakeHome := t.TempDir()
	xdgDir := filepath.Join(fakeHome, ".config", "chaos-data")
	if err := os.MkdirAll(xdgDir, 0o755); err != nil {
		t.Fatalf("mkdirall: %v", err)
	}
	configFile := filepath.Join(xdgDir, "config.yaml")
	if err := os.WriteFile(configFile, []byte("test: true\n"), 0o644); err != nil {
		t.Fatalf("write config.yaml: %v", err)
	}

	t.Setenv("HOME", fakeHome)

	emptyDir := t.TempDir()
	orig, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	if err := os.Chdir(emptyDir); err != nil {
		t.Fatalf("chdir: %v", err)
	}
	t.Cleanup(func() { _ = os.Chdir(orig) })

	got := resolveConfigPath("")
	if got != configFile {
		t.Errorf("expected %q, got %q", configFile, got)
	}
}

func TestResolveConfigPath_ReturnsEmptyWhenNothingFound(t *testing.T) {
	// Cannot be parallel: uses t.Setenv and os.Chdir.
	emptyDir := t.TempDir()
	fakeHome := t.TempDir()

	t.Setenv("HOME", fakeHome)

	orig, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	if err := os.Chdir(emptyDir); err != nil {
		t.Fatalf("chdir: %v", err)
	}
	t.Cleanup(func() { _ = os.Chdir(orig) })

	got := resolveConfigPath("")
	if got != "" {
		t.Errorf("expected empty string, got %q", got)
	}
}

func TestResolveConfigPath_CwdTakesPrecedenceOverXDG(t *testing.T) {
	// Cannot be parallel: uses t.Setenv and os.Chdir.
	dir := t.TempDir()
	chaosFile := filepath.Join(dir, "chaos.yaml")
	if err := os.WriteFile(chaosFile, []byte("cwd: true\n"), 0o644); err != nil {
		t.Fatalf("write chaos.yaml: %v", err)
	}

	fakeHome := t.TempDir()
	xdgDir := filepath.Join(fakeHome, ".config", "chaos-data")
	if err := os.MkdirAll(xdgDir, 0o755); err != nil {
		t.Fatalf("mkdirall: %v", err)
	}
	xdgFile := filepath.Join(xdgDir, "config.yaml")
	if err := os.WriteFile(xdgFile, []byte("xdg: true\n"), 0o644); err != nil {
		t.Fatalf("write xdg config: %v", err)
	}

	t.Setenv("HOME", fakeHome)

	orig, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatalf("chdir: %v", err)
	}
	t.Cleanup(func() { _ = os.Chdir(orig) })

	got := resolveConfigPath("")
	if got != "chaos.yaml" {
		t.Errorf("expected 'chaos.yaml' (cwd precedence), got %q", got)
	}
}
