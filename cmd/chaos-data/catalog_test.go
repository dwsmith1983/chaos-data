package main

import (
	"bytes"
	"testing"
)

func TestCatalogCmd_ListsScenarios(t *testing.T) {
	t.Parallel()

	cmd := rootCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{"catalog"})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("catalog command failed: %v", err)
	}

	output := buf.String()

	// The built-in catalog contains at least the late-data scenario.
	if !bytes.Contains([]byte(output), []byte("late-data")) {
		t.Errorf("expected output to contain %q, got:\n%s", "late-data", output)
	}

	// Verify key columns are present in the header.
	for _, col := range []string{"NAME", "CATEGORY", "SEVERITY", "DESCRIPTION"} {
		if !bytes.Contains([]byte(output), []byte(col)) {
			t.Errorf("expected output to contain column header %q, got:\n%s", col, output)
		}
	}
}

func TestCatalogCmd_ShowsCategory(t *testing.T) {
	t.Parallel()

	cmd := rootCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{"catalog"})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("catalog command failed: %v", err)
	}

	output := buf.String()

	// late-data scenario has category "data-arrival".
	if !bytes.Contains([]byte(output), []byte("data-arrival")) {
		t.Errorf("expected output to contain %q, got:\n%s", "data-arrival", output)
	}
}

func TestCatalogCmd_ShowsSeverity(t *testing.T) {
	t.Parallel()

	cmd := rootCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{"catalog"})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("catalog command failed: %v", err)
	}

	output := buf.String()

	// late-data scenario has severity "low".
	if !bytes.Contains([]byte(output), []byte("low")) {
		t.Errorf("expected output to contain severity %q, got:\n%s", "low", output)
	}
}
