package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestCLI_ParseFlags(t *testing.T) {
	t.Run("Default flags", func(t *testing.T) {
		cmd := engineCmd()
		timeout, _ := cmd.Flags().GetDuration("timeout")
		if timeout.Seconds() != 30 {
			t.Errorf("Expected 30s timeout, got %v", timeout)
		}
		format, _ := cmd.Flags().GetString("format")
		if format != "text" {
			t.Errorf("Expected text format, got %s", format)
		}
		gen, _ := cmd.Flags().GetString("generator")
		if gen != "all" {
			t.Errorf("Expected all generator default, got %s", gen)
		}
	})

	t.Run("JSON format", func(t *testing.T) {
		cmd := engineCmd()
		if err := cmd.ParseFlags([]string{"--format", "json"}); err != nil {
			t.Fatalf("ParseFlags: %v", err)
		}
		format, _ := cmd.Flags().GetString("format")
		if format != "json" {
			t.Errorf("Expected json format, got %s", format)
		}
	})

	t.Run("Custom timeout", func(t *testing.T) {
		cmd := engineCmd()
		if err := cmd.ParseFlags([]string{"--timeout", "5s"}); err != nil {
			t.Fatalf("ParseFlags: %v", err)
		}
		timeout, _ := cmd.Flags().GetDuration("timeout")
		if timeout.Seconds() != 5 {
			t.Errorf("Expected 5s timeout, got %v", timeout)
		}
	})
}

func TestCLI_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	t.Run("JSON output contains generators", func(t *testing.T) {
		cmd := rootCmd()
		buf := new(bytes.Buffer)
		cmd.SetOut(buf)
		cmd.SetArgs([]string{"engine", "--generator", "all", "--format", "json"})
		if err := cmd.Execute(); err != nil {
			t.Fatalf("Execute: %v", err)
		}
		out := buf.String()
		if !strings.Contains(out, "boundary") {
			t.Error("Expected output to contain boundary generator")
		}
	})

	t.Run("Single generator output", func(t *testing.T) {
		cmd := rootCmd()
		buf := new(bytes.Buffer)
		cmd.SetOut(buf)
		cmd.SetArgs([]string{"engine", "--generator", "boundary", "--format", "json"})
		if err := cmd.Execute(); err != nil {
			t.Fatalf("Execute: %v", err)
		}
		out := buf.String()
		if !strings.Contains(out, "boundary") {
			t.Error("Expected output to contain boundary generator")
		}
	})

	t.Run("Invalid generator returns error", func(t *testing.T) {
		cmd := rootCmd()
		cmd.SetArgs([]string{"engine", "--generator", "nonexistent"})
		err := cmd.Execute()
		if err == nil {
			t.Error("Expected error for nonexistent generator")
		}
	})
}
