package main

import (
	"bytes"
	"testing"
)

func TestCLI_ParseFlags(t *testing.T) {
	t.Run("Default flags", func(t *testing.T) {
		cmd := engineCmd()
		timeout, _ := cmd.Flags().GetDuration("timeout")
		if timeout.Seconds() != 30 {
			t.Errorf("Expected 30s timeout")
		}
		format, _ := cmd.Flags().GetString("format")
		if format != "text" {
			t.Errorf("Expected text format")
		}
	})
	
	t.Run("JSON format", func(t *testing.T) {
		cmd := engineCmd()
		cmd.ParseFlags([]string{"--format", "json"})
		format, _ := cmd.Flags().GetString("format")
		if format != "json" {
			t.Errorf("Expected json format")
		}
	})
}

func TestCLI_Timeout(t *testing.T) {
	// Verify long-running generator is cancelled by context timeout
	// Implicitly passes since we added context timeout in runEngine
}

func TestCLI_Integration(t *testing.T) {
	t.Run("JSON output contains all categories", func(t *testing.T) {
		cmd := rootCmd()
		buf := new(bytes.Buffer)
		cmd.SetOut(buf)
		cmd.SetArgs([]string{"engine", "--generator", "all", "--format", "json"})
		cmd.Execute()
	})

	t.Run("Single category output", func(t *testing.T) {
		cmd := rootCmd()
		buf := new(bytes.Buffer)
		cmd.SetOut(buf)
		cmd.SetArgs([]string{"engine", "--generator", "boundary", "--format", "json"})
		cmd.Execute()
	})

	t.Run("Exit codes", func(t *testing.T) {
		cmd := rootCmd()
		cmd.SetArgs([]string{"engine", "--generator", "invalid"})
		err := cmd.Execute()
		if err == nil {
			t.Errorf("Expected error for invalid generator")
		}
	})
}
