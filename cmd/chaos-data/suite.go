package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	interlocksuite "github.com/dwsmith1983/chaos-data/suites/interlock"
)

// suiteCmd returns a cobra command tree for running chaos testing suites.
func suiteCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "suite",
		Short: "Run chaos testing suites",
	}
	cmd.AddCommand(suiteRunCmd())
	cmd.AddCommand(suiteMatrixCmd())
	cmd.AddCommand(suiteGapsCmd())
	return cmd
}

// suiteRunCmd returns the "suite run" subcommand.
func suiteRunCmd() *cobra.Command {
	var dir, format string
	cmd := &cobra.Command{
		Use:   "run",
		Short: "Run a chaos testing suite",
		RunE: func(_ *cobra.Command, _ []string) error {
			ct, err := interlocksuite.NewCoverageTracker(dir + "/coverage.yaml")
			if err != nil {
				return fmt.Errorf("load coverage registry: %w", err)
			}

			matrix := ct.Matrix()
			return writeMatrix(matrix, format)
		},
	}
	cmd.Flags().StringVar(&dir, "dir", "suites/interlock", "Suite directory")
	cmd.Flags().StringVar(&format, "format", "table", "Output format: table, json, md")
	return cmd
}

// suiteMatrixCmd returns the "suite matrix" subcommand.
func suiteMatrixCmd() *cobra.Command {
	var dir, format string
	cmd := &cobra.Command{
		Use:   "matrix",
		Short: "Show coverage matrix without running scenarios",
		RunE: func(_ *cobra.Command, _ []string) error {
			ct, err := interlocksuite.NewCoverageTracker(dir + "/coverage.yaml")
			if err != nil {
				return fmt.Errorf("load coverage registry: %w", err)
			}

			matrix := ct.Matrix()
			return writeMatrix(matrix, format)
		},
	}
	cmd.Flags().StringVar(&dir, "dir", "suites/interlock", "Suite directory")
	cmd.Flags().StringVar(&format, "format", "table", "Output format: table, json, md")
	return cmd
}

// suiteGapsCmd returns the "suite gaps" subcommand.
func suiteGapsCmd() *cobra.Command {
	var dir string
	cmd := &cobra.Command{
		Use:   "gaps",
		Short: "Show only GAP and UNTESTED capabilities",
		RunE: func(_ *cobra.Command, _ []string) error {
			ct, err := interlocksuite.NewCoverageTracker(dir + "/coverage.yaml")
			if err != nil {
				return fmt.Errorf("load coverage registry: %w", err)
			}

			matrix := ct.Matrix()

			var filtered []interlocksuite.CapabilityResult
			for _, r := range matrix.Results {
				if r.Status != interlocksuite.StatusCovered {
					filtered = append(filtered, r)
				}
			}
			filteredMatrix := interlocksuite.CoverageMatrix{
				Results:  filtered,
				Total:    matrix.Total,
				Covered:  matrix.Covered,
				Gaps:     matrix.Gaps,
				Untested: matrix.Untested,
			}
			return interlocksuite.FormatTable(filteredMatrix, os.Stdout)
		},
	}
	cmd.Flags().StringVar(&dir, "dir", "suites/interlock", "Suite directory")
	return cmd
}

// writeMatrix dispatches matrix output to the requested formatter.
func writeMatrix(matrix interlocksuite.CoverageMatrix, format string) error {
	switch format {
	case "json":
		return interlocksuite.FormatJSON(matrix, os.Stdout)
	case "md", "markdown":
		return interlocksuite.FormatMarkdown(matrix, os.Stdout)
	default:
		return interlocksuite.FormatTable(matrix, os.Stdout)
	}
}
