package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"

	interlockadapter "github.com/dwsmith1983/chaos-data/adapters/interlock"
	"github.com/dwsmith1983/chaos-data/adapters/local"
	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/mutation"
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
		RunE: func(cmd *cobra.Command, _ []string) error {
			// 1. Create state store.
			store, err := local.NewSQLiteState(":memory:")
			if err != nil {
				return fmt.Errorf("create state store: %w", err)
			}
			defer store.Close()

			// 2. Create clock.
			clk := adapter.NewWallClock()

			// 3. Load coverage tracker.
			ct, err := interlocksuite.NewCoverageTracker(filepath.Join(dir, "coverage.yaml"))
			if err != nil {
				return fmt.Errorf("load coverage: %w", err)
			}

			// 4. Create mutation registry with interlock mutations.
			reg := mutation.NewRegistry()
			if err := interlockadapter.RegisterAll(reg, store, interlockadapter.Config{}); err != nil {
				return fmt.Errorf("register mutations: %w", err)
			}

			// 5. Create event reader and evaluator.
			eventReader := interlocksuite.NewLocalEventReader()
			evaluator := interlocksuite.NewLocalInterlockEvaluator(store, eventReader, clk)

			// 6. Create suite runner.
			runner := interlocksuite.NewSuiteRunner(store, reg, ct,
				interlocksuite.WithSuiteClock(clk),
				interlocksuite.WithSuiteEvaluator(evaluator),
				interlocksuite.WithSuiteEventReader(eventReader),
			)

			// 7. Load and run all scenarios.
			scenarioDir := filepath.Join(dir, "scenarios")
			entries, err := os.ReadDir(scenarioDir)
			if err != nil {
				return fmt.Errorf("read scenarios dir: %w", err)
			}
			for _, category := range entries {
				if !category.IsDir() {
					continue
				}
				catPath := filepath.Join(scenarioDir, category.Name())
				files, _ := filepath.Glob(filepath.Join(catPath, "*.yaml"))
				for _, f := range files {
					ss, err := interlocksuite.LoadSuiteScenario(f)
					if err != nil {
						fmt.Fprintf(os.Stderr, "skip %s: %v\n", f, err)
						continue
					}
					runner.RunScenario(cmd.Context(), ss)
				}
			}

			// 8. Output matrix.
			matrix := runner.Report()
			return writeMatrix(matrix, format, os.Stdout)
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
			return writeMatrix(matrix, format, os.Stdout)
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
func writeMatrix(matrix interlocksuite.CoverageMatrix, format string, w io.Writer) error {
	switch format {
	case "json":
		return interlocksuite.FormatJSON(matrix, w)
	case "md", "markdown":
		return interlocksuite.FormatMarkdown(matrix, w)
	default:
		return interlocksuite.FormatTable(matrix, w)
	}
}
