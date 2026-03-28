package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/spf13/cobra"

	chaosaws "github.com/dwsmith1983/chaos-data/adapters/aws"
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
	var dir, format, target, stateTable, eventsTable string
	cmd := &cobra.Command{
		Use:   "run",
		Short: "Run a chaos testing suite",
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx := cmd.Context()

			// 1. Create clock.
			clk := adapter.NewWallClock()

			// 2. Load coverage tracker.
			ct, err := interlocksuite.NewCoverageTracker(filepath.Join(dir, "coverage.yaml"))
			if err != nil {
				return fmt.Errorf("load coverage: %w", err)
			}

			// 3. Build target-specific dependencies.
			var (
				store             adapter.StateStore
				eventReader       interlocksuite.InterlockEventReader
				evaluator         interlocksuite.InterlockEvaluator
				compositeAsserter *interlocksuite.CompositeAsserter
				cleanup           func()
			)

			switch target {
			case "local":
				sqlStore, err := local.NewSQLiteState(":memory:")
				if err != nil {
					return fmt.Errorf("create state store: %w", err)
				}
				cleanup = func() { sqlStore.Close() }
				store = sqlStore

				localReader := interlocksuite.NewLocalEventReader()
				eventReader = localReader
				evaluator = interlocksuite.NewLocalInterlockEvaluator(sqlStore, localReader, clk)

				suiteAsserter := interlocksuite.NewSuiteAsserter(localReader)
				triggerAsserter := interlocksuite.NewTriggerStateAsserter(sqlStore)
				compositeAsserter = interlocksuite.NewCompositeAsserter(suiteAsserter, triggerAsserter)

			case "aws":
				cfg, err := awsconfig.LoadDefaultConfig(ctx)
				if err != nil {
					return fmt.Errorf("load aws config: %w", err)
				}
				ddbClient := dynamodb.NewFromConfig(cfg)

				ddbState := chaosaws.NewDynamoDBState(ddbClient, stateTable)
				store = ddbState
				cleanup = func() {} // no cleanup needed for DynamoDB

				awsReader := interlocksuite.NewAWSEventReader(ddbClient, eventsTable)
				eventReader = awsReader
				evaluator = interlocksuite.NewAWSInterlockEvaluator()

				suiteAsserter := interlocksuite.NewSuiteAsserter(awsReader)
				triggerAsserter := interlocksuite.NewTriggerStateAsserter(ddbState)
				compositeAsserter = interlocksuite.NewCompositeAsserter(suiteAsserter, triggerAsserter)

			default:
				return fmt.Errorf("unsupported target %q (must be \"local\" or \"aws\")", target)
			}
			defer cleanup()

			// 4. Create mutation registry with interlock mutations.
			reg := mutation.NewRegistry()
			if err := interlockadapter.RegisterAll(reg, store, interlockadapter.Config{}); err != nil {
				return fmt.Errorf("register mutations: %w", err)
			}

			// 5. Create suite runner.
			runner := interlocksuite.NewSuiteRunner(store, reg, ct,
				interlocksuite.WithSuiteClock(clk),
				interlocksuite.WithSuiteEvaluator(evaluator),
				interlocksuite.WithSuiteEventReader(eventReader),
				interlocksuite.WithSuiteAsserter(compositeAsserter),
			)

			// 6. Load and run all scenarios.
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
					runner.RunScenario(ctx, ss)
				}
			}

			// 7. Output matrix.
			matrix := runner.Report()
			return writeMatrix(matrix, format, os.Stdout)
		},
	}
	cmd.Flags().StringVar(&dir, "dir", "suites/interlock", "Suite directory")
	cmd.Flags().StringVar(&format, "format", "table", "Output format: table, json, md")
	cmd.Flags().StringVar(&target, "target", "local", "Execution target: local, aws")
	cmd.Flags().StringVar(&stateTable, "state-table", "chaos-data-state", "DynamoDB state table name (aws target)")
	cmd.Flags().StringVar(&eventsTable, "events-table", "interlock-events", "DynamoDB events table name (aws target)")
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
