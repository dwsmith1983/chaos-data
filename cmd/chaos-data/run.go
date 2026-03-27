package main

import (
	"context"
	"errors"
	"fmt"
	"text/tabwriter"
	"time"

	"github.com/dwsmith1983/chaos-data/adapters/local"
	"github.com/dwsmith1983/chaos-data/pkg/config"
	"github.com/dwsmith1983/chaos-data/pkg/engine"
	"github.com/dwsmith1983/chaos-data/pkg/mutation"
	"github.com/dwsmith1983/chaos-data/pkg/scenario"
	"github.com/dwsmith1983/chaos-data/pkg/types"
	"github.com/spf13/cobra"
)

// defaultRegistry creates a mutation registry populated with all built-in mutations.
func defaultRegistry() *mutation.Registry {
	r := mutation.NewRegistry()
	for _, m := range []mutation.Mutation{
		&mutation.DelayMutation{},
		&mutation.DropMutation{},
		&mutation.CorruptMutation{},
		&mutation.OutOfOrderMutation{},
		&mutation.PostRunDriftMutation{},
	} {
		if err := r.Register(m); err != nil {
			panic(fmt.Sprintf("register built-in mutation: %v", err))
		}
	}
	return r
}

// loadScenario resolves a scenario by name from the built-in catalog, or if
// not found, attempts to load it as a YAML file path.
func loadScenario(nameOrPath string) (scenario.Scenario, error) {
	s, err := scenario.Get(nameOrPath)
	if err == nil {
		return s, nil
	}

	// If not found in catalog, try as a file path.
	if errors.Is(err, scenario.ErrNotFound) {
		fileScenario, fileErr := scenario.LoadFile(nameOrPath)
		if fileErr != nil {
			return scenario.Scenario{}, fmt.Errorf(
				"scenario %q not found in catalog and failed to load as file: %w",
				nameOrPath, fileErr,
			)
		}
		return fileScenario, nil
	}

	return scenario.Scenario{}, err
}

// runCmd returns a cobra command that runs chaos scenarios against a data directory.
func runCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "run",
		Short: "Run chaos scenarios against a data directory",
		RunE: func(cmd *cobra.Command, _ []string) error {
			scenarioFlag, err := cmd.Flags().GetString("scenario")
			if err != nil {
				return fmt.Errorf("read --scenario flag: %w", err)
			}
			inputDir, err := cmd.Flags().GetString("input")
			if err != nil {
				return fmt.Errorf("read --input flag: %w", err)
			}
			outputDir, err := cmd.Flags().GetString("output")
			if err != nil {
				return fmt.Errorf("read --output flag: %w", err)
			}
			dryRun, err := cmd.Flags().GetBool("dry-run")
			if err != nil {
				return fmt.Errorf("read --dry-run flag: %w", err)
			}
			assertWait, err := cmd.Flags().GetBool("assert-wait")
			if err != nil {
				return fmt.Errorf("read --assert-wait flag: %w", err)
			}

			sc, err := loadScenario(scenarioFlag)
			if err != nil {
				return err
			}

			transport := local.NewFSTransport(inputDir, outputDir)
			emitter := local.NewStdoutEmitter(cmd.OutOrStdout())
			safety := local.NewConfigSafety(types.Defaults().Safety)
			registry := defaultRegistry()

			cfg := types.Defaults()
			cfg.DryRun = dryRun
			cfg.AssertWait = assertWait
			if assertWait {
				cfg.AssertPollInterval = types.Duration{Duration: time.Second}
			}

			var opts []engine.EngineOption
			opts = append(opts, engine.WithEmitter(emitter))
			opts = append(opts, engine.WithSafety(safety))

			configFlag, _ := cmd.Flags().GetString("config")
			configPath := resolveConfigPath(configFlag)
			if configPath != "" {
				fileCfg, loadErr := config.Load(configPath)
				if loadErr != nil {
					return loadErr
				}
				if err := fileCfg.Validate(); err != nil {
					return err
				}
				asserter, buildErr := fileCfg.BuildAsserter()
				if buildErr != nil {
					return buildErr
				}
				if asserter != nil {
					opts = append(opts, engine.WithAsserter(asserter))
				}
			}

			eng := engine.New(
				cfg,
				transport,
				registry,
				[]scenario.Scenario{sc},
				opts...,
			)

			ctx := context.Background()
			records, err := eng.Run(ctx)
			if err != nil {
				return fmt.Errorf("engine run: %w", err)
			}

			return printRecords(cmd, records)
		},
	}

	cmd.Flags().StringP("scenario", "s", "", "Scenario name (from catalog) or path to YAML file")
	cmd.Flags().StringP("input", "i", "", "Input staging directory")
	cmd.Flags().StringP("output", "o", "", "Output directory")
	cmd.Flags().Bool("dry-run", false, "Preview mutations without applying them")
	cmd.Flags().Bool("assert-wait", false, "Block until assertions are satisfied or timeout (polls every 1s)")

	_ = cmd.MarkFlagRequired("scenario")
	_ = cmd.MarkFlagRequired("input")
	_ = cmd.MarkFlagRequired("output")

	return cmd
}

// printRecords writes a summary table of mutation records.
func printRecords(cmd *cobra.Command, records []types.MutationRecord) error {
	out := cmd.OutOrStdout()

	if len(records) == 0 {
		fmt.Fprintln(out, "No mutations applied.")
		return nil
	}

	fmt.Fprintf(out, "\n%d mutation(s) applied:\n\n", len(records))

	w := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "OBJECT\tMUTATION\tAPPLIED\tERROR")

	for _, r := range records {
		fmt.Fprintf(w, "%s\t%s\t%t\t%s\n",
			r.ObjectKey,
			r.Mutation,
			r.Applied,
			r.Error,
		)
	}

	if err := w.Flush(); err != nil {
		return fmt.Errorf("flush output: %w", err)
	}

	_, _ = fmt.Fprintln(cmd.ErrOrStderr())
	return nil
}
