package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/dwsmith1983/chaos-data/adapters/local"
	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/config"
	"github.com/dwsmith1983/chaos-data/pkg/engine"
	"github.com/dwsmith1983/chaos-data/pkg/mutation"
	"github.com/dwsmith1983/chaos-data/pkg/scenario"
	"github.com/dwsmith1983/chaos-data/pkg/types"
	"github.com/spf13/cobra"
)

// serveCmd returns a cobra command that runs the chaos engine in continuous
// probabilistic mode, selecting scenarios based on their probability fields
// at a configurable interval until the duration expires.
func serveCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Run chaos engine in continuous probabilistic mode",
		RunE: func(cmd *cobra.Command, _ []string) error {
			inputDir, err := cmd.Flags().GetString("input")
			if err != nil {
				return fmt.Errorf("read --input flag: %w", err)
			}
			outputDir, err := cmd.Flags().GetString("output")
			if err != nil {
				return fmt.Errorf("read --output flag: %w", err)
			}
			interval, err := cmd.Flags().GetDuration("interval")
			if err != nil {
				return fmt.Errorf("read --interval flag: %w", err)
			}
			duration, err := cmd.Flags().GetDuration("duration")
			if err != nil {
				return fmt.Errorf("read --duration flag: %w", err)
			}
			dryRun, err := cmd.Flags().GetBool("dry-run")
			if err != nil {
				return fmt.Errorf("read --dry-run flag: %w", err)
			}
			assertWait, err := cmd.Flags().GetBool("assert-wait")
			if err != nil {
				return fmt.Errorf("read --assert-wait flag: %w", err)
			}

			// Load all built-in catalog scenarios.
			allScenarios, err := scenario.BuiltinCatalog()
			if err != nil {
				return fmt.Errorf("load catalog: %w", err)
			}

			registry := defaultRegistry()

			// Filter to only scenarios whose mutation type is registered.
			scenarios := filterByRegistry(allScenarios, registry)
			if len(scenarios) == 0 {
				return fmt.Errorf("no runnable scenarios found in catalog")
			}

			// Load config.
			configFlag, _ := cmd.Flags().GetString("config")
			configPath := resolveConfigPath(configFlag)
			var fileCfg config.Config
			if configPath != "" {
				var loadErr error
				fileCfg, loadErr = config.Load(configPath)
				if loadErr != nil {
					return loadErr
				}
				if err := fileCfg.Validate(); err != nil {
					return err
				}
			}

			ctx, cancel := context.WithTimeout(context.Background(), duration)
			defer cancel()

			// Transport: CLI flags override config.
			var transport adapter.DataTransport
			if inputDir != "" && outputDir != "" {
				transport = local.NewFSTransport(inputDir, outputDir)
			} else {
				var buildErr error
				transport, buildErr = fileCfg.BuildTransport(ctx)
				if buildErr != nil {
					return buildErr
				}
				if transport == nil {
					return errors.New("--input and --output are required when no transport is configured")
				}
			}

			// Safety from config (defaults to ConfigSafety).
			safety, safeErr := fileCfg.BuildSafety(ctx)
			if safeErr != nil {
				return safeErr
			}

			// Emitter from config (defaults to StdoutEmitter).
			emitter, emitErr := fileCfg.BuildEmitter(ctx, cmd.OutOrStdout())
			if emitErr != nil {
				return emitErr
			}

			// Asserter from config.
			asserter, assertErr := fileCfg.BuildAsserter()
			if assertErr != nil {
				return assertErr
			}

			// Build engine options.
			var opts []engine.EngineOption
			if emitter != nil {
				opts = append(opts, engine.WithEmitter(emitter))
			}
			if safety != nil {
				opts = append(opts, engine.WithSafety(safety))
			}
			if asserter != nil {
				opts = append(opts, engine.WithAsserter(asserter))
			}

			engCfg := types.EngineConfig{
				Mode:   "probabilistic",
				Safety: types.Defaults().Safety,
				DryRun: dryRun,
			}
			if assertWait {
				engCfg.AssertWait = true
				engCfg.AssertPollInterval = types.Duration{Duration: time.Second}
			}

			eng := engine.New(
				engCfg,
				transport,
				registry,
				scenarios,
				opts...,
			)

			rng := rand.New(rand.NewSource(time.Now().UnixNano())) //nolint:gosec

			records, err := eng.RunProbabilistic(ctx, interval, rng)
			if err != nil {
				return fmt.Errorf("probabilistic run: %w", err)
			}

			if assertWait {
				// Collect scenarios that had mutations applied.
				scenarioApplied := make(map[string]bool)
				for _, r := range records {
					if r.Applied {
						scenarioApplied[r.Scenario] = true
					}
				}
				var appliedScenarios []scenario.Scenario
				for _, sc := range scenarios {
					if sc.Expected != nil && scenarioApplied[sc.Name] {
						appliedScenarios = append(appliedScenarios, sc)
					}
				}
				if len(appliedScenarios) > 0 {
					assertResults := eng.EvaluateAssertions(ctx, appliedScenarios)
					if len(assertResults) > 0 {
						out := cmd.OutOrStdout()
						fmt.Fprintf(out, "\n%d assertion(s) evaluated:\n", len(assertResults))
						for _, r := range assertResults {
							status := "UNSATISFIED"
							if r.Satisfied {
								status = "SATISFIED"
							}
							fmt.Fprintf(out, "  %s %s %s: %s\n", r.Assertion.Type, r.Assertion.Target, r.Assertion.Condition, status)
							if r.Error != "" {
								fmt.Fprintf(out, "    error: %s\n", r.Error)
							}
						}
					}
				}
			}

			return printRecords(cmd, records)
		},
	}

	cmd.Flags().StringP("input", "i", "", "Input staging directory")
	cmd.Flags().StringP("output", "o", "", "Output directory")
	cmd.Flags().DurationP("interval", "n", 30*time.Second, "Interval between chaos iterations")
	cmd.Flags().DurationP("duration", "d", 1*time.Hour, "Total duration to run")
	cmd.Flags().Bool("dry-run", false, "Preview mutations without applying them")
	cmd.Flags().Bool("assert-wait", false, "Evaluate assertions after probabilistic run completes")

	return cmd
}

// filterByRegistry returns only scenarios whose mutation type is available
// in the given registry. Scenarios referencing unregistered mutations are
// silently excluded. The input slice is not modified.
func filterByRegistry(scenarios []scenario.Scenario, reg *mutation.Registry) []scenario.Scenario {
	result := make([]scenario.Scenario, 0, len(scenarios))
	for _, sc := range scenarios {
		if _, err := reg.Get(sc.Mutation.Type); err == nil {
			result = append(result, sc)
		}
	}
	return result
}
