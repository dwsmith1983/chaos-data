package main

import (
	"context"
	"fmt"
	"time"

	"github.com/dwsmith1983/chaos-data/adapters/interlock"
	"github.com/dwsmith1983/chaos-data/adapters/local"
	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/config"
	"github.com/dwsmith1983/chaos-data/pkg/engine"
	"github.com/dwsmith1983/chaos-data/pkg/mutation"
	"github.com/dwsmith1983/chaos-data/pkg/scenario"
	"github.com/dwsmith1983/chaos-data/pkg/types"
	"github.com/spf13/cobra"
)

// fullStatefulRegistry creates a mutation registry with all built-in mutations
// including state mutations that require a StateStore.
func fullStatefulRegistry(store adapter.StateStore) *mutation.Registry {
	r := mutation.NewRegistry()

	dataMutations := []mutation.Mutation{
		&mutation.DelayMutation{},
		&mutation.DropMutation{},
		&mutation.CorruptMutation{},
		&mutation.DuplicateMutation{},
		&mutation.EmptyMutation{},
		&mutation.SchemaDriftMutation{},
		&mutation.StaleReplayMutation{},
		&mutation.MultiDayMutation{},
		&mutation.PartialMutation{},
		&mutation.SlowWriteMutation{},
		&mutation.StreamingLagMutation{},
		&mutation.RollingDegradationMutation{},
		&mutation.OutOfOrderMutation{},
		&mutation.PostRunDriftMutation{},
	}
	for _, m := range dataMutations {
		if err := r.Register(m); err != nil {
			panic(fmt.Sprintf("register built-in mutation %s: %v", m.Type(), err))
		}
	}

	stateMutations := []mutation.Mutation{
		mutation.NewStaleSensorMutation(store),
		mutation.NewPhantomSensorMutation(store),
		mutation.NewSplitSensorMutation(store),
		mutation.NewSensorFlappingMutation(store),
		mutation.NewTimestampForgeryMutation(store),
		mutation.NewPhantomTriggerMutation(store),
		mutation.NewJobKillMutation(store),
		mutation.NewTriggerTimeoutMutation(store),
		mutation.NewFalseSuccessMutation(store),
		mutation.NewCascadeDelayMutation(store),
	}
	for _, m := range stateMutations {
		if err := r.Register(m); err != nil {
			panic(fmt.Sprintf("register state mutation %s: %v", m.Type(), err))
		}
	}

	return r
}

// injectCmd returns a cobra command that injects a single chaos scenario
// directly into a synthetic DataObject, targeting state or data layers.
func injectCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "inject",
		Short: "Inject a single chaos scenario immediately",
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
			stateDB, err := cmd.Flags().GetString("state-db")
			if err != nil {
				return fmt.Errorf("read --state-db flag: %w", err)
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

			stateStore, err := local.NewSQLiteState(stateDB)
			if err != nil {
				return fmt.Errorf("open state store %q: %w", stateDB, err)
			}
			defer stateStore.Close()

			transport := local.NewFSTransport(inputDir, outputDir)
			registry := fullStatefulRegistry(stateStore)

			cfg := types.EngineConfig{
				Mode: "deterministic",
				Safety: types.SafetyConfig{
					MaxSeverity:    types.SeverityCritical,
					MaxAffectedPct: 100,
					MaxPipelines:   100,
				},
				DryRun:     dryRun,
				AssertWait: assertWait,
			}
			if assertWait {
				cfg.AssertPollInterval = types.Duration{Duration: time.Second}
			}

			var opts []engine.EngineOption
			configFlag, _ := cmd.Flags().GetString("config")
			configPath := resolveConfigPath(configFlag)
			if configPath != "" {
				fileCfg, loadErr := config.Load(configPath)
				if loadErr != nil {
					return loadErr
				}
				if valErr := fileCfg.Validate(); valErr != nil {
					return valErr
				}
				a, buildErr := fileCfg.BuildAsserter()
				if buildErr != nil {
					return buildErr
				}
				if a != nil {
					opts = append(opts, engine.WithAsserter(a))
				}
			} else {
				reader := local.NewNoopEventReader()
				a := interlock.NewAdapterAsserter(stateStore, reader)
				opts = append(opts, engine.WithAsserter(a))
			}

			eng := engine.New(
				cfg,
				transport,
				registry,
				[]scenario.Scenario{sc},
				opts...,
			)

			ctx := context.Background()
			obj := types.DataObject{Key: "inject"}
			records, err := eng.ProcessObject(ctx, obj)
			if err != nil {
				return fmt.Errorf("inject: %w", err)
			}

			printErr := printRecords(cmd, records)

			if assertWait && sc.Expected != nil && len(sc.Expected.Asserts) > 0 {
				assertResults := eng.EvaluateAssertions(ctx, []scenario.Scenario{sc})
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

			return printErr
		},
	}

	cmd.Flags().StringP("scenario", "s", "", "Scenario name (from catalog) or path to YAML file")
	cmd.Flags().StringP("input", "i", "", "Input staging directory")
	cmd.Flags().StringP("output", "o", "", "Output directory")
	cmd.Flags().String("state-db", ":memory:", "SQLite state database path (use :memory: for ephemeral)")
	cmd.Flags().Bool("dry-run", false, "Preview injection without applying side effects")
	cmd.Flags().Bool("assert-wait", false, "Block until assertions are satisfied or timeout (polls every 1s)")

	_ = cmd.MarkFlagRequired("scenario")
	_ = cmd.MarkFlagRequired("input")
	_ = cmd.MarkFlagRequired("output")

	return cmd
}
