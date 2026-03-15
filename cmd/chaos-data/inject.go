package main

import (
	"context"
	"fmt"

	"github.com/dwsmith1983/chaos-data/adapters/local"
	"github.com/dwsmith1983/chaos-data/pkg/adapter"
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
				DryRun: dryRun,
			}

			eng := engine.New(
				cfg,
				transport,
				registry,
				[]scenario.Scenario{sc},
			)

			ctx := context.Background()
			obj := types.DataObject{Key: "inject"}
			records, err := eng.ProcessObject(ctx, obj)
			if err != nil {
				return fmt.Errorf("inject: %w", err)
			}

			return printRecords(cmd, records)
		},
	}

	cmd.Flags().StringP("scenario", "s", "", "Scenario name (from catalog) or path to YAML file")
	cmd.Flags().StringP("input", "i", "", "Input staging directory")
	cmd.Flags().StringP("output", "o", "", "Output directory")
	cmd.Flags().String("state-db", ":memory:", "SQLite state database path (use :memory: for ephemeral)")
	cmd.Flags().Bool("dry-run", false, "Preview injection without applying side effects")

	_ = cmd.MarkFlagRequired("scenario")
	_ = cmd.MarkFlagRequired("input")
	_ = cmd.MarkFlagRequired("output")

	return cmd
}
