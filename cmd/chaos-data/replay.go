package main

import (
	"context"
	"fmt"
	"os"

	"github.com/dwsmith1983/chaos-data/adapters/local"
	"github.com/dwsmith1983/chaos-data/pkg/engine"
	"github.com/dwsmith1983/chaos-data/pkg/scenario"
	"github.com/dwsmith1983/chaos-data/pkg/types"
	"github.com/spf13/cobra"
)

// replayCmd returns a cobra command that replays a chaos experiment from a
// JSONL manifest file, re-applying the exact same mutations in order.
func replayCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "replay",
		Short: "Replay a chaos experiment from a manifest file",
		RunE: func(cmd *cobra.Command, _ []string) error {
			manifestPath, err := cmd.Flags().GetString("manifest")
			if err != nil {
				return fmt.Errorf("read --manifest flag: %w", err)
			}
			inputDir, err := cmd.Flags().GetString("input")
			if err != nil {
				return fmt.Errorf("read --input flag: %w", err)
			}
			outputDir, err := cmd.Flags().GetString("output")
			if err != nil {
				return fmt.Errorf("read --output flag: %w", err)
			}

			// Read the manifest file.
			manifest, err := os.ReadFile(manifestPath)
			if err != nil {
				return fmt.Errorf("read manifest file %q: %w", manifestPath, err)
			}

			transport := local.NewFSTransport(inputDir, outputDir)
			registry := defaultRegistry()

			// Load all catalog scenarios so the engine has them available
			// (though replay uses the manifest events directly, not scenarios).
			scenarios, err := scenario.BuiltinCatalog()
			if err != nil {
				return fmt.Errorf("load catalog: %w", err)
			}

			eng := engine.New(
				types.EngineConfig{
					Mode:   "replay",
					Safety: types.Defaults().Safety,
				},
				transport,
				registry,
				scenarios,
			)

			ctx := context.Background()
			records, err := eng.ReplayFromManifest(ctx, manifest)
			if err != nil {
				return fmt.Errorf("replay: %w", err)
			}

			return printRecords(cmd, records)
		},
	}

	cmd.Flags().StringP("manifest", "m", "", "Path to JSONL manifest file")
	cmd.Flags().StringP("input", "i", "", "Input staging directory")
	cmd.Flags().StringP("output", "o", "", "Output directory")

	_ = cmd.MarkFlagRequired("manifest")
	_ = cmd.MarkFlagRequired("input")
	_ = cmd.MarkFlagRequired("output")

	return cmd
}
