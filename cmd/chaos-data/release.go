package main

import (
	"context"
	"fmt"

	"github.com/dwsmith1983/chaos-data/adapters/local"
	"github.com/spf13/cobra"
)

// releaseCmd returns a cobra command that releases held objects from the chaos
// transport, moving them from .chaos-hold/ to the output directory.
func releaseCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "release",
		Short: "Release held objects back to the output directory",
		RunE: func(cmd *cobra.Command, _ []string) error {
			inputDir, err := cmd.Flags().GetString("input")
			if err != nil {
				return fmt.Errorf("read --input flag: %w", err)
			}
			outputDir, err := cmd.Flags().GetString("output")
			if err != nil {
				return fmt.Errorf("read --output flag: %w", err)
			}
			key, err := cmd.Flags().GetString("key")
			if err != nil {
				return fmt.Errorf("read --key flag: %w", err)
			}

			transport := local.NewFSTransport(inputDir, outputDir)
			ctx := context.Background()
			out := cmd.OutOrStdout()

			if key != "" {
				if err := transport.Release(ctx, key); err != nil {
					return fmt.Errorf("release %q: %w", key, err)
				}
				fmt.Fprintf(out, "Released: %s\n", key)
				return nil
			}

			if err := transport.ReleaseAll(ctx); err != nil {
				return fmt.Errorf("release all: %w", err)
			}

			fmt.Fprintln(out, "All held objects released.")
			return nil
		},
	}

	cmd.Flags().StringP("input", "i", "", "Input staging directory")
	cmd.Flags().StringP("output", "o", "", "Output directory")
	cmd.Flags().StringP("key", "k", "", "Release a specific held object by key (optional; omit to release all)")

	_ = cmd.MarkFlagRequired("input")
	_ = cmd.MarkFlagRequired("output")

	return cmd
}
