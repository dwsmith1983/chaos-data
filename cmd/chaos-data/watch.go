package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/dwsmith1983/chaos-data/adapters/local"
	"github.com/spf13/cobra"
)

func watchCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "watch",
		Short: "Auto-release held objects when their hold time expires",
		RunE: func(cmd *cobra.Command, _ []string) error {
			inputDir, err := cmd.Flags().GetString("input")
			if err != nil {
				return fmt.Errorf("read --input flag: %w", err)
			}
			outputDir, err := cmd.Flags().GetString("output")
			if err != nil {
				return fmt.Errorf("read --output flag: %w", err)
			}
			pollStr, err := cmd.Flags().GetString("poll-interval")
			if err != nil {
				return fmt.Errorf("read --poll-interval flag: %w", err)
			}
			pollInterval, err := time.ParseDuration(pollStr)
			if err != nil {
				return fmt.Errorf("invalid --poll-interval %q: %w", pollStr, err)
			}

			transport := local.NewFSTransport(inputDir, outputDir)
			out := cmd.OutOrStdout()

			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}
			ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
			defer stop()

			ticker := time.NewTicker(pollInterval)
			defer ticker.Stop()

			var totalReleased int

			for {
				select {
				case <-ctx.Done():
					if totalReleased > 0 {
						fmt.Fprintf(out, "watch stopped: released %d objects total\n", totalReleased)
					}
					return nil
				case <-ticker.C:
					held, listErr := transport.ListHeld(ctx)
					if listErr != nil {
						fmt.Fprintf(out, "warning: list held: %v\n", listErr)
						continue
					}
					for _, obj := range held {
						if obj.HeldUntil.IsZero() {
							continue
						}
						if !time.Now().After(obj.HeldUntil) {
							continue
						}

						// Orphaned sidecar protection: if data file is empty/missing,
						// clean up via Release (which may error, that's OK).
						if obj.Size == 0 {
							fmt.Fprintf(out, "warning: orphaned sidecar for %s, cleaning up\n", obj.Key)
							_ = transport.Release(ctx, obj.Key)
							continue
						}

						if releaseErr := transport.Release(ctx, obj.Key); releaseErr != nil {
							fmt.Fprintf(out, "warning: release %s: %v\n", obj.Key, releaseErr)
							continue
						}
						fmt.Fprintf(out, "released %s (held until %s)\n", obj.Key, obj.HeldUntil.Format(time.RFC3339))
						totalReleased++
					}
				}
			}
		},
	}

	cmd.Flags().StringP("input", "i", "", "Input staging directory")
	cmd.Flags().StringP("output", "o", "", "Output directory")
	cmd.Flags().String("poll-interval", "10s", "How often to check for expired holds")

	_ = cmd.MarkFlagRequired("input")
	_ = cmd.MarkFlagRequired("output")

	return cmd
}
