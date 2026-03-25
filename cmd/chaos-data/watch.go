package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"

	chaosaws "github.com/dwsmith1983/chaos-data/adapters/aws"
	"github.com/dwsmith1983/chaos-data/adapters/local"
	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
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

			emitterFlag, err := cmd.Flags().GetString("emitter")
			if err != nil {
				return fmt.Errorf("read --emitter flag: %w", err)
			}
			region, err := cmd.Flags().GetString("region")
			if err != nil {
				return fmt.Errorf("read --region flag: %w", err)
			}
			eventBus, err := cmd.Flags().GetString("event-bus")
			if err != nil {
				return fmt.Errorf("read --event-bus flag: %w", err)
			}

			transport := local.NewFSTransport(inputDir, outputDir)
			out := cmd.OutOrStdout()

			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}
			ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
			defer stop()

			var emitter adapter.EventEmitter
			switch emitterFlag {
			case "stdout":
				emitter = local.NewStdoutEmitter(cmd.OutOrStdout())
			case "eventbridge":
				if region == "" {
					return errors.New("--region is required when --emitter=eventbridge")
				}
				awsCfg, loadErr := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
				if loadErr != nil {
					return fmt.Errorf("load aws config: %w", loadErr)
				}
				ebClient := eventbridge.NewFromConfig(awsCfg)
				emitter = chaosaws.NewEventBridgeEmitter(ebClient, eventBus)
			case "none":
				emitter = nil
			default:
				return fmt.Errorf("unknown emitter %q: must be stdout, eventbridge, or none", emitterFlag)
			}

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

						if emitter != nil {
							event := types.ChaosEvent{
								ID:       fmt.Sprintf("watch-%s-%d", obj.Key, time.Now().UnixNano()),
								Scenario: "watch",
								Category: "data-arrival",
								Severity: types.SeverityLow,
								Target:   obj.Key,
								Mutation: "object-released",
								Params: map[string]string{
									"held_until":  obj.HeldUntil.Format(time.RFC3339),
									"released_at": time.Now().UTC().Format(time.RFC3339),
								},
								Timestamp: time.Now().UTC(),
								Mode:      "deterministic",
							}
							if emitErr := emitter.Emit(ctx, event); emitErr != nil {
								fmt.Fprintf(out, "warning: emit event for %s: %v\n", obj.Key, emitErr)
							}
						}
					}
				}
			}
		},
	}

	cmd.Flags().StringP("input", "i", "", "Input staging directory")
	cmd.Flags().StringP("output", "o", "", "Output directory")
	cmd.Flags().String("poll-interval", "10s", "How often to check for expired holds")
	cmd.Flags().String("emitter", "stdout", "Event emitter: stdout, eventbridge, or none")
	cmd.Flags().String("region", "", "AWS region (required when --emitter=eventbridge)")
	cmd.Flags().String("event-bus", "default", "EventBridge bus name")

	_ = cmd.MarkFlagRequired("input")
	_ = cmd.MarkFlagRequired("output")

	return cmd
}
