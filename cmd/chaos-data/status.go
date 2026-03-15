package main

import (
	"context"
	"fmt"
	"text/tabwriter"
	"time"

	"github.com/dwsmith1983/chaos-data/adapters/local"
	"github.com/spf13/cobra"
)

// statusCmd returns a cobra command that lists objects currently held by the
// chaos transport (i.e., delayed files sitting in .chaos-hold/).
func statusCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "status",
		Short: "List objects currently held by chaos injection",
		RunE: func(cmd *cobra.Command, _ []string) error {
			inputDir, err := cmd.Flags().GetString("input")
			if err != nil {
				return fmt.Errorf("read --input flag: %w", err)
			}
			outputDir, err := cmd.Flags().GetString("output")
			if err != nil {
				return fmt.Errorf("read --output flag: %w", err)
			}

			transport := local.NewFSTransport(inputDir, outputDir)

			ctx := context.Background()
			held, err := transport.ListHeld(ctx)
			if err != nil {
				return fmt.Errorf("list held objects: %w", err)
			}

			out := cmd.OutOrStdout()

			if len(held) == 0 {
				fmt.Fprintln(out, "No held objects.")
				return nil
			}

			fmt.Fprintf(out, "%d held object(s):\n\n", len(held))

			w := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
			fmt.Fprintln(w, "KEY\tSIZE\tLAST_MODIFIED")
			for _, obj := range held {
				fmt.Fprintf(w, "%s\t%d\t%s\n",
					obj.Key,
					obj.Size,
					obj.LastModified.Format(time.RFC3339),
				)
			}
			if err := w.Flush(); err != nil {
				return fmt.Errorf("flush output: %w", err)
			}

			return nil
		},
	}

	cmd.Flags().StringP("input", "i", "", "Input staging directory")
	cmd.Flags().StringP("output", "o", "", "Output directory")

	_ = cmd.MarkFlagRequired("input")
	_ = cmd.MarkFlagRequired("output")

	return cmd
}
