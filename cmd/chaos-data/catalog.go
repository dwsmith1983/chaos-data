package main

import (
	"fmt"
	"text/tabwriter"

	"github.com/dwsmith1983/chaos-data/pkg/scenario"
	"github.com/spf13/cobra"
)

// catalogCmd returns a cobra command that lists all built-in chaos scenarios.
func catalogCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "catalog",
		Short: "List built-in chaos scenarios",
		RunE: func(cmd *cobra.Command, _ []string) error {
			scenarios, err := scenario.BuiltinCatalog()
			if err != nil {
				return fmt.Errorf("load catalog: %w", err)
			}

			w := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 4, 2, ' ', 0)
			fmt.Fprintln(w, "NAME\tCATEGORY\tSEVERITY\tDESCRIPTION")

			for _, s := range scenarios {
				fmt.Fprintf(w, "%s\t%s\t%s\t%s\n",
					s.Name,
					s.Category,
					s.Severity,
					s.Description,
				)
			}

			return w.Flush()
		},
	}
}
