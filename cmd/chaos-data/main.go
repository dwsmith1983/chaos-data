// Package main provides the chaos-data CLI for chaos testing data pipelines.
package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

// rootCmd builds the top-level cobra command tree.
func rootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:   "chaos-data",
		Short: "Chaos testing for data pipelines",
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	root.AddCommand(catalogCmd())
	root.AddCommand(runCmd())

	return root
}

func main() {
	if err := rootCmd().Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
