// Package main provides the chaos-data CLI for chaos testing data pipelines.
package main

import (
	"fmt"
	"os"

	_ "github.com/dwsmith1983/chaos-data/chaosdata/boundary"
	_ "github.com/dwsmith1983/chaos-data/chaosdata/concurrency"
	_ "github.com/dwsmith1983/chaos-data/chaosdata/encoding"
	_ "github.com/dwsmith1983/chaos-data/chaosdata/gospecific"
	_ "github.com/dwsmith1983/chaos-data/chaosdata/injection"
	_ "github.com/dwsmith1983/chaos-data/chaosdata/nulls"
	_ "github.com/dwsmith1983/chaos-data/chaosdata/numeric"
	_ "github.com/dwsmith1983/chaos-data/chaosdata/protocol"
	_ "github.com/dwsmith1983/chaos-data/chaosdata/referential"
	_ "github.com/dwsmith1983/chaos-data/chaosdata/schemadrift"
	_ "github.com/dwsmith1983/chaos-data/chaosdata/structural"
	_ "github.com/dwsmith1983/chaos-data/chaosdata/temporal"
	_ "github.com/dwsmith1983/chaos-data/chaosdata/volume"

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

	root.PersistentFlags().String("config", "", "Path to chaos-data config file (YAML)")

	root.AddCommand(apiCmd())
	root.AddCommand(catalogCmd())
	root.AddCommand(injectCmd())
	root.AddCommand(releaseCmd())
	root.AddCommand(runCmd())
	root.AddCommand(serveCmd())
	root.AddCommand(replayCmd())
	root.AddCommand(statusCmd())
	root.AddCommand(watchCmd())
	root.AddCommand(suiteCmd())
	root.AddCommand(engineCmd())

	return root
}

func main() {
	if err := rootCmd().Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
