package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/dwsmith1983/chaos-data/chaosdata"
	"github.com/spf13/cobra"
)

// runEngine is the actual engine runner implementation reading from stdin
func runEngine(cmd *cobra.Command, args []string) error {
	timeout, _ := cmd.Flags().GetDuration("timeout")
	generatorName, _ := cmd.Flags().GetString("generator")
	format, _ := cmd.Flags().GetString("format")

	_, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// Read stdin
	var input []byte
	if stat, _ := os.Stdin.Stat(); (stat.Mode() & os.ModeCharDevice) == 0 {
		input, _ = io.ReadAll(os.Stdin)
	}

	var results []map[string]interface{}

	if generatorName == "all" {
		for _, g := range chaosdata.All() {
			p, err := g.Generate(chaosdata.GenerateOpts{Count: 1})
			if err != nil {
				return err
			}
			res := map[string]interface{}{
				"generator": g.Name(),
				"payload":   string(p.Data),
			}
			if len(input) > 0 {
				res["base_input"] = string(input)
			}
			results = append(results, res)
		}
	} else if generatorName != "" {
		found := false
		for _, g := range chaosdata.All() {
			if g.Name() == generatorName || g.Category() == generatorName {
				p, err := g.Generate(chaosdata.GenerateOpts{Count: 1})
				if err != nil {
					return err
				}
				res := map[string]interface{}{
					"generator": g.Name(),
					"payload":   string(p.Data),
				}
				if len(input) > 0 {
					res["base_input"] = string(input)
				}
				results = append(results, res)
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("generator not found: %s", generatorName)
		}
	}

	if format == "json" {
		b, _ := json.Marshal(results)
		fmt.Fprintln(cmd.OutOrStdout(), string(b))
	} else {
		for _, r := range results {
			if base, ok := r["base_input"]; ok {
				fmt.Fprintf(cmd.OutOrStdout(), "%s (input: %s): %s\n", r["generator"], base, r["payload"])
			} else {
				fmt.Fprintf(cmd.OutOrStdout(), "%s: %s\n", r["generator"], r["payload"])
			}
		}
	}

	return nil
}

func engineCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "engine",
		Short: "Engine runner processing stdin",
		RunE:  runEngine,
	}
	cmd.Flags().String("generator", "all", "Generator to run")
	cmd.Flags().String("format", "text", "Output format")
	cmd.Flags().Duration("timeout", 30*time.Second, "Timeout")
	return cmd
}
