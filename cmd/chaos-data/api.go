package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"

	"github.com/dwsmith1983/chaos-data/adapters/local"
	"github.com/dwsmith1983/chaos-data/pkg/engine"
	"github.com/dwsmith1983/chaos-data/pkg/scenario"
	"github.com/dwsmith1983/chaos-data/pkg/types"
	"github.com/spf13/cobra"
)

// apiRequest is the JSON envelope for API requests.
type apiRequest struct {
	Action string            `json:"action"`
	Params map[string]string `json:"params"`
}

// apiResponse is the JSON envelope for API responses.
type apiResponse struct {
	Success bool   `json:"success"`
	Data    any    `json:"data,omitempty"`
	Error   string `json:"error,omitempty"`
}

// apiCmd returns a cobra command that reads JSON requests from stdin
// and writes JSON responses to stdout.
func apiCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "api",
		Short: "JSON stdin/stdout API for programmatic access",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runAPI(cmd.InOrStdin(), cmd.OutOrStdout())
		},
	}
}

func runAPI(in io.Reader, out io.Writer) error {
	var req apiRequest
	if err := json.NewDecoder(in).Decode(&req); err != nil {
		return writeResponse(out, apiResponse{
			Success: false,
			Error:   fmt.Sprintf("invalid request: %v", err),
		})
	}

	switch req.Action {
	case "catalog":
		return handleCatalogAPI(out)
	case "run":
		return handleRunAPI(out, req.Params)
	default:
		return writeResponse(out, apiResponse{
			Success: false,
			Error:   fmt.Sprintf("unknown action: %q", req.Action),
		})
	}
}

// catalogEntry is the JSON representation of a scenario in API responses.
type catalogEntry struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	Category    string         `json:"category"`
	Severity    types.Severity `json:"severity"`
	Probability float64        `json:"probability"`
}

func handleCatalogAPI(out io.Writer) error {
	scenarios, err := scenario.BuiltinCatalog()
	if err != nil {
		return writeResponse(out, apiResponse{
			Success: false,
			Error:   fmt.Sprintf("load catalog: %v", err),
		})
	}

	entries := make([]catalogEntry, 0, len(scenarios))
	for _, s := range scenarios {
		entries = append(entries, catalogEntry{
			Name:        s.Name,
			Description: s.Description,
			Category:    s.Category,
			Severity:    s.Severity,
			Probability: s.Probability,
		})
	}

	return writeResponse(out, apiResponse{
		Success: true,
		Data:    entries,
	})
}

func handleRunAPI(out io.Writer, params map[string]string) error {
	scenarioName := params["scenario"]
	inputDir := params["input"]
	outputDir := params["output"]

	if scenarioName == "" || inputDir == "" || outputDir == "" {
		return writeResponse(out, apiResponse{
			Success: false,
			Error:   "missing required params: scenario, input, output",
		})
	}

	// Resolve to absolute paths to prevent relative path confusion.
	inputDir, err := filepath.Abs(inputDir)
	if err != nil {
		return writeResponse(out, apiResponse{
			Success: false,
			Error:   fmt.Sprintf("invalid input path: %v", err),
		})
	}
	outputDir, err = filepath.Abs(outputDir)
	if err != nil {
		return writeResponse(out, apiResponse{
			Success: false,
			Error:   fmt.Sprintf("invalid output path: %v", err),
		})
	}

	sc, err := loadScenario(scenarioName)
	if err != nil {
		return writeResponse(out, apiResponse{
			Success: false,
			Error:   fmt.Sprintf("load scenario: %v", err),
		})
	}

	transport := local.NewFSTransport(inputDir, outputDir)
	safety := local.NewConfigSafety(types.Defaults().Safety)
	registry := defaultRegistry()

	eng := engine.New(
		types.Defaults(),
		transport,
		registry,
		[]scenario.Scenario{sc},
		engine.WithSafety(safety),
	)

	ctx := context.Background()
	records, err := eng.Run(ctx)
	if err != nil {
		return writeResponse(out, apiResponse{
			Success: false,
			Error:   fmt.Sprintf("engine run: %v", err),
		})
	}

	return writeResponse(out, apiResponse{
		Success: true,
		Data:    records,
	})
}

func writeResponse(out io.Writer, resp apiResponse) error {
	return json.NewEncoder(out).Encode(resp)
}
