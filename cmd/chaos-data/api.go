package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"time"

	"github.com/dwsmith1983/chaos-data/adapters/interlock"
	"github.com/dwsmith1983/chaos-data/adapters/local"
	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/config"
	"github.com/dwsmith1983/chaos-data/pkg/engine"
	"github.com/dwsmith1983/chaos-data/pkg/scenario"
	"github.com/dwsmith1983/chaos-data/pkg/types"
	"github.com/spf13/cobra"
)

// heldObjectEntry is the JSON representation of a held object in API responses.
type heldObjectEntry struct {
	Key          string `json:"key"`
	Size         int64  `json:"size"`
	LastModified string `json:"last_modified"`
}

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
			var asserter adapter.Asserter
			configPath, _ := cmd.Flags().GetString("config")
			if configPath != "" {
				fileCfg, err := config.Load(configPath)
				if err != nil {
					return err
				}
				if err := fileCfg.Validate(); err != nil {
					return err
				}
				var buildErr error
				asserter, buildErr = fileCfg.BuildAsserter()
				if buildErr != nil {
					return buildErr
				}
			}
			return runAPI(cmd.InOrStdin(), cmd.OutOrStdout(), asserter)
		},
	}
}

func runAPI(in io.Reader, out io.Writer, asserter adapter.Asserter) error {
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
	case "inject":
		return handleInjectAPI(out, req.Params, asserter)
	case "release":
		return handleReleaseAPI(out, req.Params)
	case "run":
		return handleRunAPI(out, req.Params, asserter)
	case "status":
		return handleStatusAPI(out, req.Params)
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

func handleRunAPI(out io.Writer, params map[string]string, asserter adapter.Asserter) error {
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

	opts := []engine.EngineOption{engine.WithSafety(safety)}
	if asserter != nil {
		opts = append(opts, engine.WithAsserter(asserter))
	}

	eng := engine.New(
		types.Defaults(),
		transport,
		registry,
		[]scenario.Scenario{sc},
		opts...,
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

func handleStatusAPI(out io.Writer, params map[string]string) error {
	inputDir := params["input"]
	outputDir := params["output"]

	if inputDir == "" || outputDir == "" {
		return writeResponse(out, apiResponse{
			Success: false,
			Error:   "missing required params: input, output",
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

	transport := local.NewFSTransport(inputDir, outputDir)
	ctx := context.Background()

	held, err := transport.ListHeld(ctx)
	if err != nil {
		return writeResponse(out, apiResponse{
			Success: false,
			Error:   fmt.Sprintf("list held: %v", err),
		})
	}

	entries := make([]heldObjectEntry, 0, len(held))
	for _, obj := range held {
		entries = append(entries, heldObjectEntry{
			Key:          obj.Key,
			Size:         obj.Size,
			LastModified: obj.LastModified.Format(time.RFC3339),
		})
	}

	return writeResponse(out, apiResponse{
		Success: true,
		Data:    entries,
	})
}

func handleReleaseAPI(out io.Writer, params map[string]string) error {
	inputDir := params["input"]
	outputDir := params["output"]
	key := params["key"]

	if inputDir == "" || outputDir == "" {
		return writeResponse(out, apiResponse{
			Success: false,
			Error:   "missing required params: input, output",
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

	transport := local.NewFSTransport(inputDir, outputDir)
	ctx := context.Background()

	if key != "" {
		if err := transport.Release(ctx, key); err != nil {
			return writeResponse(out, apiResponse{
				Success: false,
				Error:   fmt.Sprintf("release %q: %v", key, err),
			})
		}
		return writeResponse(out, apiResponse{
			Success: true,
			Data:    map[string]string{"released": key},
		})
	}

	if err := transport.ReleaseAll(ctx); err != nil {
		return writeResponse(out, apiResponse{
			Success: false,
			Error:   fmt.Sprintf("release all: %v", err),
		})
	}

	return writeResponse(out, apiResponse{
		Success: true,
		Data:    map[string]string{"released": "all"},
	})
}

func handleInjectAPI(out io.Writer, params map[string]string, asserter adapter.Asserter) error {
	scenarioName := params["scenario"]
	inputDir := params["input"]
	outputDir := params["output"]
	stateDB := params["state_db"]

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

	if stateDB == "" {
		stateDB = ":memory:"
	}

	sc, err := loadScenario(scenarioName)
	if err != nil {
		return writeResponse(out, apiResponse{
			Success: false,
			Error:   fmt.Sprintf("load scenario: %v", err),
		})
	}

	stateStore, err := local.NewSQLiteState(stateDB)
	if err != nil {
		return writeResponse(out, apiResponse{
			Success: false,
			Error:   fmt.Sprintf("open state store: %v", err),
		})
	}
	defer stateStore.Close()

	transport := local.NewFSTransport(inputDir, outputDir)
	registry := fullStatefulRegistry(stateStore)

	cfg := types.EngineConfig{
		Mode: "deterministic",
		Safety: types.SafetyConfig{
			MaxSeverity:    types.SeverityCritical,
			MaxAffectedPct: 100,
			MaxPipelines:   100,
		},
	}

	var opts []engine.EngineOption
	if asserter != nil {
		opts = append(opts, engine.WithAsserter(asserter))
	} else {
		reader := local.NewNoopEventReader()
		interlockAsserter := interlock.NewAdapterAsserter(stateStore, reader)
		opts = append(opts, engine.WithAsserter(interlockAsserter))
	}

	eng := engine.New(
		cfg,
		transport,
		registry,
		[]scenario.Scenario{sc},
		opts...,
	)

	ctx := context.Background()
	obj := types.DataObject{Key: "inject"}
	records, err := eng.ProcessObject(ctx, obj)
	if err != nil {
		return writeResponse(out, apiResponse{
			Success: false,
			Error:   fmt.Sprintf("inject: %v", err),
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
