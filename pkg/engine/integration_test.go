package engine_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/dwsmith1983/chaos-data/adapters/local"
	"github.com/dwsmith1983/chaos-data/pkg/engine"
	"github.com/dwsmith1983/chaos-data/pkg/mutation"
	"github.com/dwsmith1983/chaos-data/pkg/scenario"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

func TestIntegration_EngineRunWithLocalTransport(t *testing.T) {
	t.Parallel()

	// 1. Create temp directories.
	stagingDir := t.TempDir()
	outputDir := t.TempDir()

	// 2. Write test input files (JSONL).
	files := map[string]string{
		"events-001.jsonl": `{"user":"alice","action":"login","ts":"2024-01-01T00:00:00Z"}
{"user":"bob","action":"purchase","ts":"2024-01-01T01:00:00Z"}
`,
		"events-002.jsonl": `{"user":"carol","action":"logout","ts":"2024-01-01T02:00:00Z"}
`,
	}
	for name, content := range files {
		fpath := filepath.Join(stagingDir, name)
		if err := os.WriteFile(fpath, []byte(content), 0o644); err != nil {
			t.Fatalf("setup: write %s: %v", name, err)
		}
	}

	// 3. Create FSTransport.
	transport := local.NewFSTransport(stagingDir, outputDir)

	// 4. Create mutation registry with DelayMutation.
	reg := mutation.NewRegistry()
	if err := reg.Register(&mutation.DelayMutation{}); err != nil {
		t.Fatalf("register delay: %v", err)
	}

	// 5. Load the "late-data" scenario from the built-in catalog.
	lateData, err := scenario.Get("late-data")
	if err != nil {
		t.Fatalf("scenario.Get(late-data): %v", err)
	}

	// Force probability to 1.0 so it always matches (test determinism).
	lateData.Probability = 1.0

	// 6. Create emitter to track events.
	emitter := &mockEmitter{}

	// 7. Create engine with local transport and scenarios.
	cfg := types.EngineConfig{
		Mode: "deterministic",
		Safety: types.SafetyConfig{
			MaxSeverity:    types.SeverityCritical,
			MaxAffectedPct: 100,
			MaxPipelines:   10,
		},
	}

	eng := engine.New(
		cfg,
		transport,
		reg,
		[]scenario.Scenario{lateData},
		engine.WithEmitter(emitter),
	)

	// 8. Call engine.Run().
	records, err := eng.Run(context.Background())
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	// 9. Verify: mutation records returned.
	if len(records) != 2 {
		t.Fatalf("Run() returned %d records, want 2 (one per file)", len(records))
	}
	recordKeys := map[string]bool{}
	for _, rec := range records {
		recordKeys[rec.ObjectKey] = true
		if rec.Mutation != "delay" {
			t.Errorf("record mutation = %q, want %q", rec.Mutation, "delay")
		}
		if !rec.Applied {
			t.Errorf("record for %q not applied", rec.ObjectKey)
		}
	}
	for name := range files {
		if !recordKeys[name] {
			t.Errorf("missing mutation record for file %q", name)
		}
	}

	// 10. Verify: events emitted.
	events := emitter.getEvents()
	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}
	for _, ev := range events {
		if ev.Scenario != "late-data" {
			t.Errorf("event scenario = %q, want %q", ev.Scenario, "late-data")
		}
		if ev.Mutation != "delay" {
			t.Errorf("event mutation = %q, want %q", ev.Mutation, "delay")
		}
	}

	// 11. Verify: files moved to .chaos-hold/ directory.
	holdDir := filepath.Join(stagingDir, ".chaos-hold")
	for name := range files {
		heldPath := filepath.Join(holdDir, name)
		if _, err := os.Stat(heldPath); os.IsNotExist(err) {
			t.Errorf("expected file %q to exist in hold directory %q", name, holdDir)
		}
		// Meta sidecar should also exist.
		metaPath := filepath.Join(holdDir, name+".meta")
		if _, err := os.Stat(metaPath); os.IsNotExist(err) {
			t.Errorf("expected meta file %q to exist in hold directory", name+".meta")
		}
	}

	// Files should be gone from staging.
	for name := range files {
		stagingPath := filepath.Join(stagingDir, name)
		if _, err := os.Stat(stagingPath); !os.IsNotExist(err) {
			t.Errorf("expected file %q to be removed from staging, but it still exists", name)
		}
	}
}
