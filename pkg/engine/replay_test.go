package engine_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/dwsmith1983/chaos-data/pkg/engine"
	"github.com/dwsmith1983/chaos-data/pkg/mutation"
	"github.com/dwsmith1983/chaos-data/pkg/scenario"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// --- Step 10.2: Replay tests ---

func TestReplayFromManifest_AppliesMutationsInOrder(t *testing.T) {
	t.Parallel()

	transport := &mockTransport{
		listFn: func(_ context.Context, _ string) ([]types.DataObject, error) {
			return []types.DataObject{
				newTestObject("a.csv"),
				newTestObject("b.csv"),
			}, nil
		},
	}

	reg := mutation.NewRegistry()
	if err := reg.Register(&mutation.DelayMutation{}); err != nil {
		t.Fatalf("register: %v", err)
	}

	scenarios := []scenario.Scenario{newDelayScenario("test-delay", types.SeverityLow)}

	eng := engine.New(
		defaultConfig(),
		transport,
		reg,
		scenarios,
	)

	// Build a JSONL manifest with two events.
	events := []types.ChaosEvent{
		{
			ID:        "evt-1",
			Scenario:  "test-delay",
			Category:  "data-arrival",
			Severity:  types.SeverityLow,
			Target:    "a.csv",
			Mutation:  "delay",
			Params:    map[string]string{"duration": "10m", "release": "true"},
			Timestamp: time.Now(),
			Mode:      "deterministic",
		},
		{
			ID:        "evt-2",
			Scenario:  "test-delay",
			Category:  "data-arrival",
			Severity:  types.SeverityLow,
			Target:    "b.csv",
			Mutation:  "delay",
			Params:    map[string]string{"duration": "10m", "release": "true"},
			Timestamp: time.Now(),
			Mode:      "deterministic",
		},
	}

	var lines []string
	for _, ev := range events {
		data, err := json.Marshal(ev)
		if err != nil {
			t.Fatalf("marshal event: %v", err)
		}
		lines = append(lines, string(data))
	}
	manifest := []byte(strings.Join(lines, "\n"))

	records, err := eng.ReplayFromManifest(context.Background(), manifest)
	if err != nil {
		t.Fatalf("ReplayFromManifest() error = %v", err)
	}

	if len(records) != 2 {
		t.Fatalf("ReplayFromManifest() returned %d records, want 2", len(records))
	}

	// Verify order is preserved.
	if records[0].ObjectKey != "a.csv" {
		t.Errorf("records[0].ObjectKey = %q, want %q", records[0].ObjectKey, "a.csv")
	}
	if records[1].ObjectKey != "b.csv" {
		t.Errorf("records[1].ObjectKey = %q, want %q", records[1].ObjectKey, "b.csv")
	}

	// All should be applied.
	for i, rec := range records {
		if !rec.Applied {
			t.Errorf("records[%d].Applied = false, want true", i)
		}
		if rec.Mutation != "delay" {
			t.Errorf("records[%d].Mutation = %q, want %q", i, rec.Mutation, "delay")
		}
	}
}

func TestReplayFromManifest_EmptyManifestReturnsEmptyRecords(t *testing.T) {
	t.Parallel()

	transport := &mockTransport{}
	reg := mutation.NewRegistry()
	scenarios := []scenario.Scenario{newDelayScenario("test-delay", types.SeverityLow)}

	eng := engine.New(
		defaultConfig(),
		transport,
		reg,
		scenarios,
	)

	records, err := eng.ReplayFromManifest(context.Background(), []byte(""))
	if err != nil {
		t.Fatalf("ReplayFromManifest() error = %v", err)
	}
	if len(records) != 0 {
		t.Errorf("ReplayFromManifest() returned %d records, want 0", len(records))
	}
}

func TestReplayFromManifest_UnknownMutationReturnsError(t *testing.T) {
	t.Parallel()

	transport := &mockTransport{}

	// Empty registry -- no mutations registered.
	reg := mutation.NewRegistry()
	scenarios := []scenario.Scenario{newDelayScenario("test-delay", types.SeverityLow)}

	eng := engine.New(
		defaultConfig(),
		transport,
		reg,
		scenarios,
	)

	event := types.ChaosEvent{
		ID:        "evt-1",
		Scenario:  "test-delay",
		Category:  "data-arrival",
		Severity:  types.SeverityLow,
		Target:    "a.csv",
		Mutation:  "unknown-mutation",
		Params:    map[string]string{},
		Timestamp: time.Now(),
		Mode:      "deterministic",
	}

	data, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}

	_, err = eng.ReplayFromManifest(context.Background(), data)
	if err == nil {
		t.Fatal("ReplayFromManifest() error = nil, want error for unknown mutation type")
	}
}
