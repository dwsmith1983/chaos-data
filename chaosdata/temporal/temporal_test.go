package temporal_test

import (
	"encoding/json"
	"testing"

	"github.com/dwsmith1983/chaos-data/chaosdata"
	"github.com/dwsmith1983/chaos-data/chaosdata/temporal"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func requireNoError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func decodePayload(t *testing.T, p chaosdata.Payload) []map[string]interface{} {
	t.Helper()
	var records []map[string]interface{}
	if err := json.Unmarshal(p.Data, &records); err != nil {
		t.Fatalf("failed to unmarshal payload: %v", err)
	}
	return records
}

// ---------------------------------------------------------------------------
// Registration
// ---------------------------------------------------------------------------

func TestRegistration(t *testing.T) {
	all := chaosdata.All()
	found := make(map[string]bool)
	for _, g := range all {
		if g.Category() == "temporal" {
			found[g.Name()] = true
		}
	}

	expected := []string{
		"edge-case-timestamps",
		"temporal-ordering-anomalies",
		"mixed-format-timestamps",
	}

	for _, name := range expected {
		if !found[name] {
			t.Errorf("Generator %q not found in registry", name)
		}
	}
}

// ---------------------------------------------------------------------------
// EdgeCaseTimestamps
// ---------------------------------------------------------------------------

func TestEdgeCaseTimestamps_Name(t *testing.T) {
	g := temporal.EdgeCaseTimestamps{}
	if got := g.Name(); got != "edge-case-timestamps" {
		t.Errorf("Name() = %q, want %q", got, "edge-case-timestamps")
	}
}

func TestEdgeCaseTimestamps_Count(t *testing.T) {
	g := temporal.EdgeCaseTimestamps{}
	p, err := g.Generate(chaosdata.GenerateOpts{Count: 2})
	requireNoError(t, err)

	records := decodePayload(t, p)
	// edgeCasePool has 15 items. Count 2 means 30 items.
	if len(records) != 30 {
		t.Errorf("Expected 30 records, got %d", len(records))
	}
}

// ---------------------------------------------------------------------------
// TemporalOrderingAnomalies
// ---------------------------------------------------------------------------

func TestTemporalOrderingAnomalies_Name(t *testing.T) {
	g := temporal.TemporalOrderingAnomalies{}
	if got := g.Name(); got != "temporal-ordering-anomalies" {
		t.Errorf("Name() = %q, want %q", got, "temporal-ordering-anomalies")
	}
}

func TestTemporalOrderingAnomalies_Count(t *testing.T) {
	g := temporal.TemporalOrderingAnomalies{}
	p, err := g.Generate(chaosdata.GenerateOpts{Count: 5})
	requireNoError(t, err)

	records := decodePayload(t, p)
	if len(records) != 5 {
		t.Errorf("Expected 5 records, got %d", len(records))
	}
}

// ---------------------------------------------------------------------------
// MixedFormatTimestamps
// ---------------------------------------------------------------------------

func TestMixedFormatTimestamps_Name(t *testing.T) {
	g := temporal.MixedFormatTimestamps{}
	if got := g.Name(); got != "mixed-format-timestamps" {
		t.Errorf("Name() = %q, want %q", got, "mixed-format-timestamps")
	}
}

func TestMixedFormatTimestamps_Count(t *testing.T) {
	g := temporal.MixedFormatTimestamps{}
	p, err := g.Generate(chaosdata.GenerateOpts{Count: 1})
	requireNoError(t, err)

	records := decodePayload(t, p)
	// mixedFormatPool has 8 items.
	if len(records) != 8 {
		t.Errorf("Expected 8 records, got %d", len(records))
	}
}

// ---------------------------------------------------------------------------
// Interface compliance (compile-time)
// ---------------------------------------------------------------------------

var _ chaosdata.ChaosGenerator = temporal.EdgeCaseTimestamps{}
var _ chaosdata.ChaosGenerator = temporal.TemporalOrderingAnomalies{}
var _ chaosdata.ChaosGenerator = temporal.MixedFormatTimestamps{}
