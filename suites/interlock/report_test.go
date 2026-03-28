package interlocksuite

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestFormatTable(t *testing.T) {
	matrix := CoverageMatrix{
		Results: []CapabilityResult{
			{Category: "validation", Capability: Capability{ID: "equals"}, Status: StatusCovered, Passed: 2, Failed: 0, Duration: 300 * time.Millisecond},
			{Category: "validation", Capability: Capability{ID: "age_lt"}, Status: StatusGap, Passed: 0, Failed: 1, Duration: 800 * time.Millisecond},
			{Category: "trigger_types", Capability: Capability{ID: "lambda"}, Status: StatusUntested},
		},
		Total: 3, Covered: 1, Gaps: 1, Untested: 1,
	}
	var buf bytes.Buffer
	if err := FormatTable(matrix, &buf); err != nil {
		t.Fatalf("FormatTable: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "COVERED") {
		t.Error("missing COVERED")
	}
	if !strings.Contains(out, "GAP") {
		t.Error("missing GAP")
	}
	if !strings.Contains(out, "UNTESTED") {
		t.Error("missing UNTESTED")
	}
	if !strings.Contains(out, "Summary") {
		t.Error("missing Summary line")
	}
	if !strings.Contains(out, "1/3 COVERED") {
		t.Error("missing coverage count")
	}
}

func TestFormatTable_EmptyMatrix(t *testing.T) {
	matrix := CoverageMatrix{}
	var buf bytes.Buffer
	if err := FormatTable(matrix, &buf); err != nil {
		t.Fatalf("FormatTable: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "Summary") {
		t.Error("missing Summary line for empty matrix")
	}
	if !strings.Contains(out, "0/0 COVERED") {
		t.Error("missing 0/0 coverage count")
	}
}

func TestFormatTable_DurationFormatting(t *testing.T) {
	matrix := CoverageMatrix{
		Results: []CapabilityResult{
			{Category: "cat", Capability: Capability{ID: "fast"}, Status: StatusCovered, Passed: 1, Duration: 50 * time.Millisecond},
			{Category: "cat", Capability: Capability{ID: "slow"}, Status: StatusCovered, Passed: 1, Duration: 2500 * time.Millisecond},
		},
		Total: 2, Covered: 2,
	}
	var buf bytes.Buffer
	if err := FormatTable(matrix, &buf); err != nil {
		t.Fatalf("FormatTable: %v", err)
	}
	out := buf.String()
	// Duration should appear as seconds with one decimal
	if !strings.Contains(out, "0.1s") && !strings.Contains(out, "0.0s") {
		t.Errorf("expected short duration formatting, got: %s", out)
	}
	if !strings.Contains(out, "2.5s") {
		t.Errorf("expected slow duration formatting, got: %s", out)
	}
}

func TestFormatTable_UntestedShowsDash(t *testing.T) {
	matrix := CoverageMatrix{
		Results: []CapabilityResult{
			{Category: "cat", Capability: Capability{ID: "none"}, Status: StatusUntested},
		},
		Total: 1, Untested: 1,
	}
	var buf bytes.Buffer
	if err := FormatTable(matrix, &buf); err != nil {
		t.Fatalf("FormatTable: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "-") {
		t.Error("untested row should contain dashes")
	}
}

func TestFormatJSON(t *testing.T) {
	matrix := CoverageMatrix{
		Results: []CapabilityResult{
			{Category: "validation", Capability: Capability{ID: "equals"}, Status: StatusCovered, Passed: 1},
		},
		Total: 1, Covered: 1,
	}
	var buf bytes.Buffer
	if err := FormatJSON(matrix, &buf); err != nil {
		t.Fatalf("FormatJSON: %v", err)
	}
	// Should be valid JSON
	var decoded map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &decoded); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
}

func TestFormatJSON_RoundTrip(t *testing.T) {
	matrix := CoverageMatrix{
		Results: []CapabilityResult{
			{Category: "validation", Capability: Capability{ID: "equals"}, Status: StatusCovered, Passed: 2, Failed: 1, Duration: 500 * time.Millisecond},
			{Category: "trigger_types", Capability: Capability{ID: "lambda"}, Status: StatusUntested},
		},
		Total: 2, Covered: 1, Gaps: 0, Untested: 1,
	}
	var buf bytes.Buffer
	if err := FormatJSON(matrix, &buf); err != nil {
		t.Fatalf("FormatJSON: %v", err)
	}

	var decoded CoverageMatrix
	if err := json.Unmarshal(buf.Bytes(), &decoded); err != nil {
		t.Fatalf("invalid JSON round-trip: %v", err)
	}
	if decoded.Total != 2 {
		t.Errorf("Total: got %d, want 2", decoded.Total)
	}
	if decoded.Covered != 1 {
		t.Errorf("Covered: got %d, want 1", decoded.Covered)
	}
	if len(decoded.Results) != 2 {
		t.Errorf("Results length: got %d, want 2", len(decoded.Results))
	}
}

func TestFormatJSON_EmptyMatrix(t *testing.T) {
	matrix := CoverageMatrix{}
	var buf bytes.Buffer
	if err := FormatJSON(matrix, &buf); err != nil {
		t.Fatalf("FormatJSON: %v", err)
	}
	var decoded map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &decoded); err != nil {
		t.Fatalf("invalid JSON for empty matrix: %v", err)
	}
}

func TestFormatMarkdown(t *testing.T) {
	matrix := CoverageMatrix{
		Results: []CapabilityResult{
			{Category: "validation", Capability: Capability{ID: "equals"}, Status: StatusCovered, Passed: 1},
		},
		Total: 1, Covered: 1,
	}
	var buf bytes.Buffer
	if err := FormatMarkdown(matrix, &buf); err != nil {
		t.Fatalf("FormatMarkdown: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "| Category") {
		t.Error("missing header")
	}
	if !strings.Contains(out, "| ---") {
		t.Error("missing separator")
	}
	if !strings.Contains(out, "COVERED") {
		t.Error("missing status")
	}
}

func TestFormatMarkdown_AllStatuses(t *testing.T) {
	matrix := CoverageMatrix{
		Results: []CapabilityResult{
			{Category: "a", Capability: Capability{ID: "covered"}, Status: StatusCovered, Passed: 3, Failed: 0, Duration: 100 * time.Millisecond},
			{Category: "b", Capability: Capability{ID: "gap"}, Status: StatusGap, Passed: 1, Failed: 2, Duration: 500 * time.Millisecond},
			{Category: "c", Capability: Capability{ID: "untested"}, Status: StatusUntested},
		},
		Total: 3, Covered: 1, Gaps: 1, Untested: 1,
	}
	var buf bytes.Buffer
	if err := FormatMarkdown(matrix, &buf); err != nil {
		t.Fatalf("FormatMarkdown: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "COVERED") {
		t.Error("missing COVERED status")
	}
	if !strings.Contains(out, "GAP") {
		t.Error("missing GAP status")
	}
	if !strings.Contains(out, "UNTESTED") {
		t.Error("missing UNTESTED status")
	}
	if !strings.Contains(out, "1/3 COVERED") {
		t.Error("missing summary line")
	}
}

func TestFormatMarkdown_EmptyMatrix(t *testing.T) {
	matrix := CoverageMatrix{}
	var buf bytes.Buffer
	if err := FormatMarkdown(matrix, &buf); err != nil {
		t.Fatalf("FormatMarkdown: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "| Category") {
		t.Error("missing header for empty matrix")
	}
}
