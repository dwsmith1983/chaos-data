package interlocksuite

import (
	"testing"
	"time"
)

func TestNewCoverageTracker(t *testing.T) {
	t.Parallel()
	ct, err := NewCoverageTracker("coverage.yaml")
	if err != nil {
		t.Fatalf("NewCoverageTracker: %v", err)
	}
	matrix := ct.Matrix()
	if matrix.Total != 53 {
		t.Errorf("total capabilities = %d, want 53", matrix.Total)
	}
	if matrix.Untested != 53 {
		t.Errorf("untested = %d, want 53 (nothing recorded yet)", matrix.Untested)
	}
}

func TestCoverageTracker_Record_Covered(t *testing.T) {
	t.Parallel()
	ct, err := NewCoverageTracker("coverage.yaml")
	if err != nil {
		t.Fatal(err)
	}

	ct.Record("validation/equals", true, 100*time.Millisecond)
	matrix := ct.Matrix()

	found := false
	for _, r := range matrix.Results {
		if r.Category == "validation" && r.Capability.ID == "equals" {
			found = true
			if r.Status != StatusCovered {
				t.Errorf("status = %q, want %q", r.Status, StatusCovered)
			}
			if r.Passed != 1 {
				t.Errorf("passed = %d, want 1", r.Passed)
			}
		}
	}
	if !found {
		t.Error("validation/equals not found in results")
	}
	if matrix.Covered != 1 {
		t.Errorf("covered = %d, want 1", matrix.Covered)
	}
}

func TestCoverageTracker_Record_Gap(t *testing.T) {
	t.Parallel()
	ct, err := NewCoverageTracker("coverage.yaml")
	if err != nil {
		t.Fatal(err)
	}

	ct.Record("validation/equals", false, 50*time.Millisecond)
	matrix := ct.Matrix()

	for _, r := range matrix.Results {
		if r.Category == "validation" && r.Capability.ID == "equals" {
			if r.Status != StatusGap {
				t.Errorf("status = %q, want %q", r.Status, StatusGap)
			}
			if r.Failed != 1 {
				t.Errorf("failed = %d, want 1", r.Failed)
			}
		}
	}
	if matrix.Gaps != 1 {
		t.Errorf("gaps = %d, want 1", matrix.Gaps)
	}
}

func TestCoverageTracker_Record_UnknownCapability(t *testing.T) {
	t.Parallel()
	ct, err := NewCoverageTracker("coverage.yaml")
	if err != nil {
		t.Fatal(err)
	}

	// Recording an unknown capability should not panic or error.
	ct.Record("nonexistent/capability", true, 10*time.Millisecond)
	matrix := ct.Matrix()
	// Total should still be 53 (unknown is ignored).
	if matrix.Total != 53 {
		t.Errorf("total = %d, want 53", matrix.Total)
	}
}

func TestCoverageTracker_Record_MixedPassFail(t *testing.T) {
	t.Parallel()
	ct, err := NewCoverageTracker("coverage.yaml")
	if err != nil {
		t.Fatal(err)
	}

	ct.Record("validation/equals", true, 100*time.Millisecond)
	ct.Record("validation/equals", false, 50*time.Millisecond)
	matrix := ct.Matrix()

	for _, r := range matrix.Results {
		if r.Category == "validation" && r.Capability.ID == "equals" {
			if r.Status != StatusGap {
				t.Errorf("status = %q, want %q (mixed pass+fail should be GAP)", r.Status, StatusGap)
			}
			if r.Passed != 1 {
				t.Errorf("passed = %d, want 1", r.Passed)
			}
			if r.Failed != 1 {
				t.Errorf("failed = %d, want 1", r.Failed)
			}
			if r.Duration != 150*time.Millisecond {
				t.Errorf("duration = %v, want 150ms (accumulated)", r.Duration)
			}
		}
	}
	if matrix.Gaps != 1 {
		t.Errorf("gaps = %d, want 1", matrix.Gaps)
	}
}

func TestCoverageTracker_Matrix_Counts(t *testing.T) {
	t.Parallel()
	ct, err := NewCoverageTracker("coverage.yaml")
	if err != nil {
		t.Fatal(err)
	}

	ct.Record("validation/equals", true, 10*time.Millisecond)
	ct.Record("trigger_types/http", false, 20*time.Millisecond)
	matrix := ct.Matrix()

	if matrix.Total != 53 {
		t.Errorf("total = %d, want 53", matrix.Total)
	}
	if matrix.Covered != 1 {
		t.Errorf("covered = %d, want 1", matrix.Covered)
	}
	if matrix.Gaps != 1 {
		t.Errorf("gaps = %d, want 1", matrix.Gaps)
	}
	if matrix.Untested != 51 {
		t.Errorf("untested = %d, want 51", matrix.Untested)
	}
}

func TestNewCoverageTracker_BadPath(t *testing.T) {
	t.Parallel()
	_, err := NewCoverageTracker("nonexistent.yaml")
	if err == nil {
		t.Error("expected error for nonexistent file, got nil")
	}
}
