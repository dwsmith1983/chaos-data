package interlocksuite_test

import (
	"flag"
	"os"
	"testing"

	interlocksuite "github.com/dwsmith1983/chaos-data/suites/interlock"
)

var target = flag.String("target", "local", "execution target: local or aws")

func TestMain(m *testing.M) {
	flag.Parse()
	os.Exit(m.Run())
}

func TestSuite_CoverageMatrix(t *testing.T) {
	t.Parallel()

	ct, err := interlocksuite.NewCoverageTracker("coverage.yaml")
	if err != nil {
		t.Fatalf("load coverage: %v", err)
	}

	matrix := ct.Matrix()
	if matrix.Total == 0 {
		t.Error("expected non-zero capabilities")
	}

	t.Logf("target=%s coverage: %d total, %d covered, %d gaps, %d untested",
		*target, matrix.Total, matrix.Covered, matrix.Gaps, matrix.Untested)
}

func TestSuite_GapsFilter(t *testing.T) {
	t.Parallel()

	ct, err := interlocksuite.NewCoverageTracker("coverage.yaml")
	if err != nil {
		t.Fatalf("load coverage: %v", err)
	}

	matrix := ct.Matrix()

	var gaps int
	for _, r := range matrix.Results {
		if r.Status != interlocksuite.StatusCovered {
			gaps++
		}
	}

	want := matrix.Gaps + matrix.Untested
	if gaps != want {
		t.Errorf("filtered gaps = %d, want %d (gaps=%d + untested=%d)",
			gaps, want, matrix.Gaps, matrix.Untested)
	}
}
