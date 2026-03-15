package local_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/dwsmith1983/chaos-data/adapters/local"
)

func TestStaticResolver_GetDownstream_MatchesPrefix(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	cfg := filepath.Join(dir, "deps.yaml")
	if err := os.WriteFile(cfg, []byte(`dependencies:
  - prefix: "events-"
    downstream: ["analytics.user_events", "reporting.daily_summary"]
  - prefix: "transactions-"
    downstream: ["billing.invoices"]
`), 0600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	r, err := local.NewStaticResolver(cfg)
	if err != nil {
		t.Fatalf("NewStaticResolver() error = %v", err)
	}

	got, err := r.GetDownstream(context.Background(), "events-001.jsonl")
	if err != nil {
		t.Fatalf("GetDownstream() error = %v", err)
	}

	want := []string{"analytics.user_events", "reporting.daily_summary"}
	if len(got) != len(want) {
		t.Fatalf("GetDownstream() len = %d, want %d; got %v", len(got), len(want), got)
	}
	for i, w := range want {
		if got[i] != w {
			t.Errorf("GetDownstream()[%d] = %q, want %q", i, got[i], w)
		}
	}
}

func TestStaticResolver_GetDownstream_NoMatch(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	cfg := filepath.Join(dir, "deps.yaml")
	if err := os.WriteFile(cfg, []byte(`dependencies:
  - prefix: "events-"
    downstream: ["analytics.user_events"]
`), 0600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	r, err := local.NewStaticResolver(cfg)
	if err != nil {
		t.Fatalf("NewStaticResolver() error = %v", err)
	}

	got, err := r.GetDownstream(context.Background(), "logs-001.jsonl")
	if err != nil {
		t.Fatalf("GetDownstream() error = %v", err)
	}
	if got != nil {
		t.Errorf("GetDownstream() = %v, want nil for no-match target", got)
	}
}

func TestStaticResolver_GetDownstream_MultipleMatches(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	cfg := filepath.Join(dir, "deps.yaml")
	// "events-" and "ev" both match "events-daily.jsonl"
	if err := os.WriteFile(cfg, []byte(`dependencies:
  - prefix: "events-"
    downstream: ["analytics.user_events"]
  - prefix: "ev"
    downstream: ["monitoring.all_events"]
`), 0600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	r, err := local.NewStaticResolver(cfg)
	if err != nil {
		t.Fatalf("NewStaticResolver() error = %v", err)
	}

	got, err := r.GetDownstream(context.Background(), "events-daily.jsonl")
	if err != nil {
		t.Fatalf("GetDownstream() error = %v", err)
	}

	// Must be the union of both matching prefixes.
	if len(got) != 2 {
		t.Fatalf("GetDownstream() len = %d, want 2; got %v", len(got), got)
	}
	gotSet := make(map[string]struct{}, len(got))
	for _, g := range got {
		gotSet[g] = struct{}{}
	}
	for _, want := range []string{"analytics.user_events", "monitoring.all_events"} {
		if _, ok := gotSet[want]; !ok {
			t.Errorf("GetDownstream() missing %q; got %v", want, got)
		}
	}
}

func TestStaticResolver_GetDownstream_EmptyConfig(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	cfg := filepath.Join(dir, "deps.yaml")
	if err := os.WriteFile(cfg, []byte("dependencies: []\n"), 0600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	r, err := local.NewStaticResolver(cfg)
	if err != nil {
		t.Fatalf("NewStaticResolver() error = %v", err)
	}

	got, err := r.GetDownstream(context.Background(), "events-001.jsonl")
	if err != nil {
		t.Fatalf("GetDownstream() error = %v", err)
	}
	if got != nil {
		t.Errorf("GetDownstream() = %v, want nil for empty config", got)
	}
}

func TestStaticResolver_NewStaticResolver_FileNotFound(t *testing.T) {
	t.Parallel()

	_, err := local.NewStaticResolver("/no/such/file/deps.yaml")
	if err == nil {
		t.Fatal("NewStaticResolver() error = nil, want error for missing file")
	}
}

func TestStaticResolver_NewStaticResolver_InvalidYAML(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	cfg := filepath.Join(dir, "bad.yaml")
	if err := os.WriteFile(cfg, []byte("\t\t: bad yaml\n"), 0600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	_, err := local.NewStaticResolver(cfg)
	if err == nil {
		t.Fatal("NewStaticResolver() error = nil, want error for invalid YAML")
	}
}

func TestStaticResolver_NewStaticResolver_EmptyPrefix(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	cfg := filepath.Join(dir, "deps.yaml")
	if err := os.WriteFile(cfg, []byte(`dependencies:
  - prefix: ""
    downstream: ["analytics.user_events"]
`), 0600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	_, err := local.NewStaticResolver(cfg)
	if err == nil {
		t.Fatal("NewStaticResolver() error = nil, want error for empty prefix")
	}
}

func TestStaticResolver_GetDownstream_NilContext(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	cfg := filepath.Join(dir, "deps.yaml")
	if err := os.WriteFile(cfg, []byte(`dependencies:
  - prefix: "events-"
    downstream: ["analytics.user_events"]
`), 0600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	r, err := local.NewStaticResolver(cfg)
	if err != nil {
		t.Fatalf("NewStaticResolver() error = %v", err)
	}

	// nil context: implementation must not panic — it ignores context.
	got, err := r.GetDownstream(nil, "events-001.jsonl") //nolint:staticcheck
	if err != nil {
		t.Fatalf("GetDownstream(nil, ...) error = %v", err)
	}
	if len(got) == 0 {
		t.Error("GetDownstream(nil, ...) returned empty, want at least 1 entry")
	}
}
