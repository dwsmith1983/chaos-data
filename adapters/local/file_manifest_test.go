package local_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dwsmith1983/chaos-data/adapters/local"
	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// Compile-time assertion: FileManifestObserver satisfies EventEmitter.
var _ adapter.EventEmitter = (*local.FileManifestObserver)(nil)

func newManifestTestEvent(id, experimentID string) types.ChaosEvent {
	return types.ChaosEvent{
		ID:           id,
		ExperimentID: experimentID,
		Scenario:     "corrupt-parquet",
		Category:     "data-corruption",
		Severity:     types.SeverityModerate,
		Target:       "s3://staging/data.parquet",
		Mutation:     "bit-flip",
		Params:       map[string]string{"column": "revenue", "rate": "0.05"},
		Timestamp:    time.Date(2026, 3, 15, 10, 0, 0, 0, time.UTC),
		Mode:         "deterministic",
	}
}

func TestFileManifestObserver_Emit(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "manifest.jsonl")

	obs, err := local.NewFileManifestObserver(path)
	if err != nil {
		t.Fatalf("NewFileManifestObserver() error = %v", err)
	}
	defer obs.Close()

	evt := newManifestTestEvent("evt-001", "exp-001")
	if err := obs.Emit(context.Background(), evt); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}

	line := strings.TrimSpace(string(data))
	if line == "" {
		t.Fatal("Emit() wrote empty file")
	}

	var got types.ChaosEvent
	if err := json.Unmarshal([]byte(line), &got); err != nil {
		t.Fatalf("Emit() output is not valid JSON: %v\noutput: %s", err, line)
	}
	if got.ID != evt.ID {
		t.Errorf("ID = %q, want %q", got.ID, evt.ID)
	}
	if got.ExperimentID != evt.ExperimentID {
		t.Errorf("ExperimentID = %q, want %q", got.ExperimentID, evt.ExperimentID)
	}
	if got.Scenario != evt.Scenario {
		t.Errorf("Scenario = %q, want %q", got.Scenario, evt.Scenario)
	}
	if got.Severity != evt.Severity {
		t.Errorf("Severity = %v, want %v", got.Severity, evt.Severity)
	}
}

func TestFileManifestObserver_MultipleEmits(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "manifest.jsonl")

	obs, err := local.NewFileManifestObserver(path)
	if err != nil {
		t.Fatalf("NewFileManifestObserver() error = %v", err)
	}
	defer obs.Close()

	events := []types.ChaosEvent{
		newManifestTestEvent("evt-a", "exp-001"),
		newManifestTestEvent("evt-b", "exp-001"),
		newManifestTestEvent("evt-c", "exp-001"),
	}

	for _, ev := range events {
		if err := obs.Emit(context.Background(), ev); err != nil {
			t.Fatalf("Emit() error = %v", err)
		}
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}

	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != len(events) {
		t.Fatalf("got %d lines, want %d\ncontent:\n%s", len(lines), len(events), string(data))
	}

	for i, line := range lines {
		var got types.ChaosEvent
		if err := json.Unmarshal([]byte(line), &got); err != nil {
			t.Fatalf("line %d: not valid JSON: %v\nline: %s", i, err, line)
		}
		if got.ID != events[i].ID {
			t.Errorf("line %d: ID = %q, want %q", i, got.ID, events[i].ID)
		}
	}
}

func TestFileManifestObserver_Close(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "manifest.jsonl")

	obs, err := local.NewFileManifestObserver(path)
	if err != nil {
		t.Fatalf("NewFileManifestObserver() error = %v", err)
	}

	if err := obs.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// Emit after close must return an error.
	evt := newManifestTestEvent("evt-after-close", "exp-001")
	if err := obs.Emit(context.Background(), evt); err == nil {
		t.Fatal("Emit() after Close() returned nil, want error")
	}
}

func TestFileManifestObserver_AppendsToExisting(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "manifest.jsonl")

	// Pre-populate the file with existing content.
	existing := `{"id":"pre-existing","experiment_id":"exp-000"}` + "\n"
	if err := os.WriteFile(path, []byte(existing), 0644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	obs, err := local.NewFileManifestObserver(path)
	if err != nil {
		t.Fatalf("NewFileManifestObserver() error = %v", err)
	}
	defer obs.Close()

	evt := newManifestTestEvent("evt-appended", "exp-001")
	if err := obs.Emit(context.Background(), evt); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}

	content := string(data)

	// Original content must be preserved.
	if !strings.HasPrefix(content, existing) {
		t.Errorf("original content not preserved; file starts with:\n%s", content[:min(len(content), 120)])
	}

	// Appended line must be present.
	lines := strings.Split(strings.TrimSpace(content), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 lines, got %d\ncontent:\n%s", len(lines), content)
	}

	var got types.ChaosEvent
	if err := json.Unmarshal([]byte(lines[1]), &got); err != nil {
		t.Fatalf("appended line is not valid JSON: %v\nline: %s", err, lines[1])
	}
	if got.ID != evt.ID {
		t.Errorf("appended ID = %q, want %q", got.ID, evt.ID)
	}
}

func TestFileManifestObserver_ConcurrentEmit(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "manifest.jsonl")

	obs, err := local.NewFileManifestObserver(path)
	if err != nil {
		t.Fatalf("NewFileManifestObserver() error = %v", err)
	}
	defer obs.Close()

	const goroutines = 20
	var wg sync.WaitGroup
	wg.Add(goroutines)
	errs := make(chan error, goroutines)

	for i := range goroutines {
		go func(idx int) {
			defer wg.Done()
			ev := newManifestTestEvent(
				strings.Join([]string{"concurrent", string(rune('0'+idx))}, "-"),
				"exp-concurrent",
			)
			if err := obs.Emit(context.Background(), ev); err != nil {
				errs <- err
			}
		}(i)
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		t.Errorf("concurrent Emit() error: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}

	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != goroutines {
		t.Fatalf("got %d lines, want %d", len(lines), goroutines)
	}

	for i, line := range lines {
		var got types.ChaosEvent
		if err := json.Unmarshal([]byte(line), &got); err != nil {
			t.Errorf("line %d: not valid JSON: %v\nline: %s", i, err, line)
		}
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
