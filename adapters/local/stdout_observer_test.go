package local_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dwsmith1983/chaos-data/adapters/local"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

func newTestEvent(id, scenario string, severity types.Severity) types.ChaosEvent {
	return types.ChaosEvent{
		ID:           id,
		ExperimentID: "exp-1",
		Scenario:     scenario,
		Category:     "data",
		Severity:     severity,
		Target:       "test-target",
		Mutation:     "corrupt",
		Params:       map[string]string{"key": "value"},
		Timestamp:    time.Date(2026, 3, 14, 12, 0, 0, 0, time.UTC),
		Mode:         "deterministic",
	}
}

func TestStdoutEmitter_Emit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		event types.ChaosEvent
	}{
		{
			name:  "emit single event",
			event: newTestEvent("evt-1", "delay", types.SeverityLow),
		},
		{
			name:  "emit with critical severity",
			event: newTestEvent("evt-2", "corrupt", types.SeverityCritical),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var buf bytes.Buffer
			emitter := local.NewStdoutEmitter(&buf)

			if err := emitter.Emit(context.Background(), tt.event); err != nil {
				t.Fatalf("Emit() error = %v", err)
			}

			line := strings.TrimSpace(buf.String())
			if line == "" {
				t.Fatal("Emit() wrote empty output")
			}

			// Verify it's valid JSON that unmarshals back.
			var got types.ChaosEvent
			if err := json.Unmarshal([]byte(line), &got); err != nil {
				t.Fatalf("Emit() output is not valid JSON: %v\noutput: %s", err, line)
			}
			if got.ID != tt.event.ID {
				t.Errorf("unmarshaled ID = %q, want %q", got.ID, tt.event.ID)
			}
			if got.Scenario != tt.event.Scenario {
				t.Errorf("unmarshaled Scenario = %q, want %q", got.Scenario, tt.event.Scenario)
			}
			if got.Severity != tt.event.Severity {
				t.Errorf("unmarshaled Severity = %v, want %v", got.Severity, tt.event.Severity)
			}
		})
	}
}

func TestStdoutEmitter_MultipleEmits(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	emitter := local.NewStdoutEmitter(&buf)

	events := []types.ChaosEvent{
		newTestEvent("evt-a", "delay", types.SeverityLow),
		newTestEvent("evt-b", "corrupt", types.SeverityModerate),
		newTestEvent("evt-c", "drop", types.SeveritySevere),
	}

	for _, ev := range events {
		if err := emitter.Emit(context.Background(), ev); err != nil {
			t.Fatalf("Emit() error = %v", err)
		}
	}

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != len(events) {
		t.Fatalf("got %d lines, want %d", len(lines), len(events))
	}

	for i, line := range lines {
		var got types.ChaosEvent
		if err := json.Unmarshal([]byte(line), &got); err != nil {
			t.Fatalf("line %d: not valid JSON: %v", i, err)
		}
		if got.ID != events[i].ID {
			t.Errorf("line %d: ID = %q, want %q", i, got.ID, events[i].ID)
		}
	}
}

func TestStdoutEmitter_OutputEndsWithNewline(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	emitter := local.NewStdoutEmitter(&buf)

	if err := emitter.Emit(context.Background(), newTestEvent("evt-1", "delay", types.SeverityLow)); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}

	output := buf.String()
	if !strings.HasSuffix(output, "\n") {
		t.Error("Emit() output does not end with newline")
	}
}

func TestStdoutEmitter_ConcurrentEmit(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	emitter := local.NewStdoutEmitter(&buf)

	const goroutines = 20
	var wg sync.WaitGroup
	wg.Add(goroutines)

	errs := make(chan error, goroutines)

	for i := range goroutines {
		go func(idx int) {
			defer wg.Done()
			ev := newTestEvent(
				fmt.Sprintf("concurrent-%d", idx),
				"delay",
				types.SeverityLow,
			)
			if err := emitter.Emit(context.Background(), ev); err != nil {
				errs <- err
			}
		}(i)
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		t.Errorf("Emit() error during concurrent call: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
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
