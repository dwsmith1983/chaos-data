package interlock_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/dwsmith1983/chaos-data/adapters/interlock"
	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

func TestAssertSensorStatus_SucceedsImmediately(t *testing.T) {
	t.Parallel()

	store := newMockStateStore()
	store.sensors["pipeline-a/sensor-1"] = adapter.SensorData{
		Pipeline: "pipeline-a",
		Key:      "sensor-1",
		Status:   types.SensorStatusReady,
	}

	reader := newMockEventReader()
	a := interlock.NewAsserter(store, reader, 10*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	err := a.AssertSensorStatus(ctx, "pipeline-a", "sensor-1", types.SensorStatusReady, 200*time.Millisecond)
	if err != nil {
		t.Fatalf("AssertSensorStatus() = %v, want nil", err)
	}
}

func TestAssertSensorStatus_SucceedsAfterRetry(t *testing.T) {
	t.Parallel()

	store := newMockStateStore()
	// First put the wrong status.
	store.sensors["pipeline-a/sensor-1"] = adapter.SensorData{
		Pipeline: "pipeline-a",
		Key:      "sensor-1",
		Status:   types.SensorStatusPending,
	}

	reader := newMockEventReader()
	a := interlock.NewAsserter(store, reader, 10*time.Millisecond)

	// Update the store status after a short delay.
	go func() {
		time.Sleep(30 * time.Millisecond)
		store.mu.Lock()
		store.sensors["pipeline-a/sensor-1"] = adapter.SensorData{
			Pipeline: "pipeline-a",
			Key:      "sensor-1",
			Status:   types.SensorStatusReady,
		}
		store.mu.Unlock()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	err := a.AssertSensorStatus(ctx, "pipeline-a", "sensor-1", types.SensorStatusReady, 500*time.Millisecond)
	if err != nil {
		t.Fatalf("AssertSensorStatus() = %v, want nil after retry", err)
	}
}

func TestAssertSensorStatus_TimesOut(t *testing.T) {
	t.Parallel()

	store := newMockStateStore()
	store.sensors["pipeline-a/sensor-1"] = adapter.SensorData{
		Pipeline: "pipeline-a",
		Key:      "sensor-1",
		Status:   types.SensorStatusPending,
	}

	reader := newMockEventReader()
	a := interlock.NewAsserter(store, reader, 10*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	err := a.AssertSensorStatus(ctx, "pipeline-a", "sensor-1", types.SensorStatusReady, 50*time.Millisecond)
	if err == nil {
		t.Fatal("AssertSensorStatus() = nil, want timeout error")
	}
	if !strings.Contains(err.Error(), "timed out") {
		t.Errorf("error = %q, want to contain %q", err.Error(), "timed out")
	}
}

func TestAssertTriggerStatus_Succeeds(t *testing.T) {
	t.Parallel()

	store := newMockStateStore()
	tKey := adapter.TriggerKey{Pipeline: "p", Schedule: "s", Date: "2026-03-14"}
	store.triggers["p/s/2026-03-14"] = "triggered"

	reader := newMockEventReader()
	a := interlock.NewAsserter(store, reader, 10*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	err := a.AssertTriggerStatus(ctx, tKey, "triggered", 200*time.Millisecond)
	if err != nil {
		t.Fatalf("AssertTriggerStatus() = %v, want nil", err)
	}
}

func TestAssertTriggerStatus_Fails(t *testing.T) {
	t.Parallel()

	store := newMockStateStore()
	tKey := adapter.TriggerKey{Pipeline: "p", Schedule: "s", Date: "2026-03-14"}
	store.triggers["p/s/2026-03-14"] = "pending"

	reader := newMockEventReader()
	a := interlock.NewAsserter(store, reader, 10*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	err := a.AssertTriggerStatus(ctx, tKey, "triggered", 50*time.Millisecond)
	if err == nil {
		t.Fatal("AssertTriggerStatus() = nil, want timeout error")
	}
}

func TestAssertEventEmitted_Succeeds(t *testing.T) {
	t.Parallel()

	store := newMockStateStore()
	reader := newMockEventReader()
	reader.events = []types.ChaosEvent{
		{Scenario: "test-scenario", Mutation: "stale-sensor"},
	}

	a := interlock.NewAsserter(store, reader, 10*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	err := a.AssertEventEmitted(ctx, "test-scenario", "stale-sensor", 200*time.Millisecond)
	if err != nil {
		t.Fatalf("AssertEventEmitted() = %v, want nil", err)
	}
}

func TestAssertEventEmitted_Fails(t *testing.T) {
	t.Parallel()

	store := newMockStateStore()
	reader := newMockEventReader()
	reader.events = []types.ChaosEvent{
		{Scenario: "other-scenario", Mutation: "other-mutation"},
	}

	a := interlock.NewAsserter(store, reader, 10*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	err := a.AssertEventEmitted(ctx, "test-scenario", "stale-sensor", 50*time.Millisecond)
	if err == nil {
		t.Fatal("AssertEventEmitted() = nil, want timeout error")
	}
}

func TestAssertSensorStatus_ContextCancellation(t *testing.T) {
	t.Parallel()

	store := newMockStateStore()
	store.sensors["pipeline-a/sensor-1"] = adapter.SensorData{
		Pipeline: "pipeline-a",
		Key:      "sensor-1",
		Status:   types.SensorStatusPending,
	}

	reader := newMockEventReader()
	a := interlock.NewAsserter(store, reader, 10*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately.

	err := a.AssertSensorStatus(ctx, "pipeline-a", "sensor-1", types.SensorStatusReady, 5*time.Second)
	if err == nil {
		t.Fatal("AssertSensorStatus() = nil, want context error")
	}
	if !strings.Contains(err.Error(), "context") {
		t.Errorf("error = %q, want to contain %q", err.Error(), "context")
	}
}
