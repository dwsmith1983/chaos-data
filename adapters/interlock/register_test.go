package interlock_test

import (
	"errors"
	"testing"

	"github.com/dwsmith1983/chaos-data/adapters/interlock"
	"github.com/dwsmith1983/chaos-data/pkg/mutation"
)

func TestRegisterAll(t *testing.T) {
	t.Parallel()

	store := newMockStateStore()
	cfg := interlock.Config{
		SensorTableName:  "sensor-table",
		TriggerTableName: "trigger-table",
		PipelinePrefix:   "prod/",
		DefaultSchedule:  "daily",
	}
	cfg.Defaults()

	reg := mutation.NewRegistry()
	if err := interlock.RegisterAll(reg, store, cfg); err != nil {
		t.Fatalf("RegisterAll() = %v, want nil", err)
	}

	names := reg.List()
	if len(names) != 9 {
		t.Fatalf("RegisterAll() registered %d mutations, want 9: %v", len(names), names)
	}

	want := []string{
		"interlock-false-success",
		"interlock-job-kill",
		"interlock-phantom-sensor",
		"interlock-phantom-trigger",
		"interlock-sensor-flapping",
		"interlock-split-sensor",
		"interlock-stale-sensor",
		"interlock-timestamp-forgery",
		"interlock-trigger-timeout",
	}

	for i, name := range want {
		if names[i] != name {
			t.Errorf("names[%d] = %q, want %q", i, names[i], name)
		}
	}
}

func TestRegisterSensors(t *testing.T) {
	t.Parallel()

	store := newMockStateStore()
	cfg := interlock.Config{
		SensorTableName:  "sensor-table",
		TriggerTableName: "trigger-table",
		PipelinePrefix:   "prod/",
	}
	cfg.Defaults()

	reg := mutation.NewRegistry()
	if err := interlock.RegisterSensors(reg, store, cfg); err != nil {
		t.Fatalf("RegisterSensors() = %v, want nil", err)
	}

	names := reg.List()
	if len(names) != 5 {
		t.Fatalf("RegisterSensors() registered %d mutations, want 5: %v", len(names), names)
	}

	want := []string{
		"interlock-phantom-sensor",
		"interlock-sensor-flapping",
		"interlock-split-sensor",
		"interlock-stale-sensor",
		"interlock-timestamp-forgery",
	}

	for i, name := range want {
		if names[i] != name {
			t.Errorf("names[%d] = %q, want %q", i, names[i], name)
		}
	}
}

func TestRegisterTriggers(t *testing.T) {
	t.Parallel()

	store := newMockStateStore()
	cfg := interlock.Config{
		SensorTableName:  "sensor-table",
		TriggerTableName: "trigger-table",
		PipelinePrefix:   "prod/",
		DefaultSchedule:  "daily",
	}
	cfg.Defaults()

	reg := mutation.NewRegistry()
	if err := interlock.RegisterTriggers(reg, store, cfg); err != nil {
		t.Fatalf("RegisterTriggers() = %v, want nil", err)
	}

	names := reg.List()
	if len(names) != 4 {
		t.Fatalf("RegisterTriggers() registered %d mutations, want 4: %v", len(names), names)
	}

	want := []string{
		"interlock-false-success",
		"interlock-job-kill",
		"interlock-phantom-trigger",
		"interlock-trigger-timeout",
	}

	for i, name := range want {
		if names[i] != name {
			t.Errorf("names[%d] = %q, want %q", i, names[i], name)
		}
	}
}

func TestRegisterAll_DuplicateError(t *testing.T) {
	t.Parallel()

	store := newMockStateStore()
	cfg := interlock.Config{
		SensorTableName:  "sensor-table",
		TriggerTableName: "trigger-table",
		PipelinePrefix:   "prod/",
		DefaultSchedule:  "daily",
	}
	cfg.Defaults()

	reg := mutation.NewRegistry()
	if err := interlock.RegisterAll(reg, store, cfg); err != nil {
		t.Fatalf("first RegisterAll() = %v, want nil", err)
	}

	// Second registration should fail with duplicate error.
	err := interlock.RegisterAll(reg, store, cfg)
	if err == nil {
		t.Fatal("second RegisterAll() = nil, want duplicate error")
	}
	if !errors.Is(err, mutation.ErrDuplicateMutation) {
		t.Errorf("second RegisterAll() error = %v, want errors.Is(err, ErrDuplicateMutation)", err)
	}
}
