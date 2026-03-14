package scenario_test

import (
	"errors"
	"testing"
	"time"

	"github.com/dwsmith1983/chaos-data/pkg/scenario"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

func TestBuiltinCatalog(t *testing.T) {
	scenarios, err := scenario.BuiltinCatalog()
	if err != nil {
		t.Fatalf("BuiltinCatalog() unexpected error: %v", err)
	}

	if len(scenarios) < 1 {
		t.Fatal("BuiltinCatalog() returned 0 scenarios, want at least 1")
	}

	// Every scenario must pass validation.
	for _, s := range scenarios {
		if err := s.Validate(); err != nil {
			t.Errorf("BuiltinCatalog() scenario %q failed validation: %v", s.Name, err)
		}
	}
}

func TestGet(t *testing.T) {
	s, err := scenario.Get("late-data")
	if err != nil {
		t.Fatalf("Get(%q) unexpected error: %v", "late-data", err)
	}

	if s.Name != "late-data" {
		t.Errorf("Get() Name = %q, want %q", s.Name, "late-data")
	}
	if s.Category != "data-arrival" {
		t.Errorf("Get() Category = %q, want %q", s.Category, "data-arrival")
	}
	if s.Severity != types.SeverityLow {
		t.Errorf("Get() Severity = %v, want %v", s.Severity, types.SeverityLow)
	}
	if s.Version != 1 {
		t.Errorf("Get() Version = %d, want %d", s.Version, 1)
	}
	if s.Mutation.Type != "delay" {
		t.Errorf("Get() Mutation.Type = %q, want %q", s.Mutation.Type, "delay")
	}
	if s.Mutation.Params["duration"] != "30m" {
		t.Errorf("Get() Mutation.Params[duration] = %q, want %q", s.Mutation.Params["duration"], "30m")
	}
	if s.Mutation.Params["jitter"] != "5m" {
		t.Errorf("Get() Mutation.Params[jitter] = %q, want %q", s.Mutation.Params["jitter"], "5m")
	}
	if s.Mutation.Params["release"] != "true" {
		t.Errorf("Get() Mutation.Params[release] = %q, want %q", s.Mutation.Params["release"], "true")
	}
	if s.Probability != 0.3 {
		t.Errorf("Get() Probability = %v, want %v", s.Probability, 0.3)
	}
	if s.Safety.MaxAffectedPct != 50 {
		t.Errorf("Get() Safety.MaxAffectedPct = %d, want %d", s.Safety.MaxAffectedPct, 50)
	}
	if s.Safety.Cooldown.Duration != 10*time.Minute {
		t.Errorf("Get() Safety.Cooldown = %v, want %v", s.Safety.Cooldown.Duration, 10*time.Minute)
	}
	if !s.Safety.SLAAware {
		t.Error("Get() Safety.SLAAware = false, want true")
	}
	if s.Target.Layer != "data" {
		t.Errorf("Get() Target.Layer = %q, want %q", s.Target.Layer, "data")
	}
}

func TestGetNotFound(t *testing.T) {
	_, err := scenario.Get("nonexistent-scenario")
	if err == nil {
		t.Fatal("Get() expected error for unknown scenario, got nil")
	}
	if !errors.Is(err, scenario.ErrNotFound) {
		t.Errorf("Get() error = %v, want wrapping ErrNotFound", err)
	}
}

func TestCatalogScenariosValidate(t *testing.T) {
	scenarios, err := scenario.BuiltinCatalog()
	if err != nil {
		t.Fatalf("BuiltinCatalog() unexpected error: %v", err)
	}

	for _, s := range scenarios {
		t.Run(s.Name, func(t *testing.T) {
			if err := s.Validate(); err != nil {
				t.Errorf("scenario %q failed validation: %v", s.Name, err)
			}
		})
	}
}
