package scenario_test

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/dwsmith1983/chaos-data/pkg/scenario"
	"github.com/dwsmith1983/chaos-data/pkg/types"
	"gopkg.in/yaml.v3"
)

// validScenario returns a fully populated Scenario that passes Validate().
func validScenario() scenario.Scenario {
	return scenario.Scenario{
		Name:        "late-arrival-s3",
		Description: "Delay S3 object delivery by 30 minutes",
		Category:    "data-arrival",
		Severity:    types.SeverityModerate,
		Version:     1,
		Target: scenario.TargetSpec{
			Layer:     "data",
			Transport: "s3",
			Filter: scenario.FilterSpec{
				Prefix: "ingest/",
				Match:  "*.parquet",
			},
		},
		Mutation: scenario.MutationSpec{
			Type: "delay",
			Params: map[string]string{
				"duration": "30m",
			},
		},
		Probability: 0.5,
		Safety: scenario.ScenarioSafety{
			MaxAffectedPct: 25,
			Cooldown:        types.Duration{Duration: 5 * time.Minute},
			SLAAware:        true,
		},
		Expected: &scenario.ExpectedResponse{
			Within: types.Duration{Duration: 10 * time.Minute},
			Asserts: []types.Assertion{
				{Type: types.AssertSensorState, Target: "pipeline-a/upstream", Condition: types.CondIsStale},
			},
		},
	}
}

const fullYAML = `name: late-arrival-s3
description: Delay S3 object delivery by 30 minutes
category: data-arrival
severity: moderate
version: 1
target:
  layer: data
  transport: s3
  filter:
    prefix: "ingest/"
    match: "*.parquet"
mutation:
  type: delay
  params:
    duration: "30m"
probability: 0.5
safety:
  max_affected_pct: 25
  cooldown: 5m
  sla_aware: true
expected_response:
  within: 10m
  asserts:
    - type: sensor_state
      target: pipeline-a/upstream
      condition: is_stale
`

func TestParseValidYAML(t *testing.T) {
	var s scenario.Scenario
	if err := yaml.Unmarshal([]byte(fullYAML), &s); err != nil {
		t.Fatalf("yaml.Unmarshal() error: %v", err)
	}

	if s.Name != "late-arrival-s3" {
		t.Errorf("Name = %q, want %q", s.Name, "late-arrival-s3")
	}
	if s.Description != "Delay S3 object delivery by 30 minutes" {
		t.Errorf("Description = %q, want %q", s.Description, "Delay S3 object delivery by 30 minutes")
	}
	if s.Category != "data-arrival" {
		t.Errorf("Category = %q, want %q", s.Category, "data-arrival")
	}
	if s.Severity != types.SeverityModerate {
		t.Errorf("Severity = %v, want %v", s.Severity, types.SeverityModerate)
	}
	if s.Version != 1 {
		t.Errorf("Version = %d, want %d", s.Version, 1)
	}
	if s.Target.Layer != "data" {
		t.Errorf("Target.Layer = %q, want %q", s.Target.Layer, "data")
	}
	if s.Target.Transport != "s3" {
		t.Errorf("Target.Transport = %q, want %q", s.Target.Transport, "s3")
	}
	if s.Target.Filter.Prefix != "ingest/" {
		t.Errorf("Target.Filter.Prefix = %q, want %q", s.Target.Filter.Prefix, "ingest/")
	}
	if s.Target.Filter.Match != "*.parquet" {
		t.Errorf("Target.Filter.Match = %q, want %q", s.Target.Filter.Match, "*.parquet")
	}
	if s.Mutation.Type != "delay" {
		t.Errorf("Mutation.Type = %q, want %q", s.Mutation.Type, "delay")
	}
	if s.Mutation.Params["duration"] != "30m" {
		t.Errorf("Mutation.Params[duration] = %q, want %q", s.Mutation.Params["duration"], "30m")
	}
	if s.Probability != 0.5 {
		t.Errorf("Probability = %v, want %v", s.Probability, 0.5)
	}
	if s.Safety.MaxAffectedPct != 25 {
		t.Errorf("Safety.MaxAffectedPct = %d, want %d", s.Safety.MaxAffectedPct, 25)
	}
	if s.Safety.Cooldown.Duration != 5*time.Minute {
		t.Errorf("Safety.Cooldown = %v, want %v", s.Safety.Cooldown.Duration, 5*time.Minute)
	}
	if !s.Safety.SLAAware {
		t.Error("Safety.SLAAware = false, want true")
	}
	if s.Expected == nil {
		t.Fatal("Expected is nil")
	}
	if s.Expected.Within.Duration != 10*time.Minute {
		t.Errorf("Expected.Within = %v, want %v", s.Expected.Within.Duration, 10*time.Minute)
	}
	if len(s.Expected.Asserts) != 1 {
		t.Fatalf("Expected.Asserts len = %d, want 1", len(s.Expected.Asserts))
	}
	if s.Expected.Asserts[0].Condition != types.CondIsStale {
		t.Errorf("Expected.Asserts[0].Condition = %q, want %q",
			s.Expected.Asserts[0].Condition, types.CondIsStale)
	}
}

func TestValidatePassesForValidScenario(t *testing.T) {
	s := validScenario()
	if err := s.Validate(); err != nil {
		t.Errorf("Validate() error = %v, want nil", err)
	}
}

func TestValidateFailures(t *testing.T) {
	tests := []struct {
		name   string
		modify func(*scenario.Scenario)
	}{
		{
			name: "empty name",
			modify: func(s *scenario.Scenario) {
				s.Name = ""
			},
		},
		{
			name: "invalid category",
			modify: func(s *scenario.Scenario) {
				s.Category = "unknown-category"
			},
		},
		{
			name: "invalid severity",
			modify: func(s *scenario.Scenario) {
				s.Severity = types.Severity(0)
			},
		},
		{
			name: "version less than 1",
			modify: func(s *scenario.Scenario) {
				s.Version = 0
			},
		},
		{
			name: "empty mutation type",
			modify: func(s *scenario.Scenario) {
				s.Mutation.Type = ""
			},
		},
		{
			name: "probability greater than 1",
			modify: func(s *scenario.Scenario) {
				s.Probability = 1.5
			},
		},
		{
			name: "probability less than 0",
			modify: func(s *scenario.Scenario) {
				s.Probability = -0.1
			},
		},
		{
			name: "max_affected_pct greater than 100",
			modify: func(s *scenario.Scenario) {
				s.Safety.MaxAffectedPct = 101
			},
		},
		{name: "expected_response with zero within", modify: func(s *scenario.Scenario) {
			s.Expected = &scenario.ExpectedResponse{
				Within:  types.Duration{Duration: 0},
				Asserts: []types.Assertion{{Type: types.AssertSensorState, Target: "p/k", Condition: types.CondIsStale}},
			}
		}},
		{name: "expected_response with empty asserts", modify: func(s *scenario.Scenario) {
			s.Expected = &scenario.ExpectedResponse{
				Within:  types.Duration{Duration: time.Minute},
				Asserts: []types.Assertion{},
			}
		}},
		{name: "expected_response with invalid assertion", modify: func(s *scenario.Scenario) {
			s.Expected = &scenario.ExpectedResponse{
				Within:  types.Duration{Duration: time.Minute},
				Asserts: []types.Assertion{{Type: types.AssertionType("bogus"), Target: "x", Condition: types.CondExists}},
			}
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := validScenario()
			tt.modify(&s)
			err := s.Validate()
			if err == nil {
				t.Error("Validate() should return an error")
			}
			if !errors.Is(err, scenario.ErrInvalidScenario) {
				t.Errorf("Validate() error should wrap ErrInvalidScenario, got: %v", err)
			}
		})
	}
}

func TestJSONRoundTrip(t *testing.T) {
	original := validScenario()

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("json.Marshal() error: %v", err)
	}

	var decoded scenario.Scenario
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("json.Unmarshal() error: %v", err)
	}

	if decoded.Name != original.Name {
		t.Errorf("Name = %q, want %q", decoded.Name, original.Name)
	}
	if decoded.Description != original.Description {
		t.Errorf("Description = %q, want %q", decoded.Description, original.Description)
	}
	if decoded.Category != original.Category {
		t.Errorf("Category = %q, want %q", decoded.Category, original.Category)
	}
	if decoded.Severity != original.Severity {
		t.Errorf("Severity = %v, want %v", decoded.Severity, original.Severity)
	}
	if decoded.Version != original.Version {
		t.Errorf("Version = %d, want %d", decoded.Version, original.Version)
	}
	if decoded.Target.Layer != original.Target.Layer {
		t.Errorf("Target.Layer = %q, want %q", decoded.Target.Layer, original.Target.Layer)
	}
	if decoded.Target.Transport != original.Target.Transport {
		t.Errorf("Target.Transport = %q, want %q", decoded.Target.Transport, original.Target.Transport)
	}
	if decoded.Target.Filter.Prefix != original.Target.Filter.Prefix {
		t.Errorf("Target.Filter.Prefix = %q, want %q", decoded.Target.Filter.Prefix, original.Target.Filter.Prefix)
	}
	if decoded.Target.Filter.Match != original.Target.Filter.Match {
		t.Errorf("Target.Filter.Match = %q, want %q", decoded.Target.Filter.Match, original.Target.Filter.Match)
	}
	if decoded.Mutation.Type != original.Mutation.Type {
		t.Errorf("Mutation.Type = %q, want %q", decoded.Mutation.Type, original.Mutation.Type)
	}
	if decoded.Mutation.Params["duration"] != original.Mutation.Params["duration"] {
		t.Errorf("Mutation.Params[duration] = %q, want %q",
			decoded.Mutation.Params["duration"], original.Mutation.Params["duration"])
	}
	if decoded.Probability != original.Probability {
		t.Errorf("Probability = %v, want %v", decoded.Probability, original.Probability)
	}
	if decoded.Safety.MaxAffectedPct != original.Safety.MaxAffectedPct {
		t.Errorf("Safety.MaxAffectedPct = %d, want %d",
			decoded.Safety.MaxAffectedPct, original.Safety.MaxAffectedPct)
	}
	if decoded.Safety.Cooldown != original.Safety.Cooldown {
		t.Errorf("Safety.Cooldown = %v, want %v",
			decoded.Safety.Cooldown, original.Safety.Cooldown)
	}
	if decoded.Safety.SLAAware != original.Safety.SLAAware {
		t.Errorf("Safety.SLAAware = %v, want %v",
			decoded.Safety.SLAAware, original.Safety.SLAAware)
	}
	if decoded.Expected == nil {
		t.Fatal("Expected is nil after JSON round-trip")
	}
	if decoded.Expected.Within != original.Expected.Within {
		t.Errorf("Expected.Within = %v, want %v",
			decoded.Expected.Within, original.Expected.Within)
	}
	if len(decoded.Expected.Asserts) != len(original.Expected.Asserts) {
		t.Fatalf("Expected.Asserts len = %d, want %d",
			len(decoded.Expected.Asserts), len(original.Expected.Asserts))
	}
	if decoded.Expected.Asserts[0] != original.Expected.Asserts[0] {
		t.Errorf("Expected.Asserts[0] = %+v, want %+v",
			decoded.Expected.Asserts[0], original.Expected.Asserts[0])
	}
}

func TestYAMLRoundTrip(t *testing.T) {
	original := validScenario()

	data, err := yaml.Marshal(original)
	if err != nil {
		t.Fatalf("yaml.Marshal() error: %v", err)
	}

	var decoded scenario.Scenario
	if err := yaml.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("yaml.Unmarshal() error: %v", err)
	}

	if decoded.Name != original.Name {
		t.Errorf("Name = %q, want %q", decoded.Name, original.Name)
	}
	if decoded.Description != original.Description {
		t.Errorf("Description = %q, want %q", decoded.Description, original.Description)
	}
	if decoded.Category != original.Category {
		t.Errorf("Category = %q, want %q", decoded.Category, original.Category)
	}
	if decoded.Severity != original.Severity {
		t.Errorf("Severity = %v, want %v", decoded.Severity, original.Severity)
	}
	if decoded.Version != original.Version {
		t.Errorf("Version = %d, want %d", decoded.Version, original.Version)
	}
	if decoded.Mutation.Type != original.Mutation.Type {
		t.Errorf("Mutation.Type = %q, want %q", decoded.Mutation.Type, original.Mutation.Type)
	}
	if decoded.Probability != original.Probability {
		t.Errorf("Probability = %v, want %v", decoded.Probability, original.Probability)
	}
	if decoded.Safety.MaxAffectedPct != original.Safety.MaxAffectedPct {
		t.Errorf("Safety.MaxAffectedPct = %d, want %d",
			decoded.Safety.MaxAffectedPct, original.Safety.MaxAffectedPct)
	}
	if decoded.Safety.Cooldown != original.Safety.Cooldown {
		t.Errorf("Safety.Cooldown = %v, want %v",
			decoded.Safety.Cooldown, original.Safety.Cooldown)
	}
	if decoded.Safety.SLAAware != original.Safety.SLAAware {
		t.Errorf("Safety.SLAAware = %v, want %v",
			decoded.Safety.SLAAware, original.Safety.SLAAware)
	}
	if decoded.Expected == nil {
		t.Fatal("Expected is nil after YAML round-trip")
	}
	if decoded.Expected.Within != original.Expected.Within {
		t.Errorf("Expected.Within = %v, want %v",
			decoded.Expected.Within, original.Expected.Within)
	}
	if len(decoded.Expected.Asserts) != len(original.Expected.Asserts) {
		t.Fatalf("Expected.Asserts len = %d, want %d",
			len(decoded.Expected.Asserts), len(original.Expected.Asserts))
	}
	if decoded.Expected.Asserts[0] != original.Expected.Asserts[0] {
		t.Errorf("Expected.Asserts[0] = %+v, want %+v",
			decoded.Expected.Asserts[0], original.Expected.Asserts[0])
	}
}

func TestDefaultValuesForOptionalFields(t *testing.T) {
	minimalYAML := `
name: minimal-scenario
category: data-arrival
severity: low
version: 1
mutation:
  type: delay
`
	var s scenario.Scenario
	if err := yaml.Unmarshal([]byte(minimalYAML), &s); err != nil {
		t.Fatalf("yaml.Unmarshal() error: %v", err)
	}

	if s.Probability != 0 {
		t.Errorf("Probability = %v, want 0 (default)", s.Probability)
	}
	if s.Safety.MaxAffectedPct != 0 {
		t.Errorf("Safety.MaxAffectedPct = %d, want 0 (default)", s.Safety.MaxAffectedPct)
	}
	if s.Safety.Cooldown.Duration != 0 {
		t.Errorf("Safety.Cooldown = %v, want 0 (default)", s.Safety.Cooldown.Duration)
	}
	if s.Safety.SLAAware {
		t.Error("Safety.SLAAware = true, want false (default)")
	}
	if s.Expected != nil {
		t.Errorf("Expected = %v, want nil (default)", s.Expected)
	}
	if s.Description != "" {
		t.Errorf("Description = %q, want empty (default)", s.Description)
	}
	if s.Target.Layer != "" {
		t.Errorf("Target.Layer = %q, want empty (default)", s.Target.Layer)
	}
	if s.Target.Transport != "" {
		t.Errorf("Target.Transport = %q, want empty (default)", s.Target.Transport)
	}
}

func TestValidateAllCategories(t *testing.T) {
	categories := []string{
		"data-arrival",
		"data-quality",
		"state-consistency",
		"infrastructure",
		"orchestrator",
		"compound",
	}

	for _, cat := range categories {
		t.Run(cat, func(t *testing.T) {
			s := validScenario()
			s.Category = cat
			if err := s.Validate(); err != nil {
				t.Errorf("Validate() error = %v for valid category %q", err, cat)
			}
		})
	}
}

func TestValidateBoundaryProbability(t *testing.T) {
	tests := []struct {
		name    string
		prob    float64
		wantErr bool
	}{
		{"probability 0.0", 0.0, false},
		{"probability 1.0", 1.0, false},
		{"probability 0.5", 0.5, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := validScenario()
			s.Probability = tt.prob
			err := s.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidateBoundaryMaxAffectedPct(t *testing.T) {
	tests := []struct {
		name    string
		pct     int
		wantErr bool
	}{
		{"max_affected_pct 0", 0, false},
		{"max_affected_pct 100", 100, false},
		{"max_affected_pct 50", 50, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := validScenario()
			s.Safety.MaxAffectedPct = tt.pct
			err := s.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
