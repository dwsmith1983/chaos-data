package types_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/dwsmith1983/chaos-data/pkg/types"
)

func TestChaosEventZeroValueFailsValidation(t *testing.T) {
	var event types.ChaosEvent
	err := event.Validate()
	if err == nil {
		t.Error("zero-value ChaosEvent should fail validation")
	}
}

func TestChaosEventJSONRoundTrip(t *testing.T) {
	ts := time.Date(2026, 3, 14, 12, 0, 0, 0, time.UTC)
	original := types.ChaosEvent{
		ID:           "evt-002",
		ExperimentID: "exp-002",
		Scenario:     "latency_spike",
		Category:     "performance",
		Severity:     types.SeveritySevere,
		Target:       "api_gateway",
		Mutation:      "add_delay",
		Params:       map[string]string{"ms": "500"},
		Timestamp:    ts,
		Mode:         "probabilistic",
	}

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("json.Marshal() error: %v", err)
	}

	// Verify snake_case keys in JSON output.
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("json.Unmarshal(raw) error: %v", err)
	}
	expectedKeys := []string{
		"id", "experiment_id", "scenario", "category", "severity",
		"target", "mutation", "params", "timestamp", "mode",
	}
	for _, key := range expectedKeys {
		if _, ok := raw[key]; !ok {
			t.Errorf("JSON output missing snake_case key %q", key)
		}
	}

	var decoded types.ChaosEvent
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("json.Unmarshal() error: %v", err)
	}

	if decoded.ID != original.ID {
		t.Errorf("ID = %q, want %q", decoded.ID, original.ID)
	}
	if decoded.ExperimentID != original.ExperimentID {
		t.Errorf("ExperimentID = %q, want %q", decoded.ExperimentID, original.ExperimentID)
	}
	if decoded.Scenario != original.Scenario {
		t.Errorf("Scenario = %q, want %q", decoded.Scenario, original.Scenario)
	}
	if decoded.Category != original.Category {
		t.Errorf("Category = %q, want %q", decoded.Category, original.Category)
	}
	if decoded.Severity != original.Severity {
		t.Errorf("Severity = %v, want %v", decoded.Severity, original.Severity)
	}
	if decoded.Target != original.Target {
		t.Errorf("Target = %q, want %q", decoded.Target, original.Target)
	}
	if decoded.Mutation != original.Mutation {
		t.Errorf("Mutation = %q, want %q", decoded.Mutation, original.Mutation)
	}
	if decoded.Params["ms"] != original.Params["ms"] {
		t.Errorf("Params[ms] = %q, want %q", decoded.Params["ms"], original.Params["ms"])
	}
	if !decoded.Timestamp.Equal(original.Timestamp) {
		t.Errorf("Timestamp = %v, want %v", decoded.Timestamp, original.Timestamp)
	}
	if decoded.Mode != original.Mode {
		t.Errorf("Mode = %q, want %q", decoded.Mode, original.Mode)
	}
}

func TestChaosEventValidate(t *testing.T) {
	validEvent := func() types.ChaosEvent {
		return types.ChaosEvent{
			ID:           "evt-001",
			ExperimentID: "exp-001",
			Scenario:     "null_injection",
			Category:     "data_integrity",
			Severity:     types.SeverityModerate,
			Target:       "orders_table",
			Mutation:      "set_null",
			Params:       map[string]string{"column": "email"},
			Timestamp:    time.Now(),
			Mode:         "deterministic",
		}
	}

	tests := []struct {
		name    string
		modify  func(*types.ChaosEvent)
		wantErr bool
	}{
		{
			name:    "valid event passes",
			modify:  func(_ *types.ChaosEvent) {},
			wantErr: false,
		},
		{
			name:    "valid probabilistic mode",
			modify:  func(e *types.ChaosEvent) { e.Mode = "probabilistic" },
			wantErr: false,
		},
		{
			name:    "valid replay mode",
			modify:  func(e *types.ChaosEvent) { e.Mode = "replay" },
			wantErr: false,
		},
		{
			name:    "empty ID fails",
			modify:  func(e *types.ChaosEvent) { e.ID = "" },
			wantErr: true,
		},
		{
			name:    "empty Scenario fails",
			modify:  func(e *types.ChaosEvent) { e.Scenario = "" },
			wantErr: true,
		},
		{
			name:    "invalid Severity fails",
			modify:  func(e *types.ChaosEvent) { e.Severity = types.Severity(0) },
			wantErr: true,
		},
		{
			name:    "invalid Mode fails",
			modify:  func(e *types.ChaosEvent) { e.Mode = "chaos" },
			wantErr: true,
		},
		{
			name:    "empty Mode fails",
			modify:  func(e *types.ChaosEvent) { e.Mode = "" },
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := validEvent()
			tt.modify(&e)
			err := e.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestExperimentStateStringValues(t *testing.T) {
	tests := []struct {
		state types.ExperimentState
		want  string
	}{
		{types.ExperimentPending, "pending"},
		{types.ExperimentRunning, "running"},
		{types.ExperimentCompleted, "completed"},
		{types.ExperimentAborted, "aborted"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			got := string(tt.state)
			if got != tt.want {
				t.Errorf("ExperimentState = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestMutationRecordJSONRoundTrip(t *testing.T) {
	ts := time.Date(2026, 3, 14, 12, 0, 0, 0, time.UTC)
	original := types.MutationRecord{
		ObjectKey: "s3://bucket/key.parquet",
		Mutation:  "corrupt_column",
		Params:    map[string]string{"column": "price", "strategy": "null"},
		Applied:   true,
		Error:     "",
		Timestamp: ts,
	}

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("json.Marshal() error: %v", err)
	}

	// Verify snake_case keys.
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("json.Unmarshal(raw) error: %v", err)
	}
	expectedKeys := []string{"object_key", "mutation", "params", "applied", "error", "timestamp"}
	for _, key := range expectedKeys {
		if _, ok := raw[key]; !ok {
			t.Errorf("JSON output missing snake_case key %q", key)
		}
	}

	var decoded types.MutationRecord
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("json.Unmarshal() error: %v", err)
	}

	if decoded.ObjectKey != original.ObjectKey {
		t.Errorf("ObjectKey = %q, want %q", decoded.ObjectKey, original.ObjectKey)
	}
	if decoded.Mutation != original.Mutation {
		t.Errorf("Mutation = %q, want %q", decoded.Mutation, original.Mutation)
	}
	if decoded.Params["column"] != original.Params["column"] {
		t.Errorf("Params[column] = %q, want %q", decoded.Params["column"], original.Params["column"])
	}
	if decoded.Params["strategy"] != original.Params["strategy"] {
		t.Errorf("Params[strategy] = %q, want %q", decoded.Params["strategy"], original.Params["strategy"])
	}
	if decoded.Applied != original.Applied {
		t.Errorf("Applied = %v, want %v", decoded.Applied, original.Applied)
	}
	if decoded.Error != original.Error {
		t.Errorf("Error = %q, want %q", decoded.Error, original.Error)
	}
	if !decoded.Timestamp.Equal(original.Timestamp) {
		t.Errorf("Timestamp = %v, want %v", decoded.Timestamp, original.Timestamp)
	}
}

func TestMutationRecordWithError(t *testing.T) {
	record := types.MutationRecord{
		ObjectKey: "s3://bucket/key.parquet",
		Mutation:  "corrupt_column",
		Params:    map[string]string{"column": "price"},
		Applied:   false,
		Error:     "column not found",
		Timestamp: time.Now(),
	}

	data, err := json.Marshal(record)
	if err != nil {
		t.Fatalf("json.Marshal() error: %v", err)
	}

	var decoded types.MutationRecord
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("json.Unmarshal() error: %v", err)
	}

	if decoded.Applied {
		t.Error("Applied should be false for failed mutation")
	}
	if decoded.Error != "column not found" {
		t.Errorf("Error = %q, want %q", decoded.Error, "column not found")
	}
}

func TestExperimentStatsValidate(t *testing.T) {
	validStats := func() types.ExperimentStats {
		return types.ExperimentStats{
			ExperimentID:      "exp-001",
			TotalEvents:       10,
			AffectedTargets:   3,
			AffectedPipelines: 2,
			AffectedPct:       30.0,
			StartTime:         time.Now(),
			EndTime:           time.Now().Add(5 * time.Minute),
		}
	}

	tests := []struct {
		name    string
		modify  func(*types.ExperimentStats)
		wantErr bool
	}{
		{
			name:    "valid stats passes",
			modify:  func(_ *types.ExperimentStats) {},
			wantErr: false,
		},
		{
			name:    "affected_pct over 100 fails",
			modify:  func(s *types.ExperimentStats) { s.AffectedPct = 100.1 },
			wantErr: true,
		},
		{
			name:    "affected_pct negative fails",
			modify:  func(s *types.ExperimentStats) { s.AffectedPct = -1.0 },
			wantErr: true,
		},
		{
			name:    "negative total_events fails",
			modify:  func(s *types.ExperimentStats) { s.TotalEvents = -1 },
			wantErr: true,
		},
		{
			name:    "negative affected_targets fails",
			modify:  func(s *types.ExperimentStats) { s.AffectedTargets = -1 },
			wantErr: true,
		},
		{
			name:    "negative affected_pipelines fails",
			modify:  func(s *types.ExperimentStats) { s.AffectedPipelines = -1 },
			wantErr: true,
		},
		{
			name:    "zero values pass",
			modify: func(s *types.ExperimentStats) {
				s.TotalEvents = 0
				s.AffectedTargets = 0
				s.AffectedPipelines = 0
				s.AffectedPct = 0
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := validStats()
			tt.modify(&s)
			err := s.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("ExperimentStats.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestExperimentStatsJSONRoundTrip(t *testing.T) {
	start := time.Date(2026, 3, 14, 12, 0, 0, 0, time.UTC)
	end := start.Add(10 * time.Minute)
	original := types.ExperimentStats{
		ExperimentID:      "exp-001",
		TotalEvents:       42,
		AffectedTargets:   5,
		AffectedPipelines: 3,
		AffectedPct:       25.5,
		StartTime:         start,
		EndTime:           end,
	}

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("json.Marshal() error: %v", err)
	}

	// Verify snake_case keys.
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("json.Unmarshal(raw) error: %v", err)
	}
	expectedKeys := []string{
		"experiment_id", "total_events", "affected_targets",
		"affected_pipelines", "affected_pct", "start_time", "end_time",
	}
	for _, key := range expectedKeys {
		if _, ok := raw[key]; !ok {
			t.Errorf("JSON output missing snake_case key %q", key)
		}
	}

	var decoded types.ExperimentStats
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("json.Unmarshal() error: %v", err)
	}

	if decoded.ExperimentID != original.ExperimentID {
		t.Errorf("ExperimentID = %q, want %q", decoded.ExperimentID, original.ExperimentID)
	}
	if decoded.TotalEvents != original.TotalEvents {
		t.Errorf("TotalEvents = %d, want %d", decoded.TotalEvents, original.TotalEvents)
	}
	if decoded.AffectedTargets != original.AffectedTargets {
		t.Errorf("AffectedTargets = %d, want %d", decoded.AffectedTargets, original.AffectedTargets)
	}
	if decoded.AffectedPipelines != original.AffectedPipelines {
		t.Errorf("AffectedPipelines = %d, want %d", decoded.AffectedPipelines, original.AffectedPipelines)
	}
	if decoded.AffectedPct != original.AffectedPct {
		t.Errorf("AffectedPct = %v, want %v", decoded.AffectedPct, original.AffectedPct)
	}
	if !decoded.StartTime.Equal(original.StartTime) {
		t.Errorf("StartTime = %v, want %v", decoded.StartTime, original.StartTime)
	}
	if !decoded.EndTime.Equal(original.EndTime) {
		t.Errorf("EndTime = %v, want %v", decoded.EndTime, original.EndTime)
	}
}

func TestExperimentStateIsValid(t *testing.T) {
	tests := []struct {
		state types.ExperimentState
		want  bool
	}{
		{types.ExperimentPending, true},
		{types.ExperimentRunning, true},
		{types.ExperimentCompleted, true},
		{types.ExperimentAborted, true},
		{types.ExperimentState("unknown"), false},
		{types.ExperimentState(""), false},
		{types.ExperimentState("PENDING"), false},
	}

	for _, tt := range tests {
		t.Run(string(tt.state), func(t *testing.T) {
			got := tt.state.IsValid()
			if got != tt.want {
				t.Errorf("ExperimentState(%q).IsValid() = %v, want %v", tt.state, got, tt.want)
			}
		})
	}
}
