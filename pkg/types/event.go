package types

import (
	"fmt"
	"time"
)

// ChaosEvent represents a single chaos injection event within an experiment.
type ChaosEvent struct {
	ID           string            `json:"id"`
	ExperimentID string            `json:"experiment_id"`
	Scenario     string            `json:"scenario"`
	Category     string            `json:"category"`
	Severity     Severity          `json:"severity"`
	Target       string            `json:"target"`
	Mutation     string            `json:"mutation"`
	Params       map[string]string `json:"params"`
	Timestamp    time.Time         `json:"timestamp"`
	Mode         string            `json:"mode"`
}

// Validate checks that the ChaosEvent has all required fields populated
// and that values are within allowed ranges.
func (e ChaosEvent) Validate() error {
	if e.ID == "" {
		return fmt.Errorf("chaos event: id must not be empty")
	}
	if e.Scenario == "" {
		return fmt.Errorf("chaos event: scenario must not be empty")
	}
	if !e.Severity.IsValid() {
		return fmt.Errorf("chaos event: invalid severity %d", int(e.Severity))
	}
	if !ValidMode(e.Mode) {
		return fmt.Errorf("chaos event: invalid mode %q (must be deterministic, probabilistic, or replay)", e.Mode)
	}
	return nil
}

// ExperimentStats holds aggregate statistics for a chaos experiment run.
type ExperimentStats struct {
	ExperimentID      string    `json:"experiment_id"`
	TotalEvents       int       `json:"total_events"`
	AffectedTargets   int       `json:"affected_targets"`
	AffectedPipelines int       `json:"affected_pipelines"`
	AffectedPct       float64   `json:"affected_pct"`
	StartTime         time.Time `json:"start_time"`
	EndTime           time.Time `json:"end_time"`
}

// Validate checks that the ExperimentStats values are within acceptable ranges.
func (s ExperimentStats) Validate() error {
	if s.AffectedPct < 0 || s.AffectedPct > 100 {
		return fmt.Errorf("experiment stats: affected_pct must be 0-100, got %v", s.AffectedPct)
	}
	if s.TotalEvents < 0 {
		return fmt.Errorf("experiment stats: total_events must be >= 0, got %d", s.TotalEvents)
	}
	if s.AffectedTargets < 0 {
		return fmt.Errorf("experiment stats: affected_targets must be >= 0, got %d", s.AffectedTargets)
	}
	if s.AffectedPipelines < 0 {
		return fmt.Errorf("experiment stats: affected_pipelines must be >= 0, got %d", s.AffectedPipelines)
	}
	return nil
}

// ExperimentState represents the lifecycle state of an experiment.
type ExperimentState string

// Experiment lifecycle states.
const (
	ExperimentPending   ExperimentState = "pending"
	ExperimentRunning   ExperimentState = "running"
	ExperimentCompleted ExperimentState = "completed"
	ExperimentAborted   ExperimentState = "aborted"
)

// IsValid returns true if the state is one of the known experiment states.
func (s ExperimentState) IsValid() bool {
	switch s {
	case ExperimentPending, ExperimentRunning, ExperimentCompleted, ExperimentAborted:
		return true
	default:
		return false
	}
}

// MutationRecord tracks the result of applying a single mutation to an object.
type MutationRecord struct {
	ObjectKey string            `json:"object_key"`
	Mutation  string            `json:"mutation"`
	Params    map[string]string `json:"params"`
	Applied   bool              `json:"applied"`
	Error     string            `json:"error"`
	Timestamp time.Time         `json:"timestamp"`
}
