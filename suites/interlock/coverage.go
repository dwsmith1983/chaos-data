package interlocksuite

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v3"
)

// CapabilityStatus represents the testing status of a capability.
type CapabilityStatus string

const (
	// StatusCovered means all recorded scenarios passed.
	StatusCovered CapabilityStatus = "COVERED"
	// StatusGap means at least one recorded scenario failed.
	StatusGap CapabilityStatus = "GAP"
	// StatusUntested means no scenarios have been recorded.
	StatusUntested CapabilityStatus = "UNTESTED"
)

// Capability represents a single Interlock capability from the registry.
type Capability struct {
	ID          string   `yaml:"id"`
	Description string   `yaml:"description"`
	Scenarios   []string `yaml:"scenarios"`
}

// CapabilityResult tracks the outcome of testing a capability.
type CapabilityResult struct {
	Category   string
	Capability Capability
	Status     CapabilityStatus
	Passed     int
	Failed     int
	Duration   time.Duration
}

// CoverageMatrix is the full output of the coverage analysis.
type CoverageMatrix struct {
	Results  []CapabilityResult
	Total    int
	Covered  int
	Gaps     int
	Untested int
}

// CoverageRegistry is the YAML structure of coverage.yaml.
type CoverageRegistry struct {
	Capabilities map[string][]Capability `yaml:"capabilities"`
}

// CoverageTracker loads the registry and tracks scenario results.
type CoverageTracker struct {
	mu       sync.RWMutex
	registry CoverageRegistry
	results  map[string]*CapabilityResult // keyed by "category/id"
}

// NewCoverageTracker loads the capability registry from a YAML file.
func NewCoverageTracker(registryPath string) (*CoverageTracker, error) {
	data, err := os.ReadFile(registryPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read registry %s: %w", registryPath, err)
	}

	var reg CoverageRegistry
	if err := yaml.Unmarshal(data, &reg); err != nil {
		return nil, fmt.Errorf("failed to parse registry %s: %w", registryPath, err)
	}

	results := make(map[string]*CapabilityResult, len(reg.Capabilities))
	for category, caps := range reg.Capabilities {
		for _, cap := range caps {
			key := category + "/" + cap.ID
			results[key] = &CapabilityResult{
				Category:   category,
				Capability: cap,
				Status:     StatusUntested,
			}
		}
	}

	return &CoverageTracker{
		registry: reg,
		results:  results,
	}, nil
}

// Record records the result of running a scenario for a capability.
// The capability parameter uses the format "category/id" (e.g., "validation/equals").
// Unknown capabilities are silently ignored.
func (ct *CoverageTracker) Record(capability string, passed bool, duration time.Duration) {
	parts := strings.SplitN(capability, "/", 2)
	if len(parts) != 2 {
		return
	}

	ct.mu.Lock()
	defer ct.mu.Unlock()

	result, ok := ct.results[capability]
	if !ok {
		return
	}

	if passed {
		result.Passed++
	} else {
		result.Failed++
	}
	result.Duration += duration
}

// Matrix returns the full coverage matrix with computed statuses.
// Results are sorted deterministically by Category + "/" + Capability.ID.
func (ct *CoverageTracker) Matrix() CoverageMatrix {
	ct.mu.RLock()
	defer ct.mu.RUnlock()

	matrix := CoverageMatrix{
		Results: make([]CapabilityResult, 0, len(ct.results)),
	}

	for _, result := range ct.results {
		// Compute status based on recorded outcomes.
		status := StatusUntested
		recorded := result.Passed + result.Failed
		if recorded > 0 {
			if result.Failed > 0 {
				status = StatusGap
			} else {
				status = StatusCovered
			}
		}

		entry := CapabilityResult{
			Category:   result.Category,
			Capability: result.Capability,
			Status:     status,
			Passed:     result.Passed,
			Failed:     result.Failed,
			Duration:   result.Duration,
		}

		matrix.Results = append(matrix.Results, entry)
		matrix.Total++

		switch status {
		case StatusCovered:
			matrix.Covered++
		case StatusGap:
			matrix.Gaps++
		case StatusUntested:
			matrix.Untested++
		}
	}

	sort.Slice(matrix.Results, func(i, j int) bool {
		ki := matrix.Results[i].Category + "/" + matrix.Results[i].Capability.ID
		kj := matrix.Results[j].Category + "/" + matrix.Results[j].Capability.ID
		return ki < kj
	})

	return matrix
}
