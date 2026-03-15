package local

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"gopkg.in/yaml.v3"
)

// Compile-time interface assertion.
var _ adapter.DependencyResolver = (*StaticResolver)(nil)

// dependencyEntry is a single rule in the YAML configuration.
type dependencyEntry struct {
	Prefix     string   `yaml:"prefix"`
	Downstream []string `yaml:"downstream"`
}

// staticResolverConfig is the top-level structure parsed from the YAML file.
type staticResolverConfig struct {
	Dependencies []dependencyEntry `yaml:"dependencies"`
}

// StaticResolver is a DependencyResolver backed by a static YAML file that
// maps object key prefixes to lists of downstream systems.
//
// Example YAML:
//
//	dependencies:
//	  - prefix: "events-"
//	    downstream: ["analytics.user_events", "reporting.daily_summary"]
//	  - prefix: "transactions-"
//	    downstream: ["billing.invoices"]
type StaticResolver struct {
	deps []dependencyEntry
}

// NewStaticResolver reads and parses the YAML file at path, returning a
// StaticResolver ready to resolve downstream dependencies. Returns an error
// if the file cannot be read or contains invalid YAML.
func NewStaticResolver(path string) (*StaticResolver, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("static resolver: read %q: %w", path, err)
	}

	var cfg staticResolverConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("static resolver: parse %q: %w", path, err)
	}

	for i, dep := range cfg.Dependencies {
		if dep.Prefix == "" {
			return nil, fmt.Errorf("static resolver: dependency %d has empty prefix", i)
		}
	}

	return &StaticResolver{deps: cfg.Dependencies}, nil
}

// GetDownstream returns the union of all downstream entries for rules whose
// prefix matches the given target (via strings.HasPrefix). Returns nil when
// no rule matches. The context is accepted for interface compliance but is not
// consulted — the resolver operates entirely from in-memory configuration.
func (r *StaticResolver) GetDownstream(_ context.Context, target string) ([]string, error) {
	var result []string
	for _, dep := range r.deps {
		if strings.HasPrefix(target, dep.Prefix) {
			result = append(result, dep.Downstream...)
		}
	}
	return result, nil
}
