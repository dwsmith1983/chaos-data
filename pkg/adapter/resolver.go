package adapter

import "context"

// DependencyResolver resolves downstream dependencies for a given target
// object. Implementations are used to compute blast radius for an experiment.
type DependencyResolver interface {
	// GetDownstream returns the list of downstream systems or datasets that
	// depend on the given target object. The target is typically an object key
	// (e.g., a file path or S3 key). Implementations match by prefix or any
	// other rule. Returns nil (not an error) when no match is found.
	GetDownstream(ctx context.Context, target string) ([]string, error)
}
