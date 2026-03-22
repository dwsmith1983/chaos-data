package dagster

import (
	"errors"
	"net/url"
)

// Config holds the settings for the Dagster adapter.
type Config struct {
	// URL is the Dagster GraphQL endpoint (e.g., "http://localhost:3000/graphql").
	// Required.
	URL string

	// Headers are additional HTTP headers sent with every GraphQL request.
	// Use for authentication tokens (e.g., {"Dagster-Cloud-Api-Token": "..."}).
	// Optional.
	Headers map[string]string

	// RepositoryLocationName scopes sensor, schedule, and job queries to a
	// specific code location. Required for multi-code-location deployments.
	// Optional — when empty, queries rely on Dagster's default resolution.
	RepositoryLocationName string

	// RepositoryName scopes queries to a specific repository within the code
	// location. Must be set together with RepositoryLocationName.
	// Optional — when empty, queries omit the field.
	RepositoryName string
}

// Defaults sets default values for optional fields.
// Config has no optional fields with non-zero defaults, so this is a no-op.
func (c *Config) Defaults() {}

// Validate checks that required fields are present and consistent.
// It returns an error if URL is empty or if RepositoryName is set without
// RepositoryLocationName.
func (c *Config) Validate() error {
	if c.URL == "" {
		return errors.New("dagster: URL is required")
	}
	u, err := url.Parse(c.URL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") {
		return errors.New("dagster: URL must be a valid http or https URL")
	}
	if c.RepositoryName != "" && c.RepositoryLocationName == "" {
		return errors.New("dagster: RepositoryLocationName is required when RepositoryName is set")
	}
	return nil
}
