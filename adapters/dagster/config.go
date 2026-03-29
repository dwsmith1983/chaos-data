package dagster

import (
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
)

// warnWriter is the destination for non-fatal warnings emitted by Validate.
// It defaults to os.Stderr and can be overridden in tests via SetWarnWriter.
// Not safe for concurrent use — tests that swap this must not use t.Parallel().
var warnWriter io.Writer = os.Stderr

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
	if u.Scheme == "http" {
		safe := &url.URL{Scheme: u.Scheme, Host: u.Host, Path: u.Path}
		fmt.Fprintf(warnWriter, "dagster: WARNING: URL %q uses http:// — consider using https:// for production\n", safe.String())
	}
	if c.RepositoryName != "" && c.RepositoryLocationName == "" {
		return errors.New("dagster: RepositoryLocationName is required when RepositoryName is set")
	}
	return nil
}

// Redacted returns a copy of the Config with sensitive fields masked.
// The Dagster-Cloud-Api-Token header value is replaced with "[REDACTED]"
// if present. The original Config is not modified.
func (c Config) Redacted() Config {
	out := Config{
		URL:                    c.URL,
		RepositoryLocationName: c.RepositoryLocationName,
		RepositoryName:         c.RepositoryName,
	}
	if c.Headers != nil {
		out.Headers = make(map[string]string, len(c.Headers))
		for k, v := range c.Headers {
			out.Headers[k] = v
		}
		if _, ok := out.Headers["Dagster-Cloud-Api-Token"]; ok {
			out.Headers["Dagster-Cloud-Api-Token"] = "[REDACTED]"
		}
	}
	return out
}
