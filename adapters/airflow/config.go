package airflow

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

// Config holds the settings for the Airflow adapter.
type Config struct {
	// URL is the Airflow REST API base URL
	// (e.g., "http://localhost:8080/api/v1").
	// Required.
	URL string

	// Headers are additional HTTP headers sent with every request.
	// Use for authentication (e.g., {"Authorization": "Basic dXNlcjpwYXNz"}).
	// Optional.
	Headers map[string]string

	// Version selects the Airflow REST API version: "v1" (default) or "v2".
	Version string

	// Username is the Airflow username used for v2 JWT authentication.
	// Required for v2 when Authorization header is not provided.
	Username string

	// Password is the Airflow password used for v2 JWT authentication.
	// Required for v2 when Authorization header is not provided.
	Password string
}

// Defaults sets default values for optional fields.
func (c *Config) Defaults() {
	if c.Version == "" {
		c.Version = "v1"
	}
}

// Validate checks that required fields are present and consistent.
func (c *Config) Validate() error {
	if c.URL == "" {
		return errors.New("airflow: URL is required")
	}
	u, err := url.Parse(c.URL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") {
		return errors.New("airflow: URL must be a valid http or https URL")
	}
	if u.Scheme == "http" {
		safe := &url.URL{Scheme: u.Scheme, Host: u.Host, Path: u.Path}
		fmt.Fprintf(warnWriter, "airflow: WARNING: URL %q uses http:// — consider using https:// for production\n", safe.String())
	}
	switch c.Version {
	case "", "v1", "v2":
	default:
		return fmt.Errorf("airflow: unknown version %q (must be v1 or v2)", c.Version)
	}
	if c.Version == "v2" {
		hasAuth := c.Headers != nil && c.Headers["Authorization"] != ""
		hasCreds := c.Username != "" && c.Password != ""
		if !hasAuth && !hasCreds {
			return errors.New("airflow: v2 requires username+password or Authorization header")
		}
	}
	return nil
}

// Redacted returns a copy of the Config with sensitive fields masked.
// Password is replaced with "[REDACTED]" if non-empty, and the
// Authorization header value is replaced with "[REDACTED]" if present.
// The original Config is not modified.
func (c Config) Redacted() Config {
	out := Config{
		URL:      c.URL,
		Version:  c.Version,
		Username: c.Username,
		Password: c.Password,
	}
	if c.Password != "" {
		out.Password = "[REDACTED]"
	}
	if c.Headers != nil {
		out.Headers = make(map[string]string, len(c.Headers))
		for k, v := range c.Headers {
			out.Headers[k] = v
		}
		if _, ok := out.Headers["Authorization"]; ok {
			out.Headers["Authorization"] = "[REDACTED]"
		}
	}
	return out
}
