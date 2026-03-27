package airflow

import (
	"errors"
	"fmt"
	"net/url"
)

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
