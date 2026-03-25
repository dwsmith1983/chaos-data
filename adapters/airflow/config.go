package airflow

import (
	"errors"
	"net/url"
)

// Config holds the settings for the Airflow adapter.
type Config struct {
	// URL is the Airflow REST API v1 base URL
	// (e.g., "http://localhost:8080/api/v1").
	// Required.
	URL string

	// Headers are additional HTTP headers sent with every request.
	// Use for authentication (e.g., {"Authorization": "Basic dXNlcjpwYXNz"}).
	// Optional.
	Headers map[string]string
}

// Defaults sets default values for optional fields.
func (c *Config) Defaults() {}

// Validate checks that required fields are present and consistent.
func (c *Config) Validate() error {
	if c.URL == "" {
		return errors.New("airflow: URL is required")
	}
	u, err := url.Parse(c.URL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") {
		return errors.New("airflow: URL must be a valid http or https URL")
	}
	return nil
}
