package types

import (
	"encoding/json"
	"fmt"
	"time"
)

// Duration wraps time.Duration with custom YAML/JSON unmarshaling that
// accepts Go duration strings (e.g., "5m", "1h30m", "500ms").
type Duration struct {
	time.Duration
}

// MarshalJSON encodes the duration as a Go duration string.
func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.Duration.String())
}

// UnmarshalJSON decodes a Go duration string into a Duration.
func (d *Duration) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("duration: unmarshal JSON: %w", err)
	}
	parsed, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("duration: parse %q: %w", s, err)
	}
	d.Duration = parsed
	return nil
}

// MarshalYAML encodes the duration as a Go duration string.
func (d Duration) MarshalYAML() (interface{}, error) {
	return d.Duration.String(), nil
}

// UnmarshalYAML decodes a Go duration string into a Duration.
func (d *Duration) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var s string
	if err := unmarshal(&s); err != nil {
		return fmt.Errorf("duration: unmarshal YAML: %w", err)
	}
	parsed, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("duration: parse %q: %w", s, err)
	}
	d.Duration = parsed
	return nil
}
