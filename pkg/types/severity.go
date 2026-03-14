package types

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// ErrInvalidSeverity is returned when a severity value is not recognized.
var ErrInvalidSeverity = errors.New("invalid severity")

// Severity represents the impact level of a chaos scenario.
type Severity int

// Severity values start at 1. The zero value is intentionally invalid.
const (
	SeverityLow      Severity = iota + 1
	SeverityModerate
	SeveritySevere
	SeverityCritical
)

var severityNames = map[Severity]string{
	SeverityLow:      "low",
	SeverityModerate: "moderate",
	SeveritySevere:   "severe",
	SeverityCritical: "critical",
}

var severityValues = map[string]Severity{
	"low":      SeverityLow,
	"moderate": SeverityModerate,
	"severe":   SeveritySevere,
	"critical": SeverityCritical,
}

// String returns the lowercase string representation of a Severity.
func (s Severity) String() string {
	if name, ok := severityNames[s]; ok {
		return name
	}
	return fmt.Sprintf("Severity(%d)", s)
}

// ParseSeverity parses a case-insensitive string into a Severity value.
func ParseSeverity(s string) (Severity, error) {
	if sev, ok := severityValues[strings.ToLower(s)]; ok {
		return sev, nil
	}
	return 0, fmt.Errorf("%w: %q", ErrInvalidSeverity, s)
}

// ExceedsThreshold returns true if s is strictly greater than threshold.
func (s Severity) ExceedsThreshold(threshold Severity) bool {
	return s > threshold
}

// IsValid returns true if the severity is a known value.
func (s Severity) IsValid() bool {
	_, ok := severityNames[s]
	return ok
}

// MarshalJSON implements json.Marshaler.
func (s Severity) MarshalJSON() ([]byte, error) {
	if !s.IsValid() {
		return nil, fmt.Errorf("%w: %d", ErrInvalidSeverity, int(s))
	}
	return json.Marshal(s.String())
}

// UnmarshalJSON implements json.Unmarshaler.
func (s *Severity) UnmarshalJSON(data []byte) error {
	var str string
	if err := json.Unmarshal(data, &str); err != nil {
		return fmt.Errorf("severity: unmarshal JSON: %w", err)
	}
	parsed, err := ParseSeverity(str)
	if err != nil {
		return err
	}
	*s = parsed
	return nil
}

// MarshalYAML implements yaml.Marshaler.
func (s Severity) MarshalYAML() (interface{}, error) {
	if !s.IsValid() {
		return nil, fmt.Errorf("%w: %d", ErrInvalidSeverity, int(s))
	}
	return s.String(), nil
}

// UnmarshalYAML implements yaml.Unmarshaler.
func (s *Severity) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var str string
	if err := unmarshal(&str); err != nil {
		return fmt.Errorf("severity: unmarshal YAML: %w", err)
	}
	parsed, err := ParseSeverity(str)
	if err != nil {
		return err
	}
	*s = parsed
	return nil
}
