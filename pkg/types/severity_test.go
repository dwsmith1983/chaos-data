package types_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/dwsmith1983/chaos-data/pkg/types"
)

func TestSeverityString(t *testing.T) {
	tests := []struct {
		severity types.Severity
		want     string
	}{
		{types.SeverityLow, "low"},
		{types.SeverityModerate, "moderate"},
		{types.SeveritySevere, "severe"},
		{types.SeverityCritical, "critical"},
		{types.Severity(0), "Severity(0)"},
		{types.Severity(99), "Severity(99)"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			got := tt.severity.String()
			if got != tt.want {
				t.Errorf("Severity.String() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestParseSeverity(t *testing.T) {
	tests := []struct {
		input   string
		want    types.Severity
		wantErr bool
	}{
		{"low", types.SeverityLow, false},
		{"moderate", types.SeverityModerate, false},
		{"severe", types.SeveritySevere, false},
		{"critical", types.SeverityCritical, false},
		{"LOW", types.SeverityLow, false},
		{"Moderate", types.SeverityModerate, false},
		{"CRITICAL", types.SeverityCritical, false},
		{"", 0, true},
		{"unknown", 0, true},
		{"high", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := types.ParseSeverity(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseSeverity(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ParseSeverity(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestSeverityRoundTrip(t *testing.T) {
	severities := []types.Severity{
		types.SeverityLow,
		types.SeverityModerate,
		types.SeveritySevere,
		types.SeverityCritical,
	}

	for _, sev := range severities {
		t.Run(sev.String(), func(t *testing.T) {
			parsed, err := types.ParseSeverity(sev.String())
			if err != nil {
				t.Fatalf("ParseSeverity(%q) returned error: %v", sev.String(), err)
			}
			if parsed != sev {
				t.Errorf("round-trip failed: got %v, want %v", parsed, sev)
			}
		})
	}
}

func TestSeverityComparison(t *testing.T) {
	tests := []struct {
		name string
		a, b types.Severity
		want bool
	}{
		{"low exceeds low", types.SeverityLow, types.SeverityLow, false},
		{"low exceeds moderate", types.SeverityLow, types.SeverityModerate, false},
		{"moderate exceeds low", types.SeverityModerate, types.SeverityLow, true},
		{"severe exceeds moderate", types.SeveritySevere, types.SeverityModerate, true},
		{"critical exceeds severe", types.SeverityCritical, types.SeveritySevere, true},
		{"critical exceeds low", types.SeverityCritical, types.SeverityLow, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.a.ExceedsThreshold(tt.b)
			if got != tt.want {
				t.Errorf("%v.ExceedsThreshold(%v) = %v, want %v", tt.a, tt.b, got, tt.want)
			}
		})
	}
}

func TestSeverityJSONMarshal(t *testing.T) {
	type wrapper struct {
		Level types.Severity `json:"level"`
	}

	w := wrapper{Level: types.SeverityModerate}
	data, err := json.Marshal(w)
	if err != nil {
		t.Fatalf("json.Marshal() error: %v", err)
	}

	want := `{"level":"moderate"}`
	if string(data) != want {
		t.Errorf("json.Marshal() = %s, want %s", data, want)
	}

	var got wrapper
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("json.Unmarshal() error: %v", err)
	}
	if got.Level != types.SeverityModerate {
		t.Errorf("json.Unmarshal() level = %v, want %v", got.Level, types.SeverityModerate)
	}
}

func TestSeverityJSONMarshalInvalid(t *testing.T) {
	_, err := json.Marshal(types.Severity(0))
	if err == nil {
		t.Error("json.Marshal(Severity(0)) should return error")
	}
	if !errors.Is(err, types.ErrInvalidSeverity) {
		t.Errorf("expected ErrInvalidSeverity, got: %v", err)
	}
}

func TestSeverityJSONUnmarshalInvalid(t *testing.T) {
	type wrapper struct {
		Level types.Severity `json:"level"`
	}

	tests := []struct {
		name  string
		input string
	}{
		{"invalid string", `{"level":"bogus"}`},
		{"non-string integer", `{"level":42}`},
		{"null value", `{"level":null}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var w wrapper
			err := json.Unmarshal([]byte(tt.input), &w)
			if err == nil {
				t.Errorf("json.Unmarshal(%s) should return error", tt.input)
			}
		})
	}
}

func TestSeverityValidate(t *testing.T) {
	valid := []types.Severity{
		types.SeverityLow,
		types.SeverityModerate,
		types.SeveritySevere,
		types.SeverityCritical,
	}
	for _, sev := range valid {
		t.Run(fmt.Sprintf("valid_%s", sev), func(t *testing.T) {
			if !sev.IsValid() {
				t.Errorf("Severity(%d).IsValid() = false, want true", sev)
			}
		})
	}

	invalid := []types.Severity{0, 5, 100}
	for _, sev := range invalid {
		t.Run(fmt.Sprintf("invalid_%d", int(sev)), func(t *testing.T) {
			if sev.IsValid() {
				t.Errorf("Severity(%d).IsValid() = true, want false", sev)
			}
		})
	}
}

func TestParseSeverityErrorIs(t *testing.T) {
	_, err := types.ParseSeverity("bogus")
	if !errors.Is(err, types.ErrInvalidSeverity) {
		t.Errorf("ParseSeverity(bogus) error should wrap ErrInvalidSeverity, got: %v", err)
	}
}
