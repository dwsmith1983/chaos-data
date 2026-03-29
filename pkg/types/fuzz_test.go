package types_test

import (
	"encoding/json"
	"testing"

	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// FuzzParseSeverity feeds arbitrary strings to ParseSeverity to verify it never
// panics on unexpected input.
func FuzzParseSeverity(f *testing.F) {
	// Seed corpus: valid values, edge cases, and garbage.
	f.Add("low")
	f.Add("moderate")
	f.Add("severe")
	f.Add("critical")
	f.Add("LOW")
	f.Add("Critical")
	f.Add("")
	f.Add("unknown")
	f.Add("  low  ")
	f.Add("\x00\xff")

	f.Fuzz(func(t *testing.T, s string) {
		sev, err := types.ParseSeverity(s)
		if err != nil {
			return // expected for invalid input
		}
		// If parsing succeeded, the severity must be valid.
		if !sev.IsValid() {
			t.Errorf("ParseSeverity(%q) returned invalid severity %d", s, int(sev))
		}
	})
}

// FuzzChaosEventJSON feeds arbitrary bytes to JSON unmarshaling of ChaosEvent
// to verify it never panics on malformed input.
func FuzzChaosEventJSON(f *testing.F) {
	// Seed corpus: valid JSON, partial, empty, and garbage.
	validEvent := `{"id":"evt-1","scenario":"test","category":"data-arrival","severity":"low","target":"data.csv","mutation":"delay","params":{},"timestamp":"2024-01-01T00:00:00Z","mode":"deterministic"}`
	f.Add([]byte(validEvent))
	f.Add([]byte(`{}`))
	f.Add([]byte(`{"id":"x"}`))
	f.Add([]byte(``))
	f.Add([]byte(`not json`))
	f.Add([]byte(`{"severity":"bogus","mode":"invalid"}`))
	f.Add([]byte(`null`))

	f.Fuzz(func(t *testing.T, data []byte) {
		var event types.ChaosEvent
		if err := json.Unmarshal(data, &event); err != nil {
			return // expected for invalid JSON
		}
		// If unmarshaling succeeded, validation must not panic.
		_ = event.Validate()
	})
}
