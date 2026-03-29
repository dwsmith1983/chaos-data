package scenario_test

import (
	"testing"

	"github.com/dwsmith1983/chaos-data/pkg/scenario"
	"gopkg.in/yaml.v3"
)

// FuzzLoadFromBytes feeds arbitrary bytes to the YAML scenario parser to verify
// it never panics on malformed input.
func FuzzLoadFromBytes(f *testing.F) {
	// Seed corpus: valid YAML, partial YAML, empty, and garbage.
	f.Add([]byte(fullYAML))
	f.Add([]byte("name: test\ncategory: data-arrival\n"))
	f.Add([]byte(""))
	f.Add([]byte("{{{invalid yaml!!!"))
	f.Add([]byte("name: x\ncategory: unknown\nversion: 0\nmutation:\n  type: \"\"\n"))

	f.Fuzz(func(t *testing.T, data []byte) {
		var s scenario.Scenario
		if err := yaml.Unmarshal(data, &s); err != nil {
			return // parse failure is expected for fuzz input
		}
		// If parsing succeeded, validation must not panic.
		_ = s.Validate()
	})
}
