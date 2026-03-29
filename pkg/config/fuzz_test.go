package config_test

import (
	"testing"

	"github.com/dwsmith1983/chaos-data/pkg/config"
)

// FuzzLoadFromBytes feeds arbitrary bytes to the YAML config parser to verify
// it never panics on malformed input.
func FuzzLoadFromBytes(f *testing.F) {
	// Seed corpus: valid YAML, partial, empty, and garbage.
	f.Add([]byte(`transport:
  type: fs
  input_dir: /tmp/in
  output_dir: /tmp/out
safety:
  type: config
emitter:
  type: stdout
`))
	f.Add([]byte(`transport:
  type: s3
  region: us-east-1
  staging_bucket: staging
  pipeline_bucket: pipeline
`))
	f.Add([]byte(``))
	f.Add([]byte(`{{{not yaml`))
	f.Add([]byte(`transport:
  type: unknown
`))
	f.Add([]byte(`adapters:
  dagster:
    url: "http://localhost:3000/graphql"
  airflow:
    url: "http://localhost:8080/api/v1"
`))

	f.Fuzz(func(t *testing.T, data []byte) {
		cfg, err := config.LoadFromBytes(data)
		if err != nil {
			return // expected for invalid YAML
		}
		// If parsing succeeded, validation must not panic.
		_ = cfg.Validate()
	})
}
