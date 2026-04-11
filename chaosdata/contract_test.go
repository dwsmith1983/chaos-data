package chaosdata_test

import (
	"testing"

	"github.com/dwsmith1983/chaos-data/chaosdata"
	_ "github.com/dwsmith1983/chaos-data/chaosdata/boundary"
	_ "github.com/dwsmith1983/chaos-data/chaosdata/concurrency"
	_ "github.com/dwsmith1983/chaos-data/chaosdata/encoding"
	_ "github.com/dwsmith1983/chaos-data/chaosdata/gospecific"
	_ "github.com/dwsmith1983/chaos-data/chaosdata/injection"
	_ "github.com/dwsmith1983/chaos-data/chaosdata/nulls"
	_ "github.com/dwsmith1983/chaos-data/chaosdata/numeric"
	_ "github.com/dwsmith1983/chaos-data/chaosdata/protocol"
	_ "github.com/dwsmith1983/chaos-data/chaosdata/referential"
	_ "github.com/dwsmith1983/chaos-data/chaosdata/schemadrift"
	_ "github.com/dwsmith1983/chaos-data/chaosdata/structural"
	_ "github.com/dwsmith1983/chaos-data/chaosdata/temporal"
	_ "github.com/dwsmith1983/chaos-data/chaosdata/volume"
)

func TestGeneratorsContract(t *testing.T) {
	generators := chaosdata.All()

	// Verify at least 13 generators are registered (one for each package imported)
	// The plan mentioned 14, let's see how many we actually get.
	if len(generators) < 13 {
		t.Errorf("Expected at least 13 generators, got %d", len(generators))
	}

	names := make(map[string]bool)
	for _, g := range generators {
		name := g.Name()
		category := g.Category()

		t.Run(name, func(t *testing.T) {
			if name == "" {
				t.Error("Generator name should not be empty")
			}
			if category == "" {
				t.Error("Generator category should not be empty")
			}

			if names[name] {
				t.Errorf("Duplicate generator name: %s", name)
			}
			names[name] = true

			// Test Generate
			payload, err := g.Generate(chaosdata.GenerateOpts{Count: 1})
			if err != nil {
				t.Errorf("Generate failed: %v", err)
			}
			if len(payload.Data) == 0 {
				t.Error("Generate returned empty payload data")
			}
		})
	}
}
