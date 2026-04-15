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
	_ "github.com/dwsmith1983/chaos-data/chaosdata/profiles"
	_ "github.com/dwsmith1983/chaos-data/chaosdata/protocol"
	_ "github.com/dwsmith1983/chaos-data/chaosdata/referential"
	_ "github.com/dwsmith1983/chaos-data/chaosdata/schemadrift"
	_ "github.com/dwsmith1983/chaos-data/chaosdata/structural"
	_ "github.com/dwsmith1983/chaos-data/chaosdata/temporal"
	_ "github.com/dwsmith1983/chaos-data/chaosdata/volume"
)

func TestGeneratorsContract(t *testing.T) {
	generators := chaosdata.All()

	const minGenerators = 14
	if len(generators) < minGenerators {
		t.Fatalf("Expected at least %d generators, got %d", minGenerators, len(generators))
	}

	// Verify all names are unique before running per-generator subtests.
	seen := make(map[string]int)
	for i, g := range generators {
		name := g.Name()
		if prev, ok := seen[name]; ok {
			t.Errorf("Duplicate generator name %q: index %d and %d", name, prev, i)
		}
		seen[name] = i
	}

	for _, g := range generators {
		t.Run(g.Name(), func(t *testing.T) {
			if g.Name() == "" {
				t.Error("Name() must not be empty")
			}
			if g.Category() == "" {
				t.Error("Category() must not be empty")
			}

			payload, err := g.Generate(chaosdata.GenerateOpts{Count: 1})
			if err != nil {
				t.Fatalf("Generate(Count:1) returned error: %v", err)
			}
			if len(payload.Data) == 0 {
				t.Error("Generate(Count:1) returned empty Data")
			}
			if payload.Type == "" {
				t.Error("Generate(Count:1) returned empty Type")
			}
		})
	}
}
