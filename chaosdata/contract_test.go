package chaosdata

import (
	"testing"
)

func TestAllGeneratorsContract(t *testing.T) {
	generators := All()
	names := make(map[string]bool)

	for _, g := range generators {
		name := g.Name()

		// Verify Name() is non-empty
		if name == "" {
			t.Error("Found generator with empty name")
		}

		// Verify Name() is unique
		if names[name] {
			t.Errorf("Duplicate generator name found: %s", name)
		}
		names[name] = true

		t.Run(name, func(t *testing.T) {
			// Verify Generate() does not panic and follows contract
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("Generate() panicked: %v", r)
				}
			}()

			opts := GenerateOpts{
				Size: 1024,
			}
			payload, err := g.Generate(opts)

			if err != nil {
				if err.Error() == "" {
					t.Error("Generate() returned non-nil error but error message is empty")
				}
			} else {
				if payload.Data == nil {
					t.Error("Generate() returned nil error but nil Data")
				}
			}
		})
	}
}
