package types

// BlastRadiusEntry describes the downstream impact of a single applied mutation.
// It records which object was mutated, the mutation type, and the downstream
// systems or datasets that may be affected as a result.
type BlastRadiusEntry struct {
	MutatedObject string   `json:"mutated_object"`
	MutationType  string   `json:"mutation_type"`
	Downstream    []string `json:"downstream"`
}
