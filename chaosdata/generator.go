package chaosdata

// Payload represents the generated chaos data.
type Payload struct {
	Data       []byte
	Type       string
	Attributes map[string]string
}

// GenerateOpts contains options for the chaos data generation.
type GenerateOpts struct {
	Size  int
	Count int
	Tags  map[string]string
}

// ChaosGenerator defines the interface for chaos data generators.
type ChaosGenerator interface {
	Name() string
	Category() string
	Generate(opts GenerateOpts) (Payload, error)
}
