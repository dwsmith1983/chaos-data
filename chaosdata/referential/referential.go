package referential

import (
	"encoding/json"
	"fmt"

	"github.com/dwsmith1983/chaos-data/chaosdata"
)

type ReferentialGenerator struct{}

func (ReferentialGenerator) Name() string {
	return "referential"
}

func (ReferentialGenerator) Category() string {
	return "referential"
}

func (g ReferentialGenerator) Generate(opts chaosdata.GenerateOpts) (chaosdata.Payload, error) {
	records := []map[string]interface{}{}
	records = append(records, map[string]interface{}{"type": "Dangling reference ID (UUID pointing to nothing)", "value": "550e8400-e29b-41d4-a716-446655440000"})
	records = append(records, map[string]interface{}{"type": "Self-referential ID", "value": "self-123"})
	records = append(records, map[string]interface{}{"type": "Duplicate IDs", "value": "dup-456"})
	records = append(records, map[string]interface{}{"type": "Empty/zero-value ID", "value": "00000000-0000-0000-0000-000000000000"})
	records = append(records, map[string]interface{}{"type": "ID with wrong type (string vs int)", "value": "ID-789"})

	count := opts.Count
	if count < 1 {
		count = 1
	}

	all := make([]map[string]interface{}, 0, len(records)*count)
	for i := 0; i < count; i++ {
		all = append(all, records...)
	}

	data, err := json.Marshal(all)
	if err != nil {
		return chaosdata.Payload{}, fmt.Errorf("referential: marshal payload: %w", err)
	}

	return chaosdata.Payload{
		Data: data,
		Type: "application/json",
		Attributes: map[string]string{
			"generator": g.Name(),
			"category":  g.Category(),
			"records":   fmt.Sprintf("%d", len(all)),
		},
	}, nil
}

func init() {
	chaosdata.Register(ReferentialGenerator{})
}
