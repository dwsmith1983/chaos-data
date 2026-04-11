package concurrency

import (
	"encoding/json"
	"fmt"

	"github.com/dwsmith1983/chaos-data/chaosdata"
)

type ConcurrencyGenerator struct{}

func (ConcurrencyGenerator) Name() string {
	return "concurrency"
}

func (ConcurrencyGenerator) Category() string {
	return "concurrency"
}

func (g ConcurrencyGenerator) Generate(opts chaosdata.GenerateOpts) (chaosdata.Payload, error) {
	records := []map[string]interface{}{}
	records = append(records, map[string]interface{}{"type": "Shared mutable map", "value": map[string]int{"a": 1, "b": 2}})
	records = append(records, map[string]interface{}{"type": "Shared mutable slice", "value": []int{1, 2, 3}})
	records = append(records, map[string]interface{}{"type": "Identical timestamps", "value": "2024-06-15T12:00:00Z"})
	records = append(records, map[string]interface{}{"type": "Overlapping ranges", "value": map[string]int{"start": 10, "end": 20}})
	records = append(records, map[string]interface{}{"type": "Race condition trigger value", "value": 42})

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
		return chaosdata.Payload{}, fmt.Errorf("concurrency: marshal payload: %w", err)
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
	chaosdata.Register(ConcurrencyGenerator{})
}
