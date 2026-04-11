package structural

import (
	"encoding/json"
	"fmt"

	"github.com/dwsmith1983/chaos-data/chaosdata"
)

var MaxDepth = 20

type StructuralGenerator struct{}

func (StructuralGenerator) Name() string {
	return "structural"
}

func (StructuralGenerator) Category() string {
	return "structural"
}

func (g StructuralGenerator) Generate(opts chaosdata.GenerateOpts) (chaosdata.Payload, error) {
	records := []map[string]interface{}{}
	records = append(records, map[string]interface{}{"type": "Deeply nested map", "value": map[string]interface{}{"a": map[string]interface{}{"b": map[string]interface{}{"c": 42}}}})
	records = append(records, map[string]interface{}{"type": "empty nested containers", "value": map[string]interface{}{"a": []interface{}{}, "b": map[string]interface{}{}}})
	records = append(records, map[string]interface{}{"type": "mixed-type slice", "value": []interface{}{1, "two", 3.0, true}})
	records = append(records, map[string]interface{}{"type": "single-element slice", "value": []interface{}{1}})
	records = append(records, map[string]interface{}{"type": "empty slice vs nil distinction", "value": map[string]interface{}{"empty": []interface{}{}, "nil": nil}})

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
		return chaosdata.Payload{}, fmt.Errorf("structural: marshal payload: %w", err)
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
	chaosdata.Register(StructuralGenerator{})
}
