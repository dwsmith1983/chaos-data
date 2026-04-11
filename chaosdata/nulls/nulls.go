package nulls

import (
	"encoding/json"
	"fmt"

	"github.com/dwsmith1983/chaos-data/chaosdata"
)

type NullsGenerator struct{}

func (NullsGenerator) Name() string {
	return "nulls"
}

func (NullsGenerator) Category() string {
	return "nulls"
}

func (g NullsGenerator) Generate(opts chaosdata.GenerateOpts) (chaosdata.Payload, error) {
	records := []map[string]interface{}{}
	records = append(records, map[string]interface{}{"type": "nil", "value": nil})
	records = append(records, map[string]interface{}{"type": "empty string", "value": ""})
	records = append(records, map[string]interface{}{"type": "zero-length slice", "value": []interface{}{}})
	records = append(records, map[string]interface{}{"type": "zero-length map", "value": map[string]interface{}{}})
	records = append(records, map[string]interface{}{"type": "string literal null", "value": "null"})
	records = append(records, map[string]interface{}{"type": "string literal NULL", "value": "NULL"})
	records = append(records, map[string]interface{}{"type": "string literal nil", "value": "nil"})
	records = append(records, map[string]interface{}{"type": "string literal None", "value": "None"})
	records = append(records, map[string]interface{}{"type": "string literal undefined", "value": "undefined"})
	records = append(records, map[string]interface{}{"type": "Unicode null", "value": "\u0000"})
	records = append(records, map[string]interface{}{"type": "null byte in middle of string", "value": "a\x00b"})
	records = append(records, map[string]interface{}{"type": "sql.NullString Valid=false", "value": map[string]interface{}{"String": "", "Valid": false}})

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
		return chaosdata.Payload{}, fmt.Errorf("nulls: marshal payload: %w", err)
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
	chaosdata.Register(NullsGenerator{})
}
