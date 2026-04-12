package schemadrift

import (
	"encoding/json"
	"fmt"

	"github.com/dwsmith1983/chaos-data/chaosdata"
)

type SchemaDriftGenerator struct{}

func (SchemaDriftGenerator) Name() string {
	return "schemadrift"
}

func (SchemaDriftGenerator) Category() string {
	return "schemadrift"
}

func (g SchemaDriftGenerator) Generate(opts chaosdata.GenerateOpts) (chaosdata.Payload, error) {
	records := []map[string]interface{}{}
	records = append(records, map[string]interface{}{"type": "Field added (extra key)", "value": map[string]interface{}{"id": 1, "name": "test", "extra": "unexpected"}})
	records = append(records, map[string]interface{}{"type": "Field removed (missing expected key)", "value": map[string]interface{}{"id": 1}})
	records = append(records, map[string]interface{}{"type": "Field type changed (string where int expected)", "value": map[string]interface{}{"id": "1"}})
	records = append(records, map[string]interface{}{"type": "Field renamed (camelCase)", "value": map[string]interface{}{"userId": 1}})
	records = append(records, map[string]interface{}{"type": "Field renamed (snake_case)", "value": map[string]interface{}{"user_id": 1}})
	records = append(records, map[string]interface{}{"type": "Field renamed (PascalCase)", "value": map[string]interface{}{"UserId": 1}})
	records = append(records, map[string]interface{}{"type": "Array where object expected", "value": []interface{}{map[string]interface{}{"id": 1}}})
	records = append(records, map[string]interface{}{"type": "Object where array expected", "value": map[string]interface{}{"0": "a", "1": "b"}})

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
		return chaosdata.Payload{}, fmt.Errorf("schemadrift: marshal payload: %w", err)
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
	chaosdata.Register(SchemaDriftGenerator{})
}
