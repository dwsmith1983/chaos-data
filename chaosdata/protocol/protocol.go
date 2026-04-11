package protocol

import (
	"encoding/json"
	"fmt"

	"github.com/dwsmith1983/chaos-data/chaosdata"
)

type ProtocolGenerator struct{}

func (ProtocolGenerator) Name() string {
	return "protocol"
}

func (ProtocolGenerator) Category() string {
	return "protocol"
}

func (g ProtocolGenerator) Generate(opts chaosdata.GenerateOpts) (chaosdata.Payload, error) {
	records := []map[string]interface{}{}
	records = append(records, map[string]interface{}{"type": "Malformed JSON (trailing comma)", "value": `{"a": 1,}`})
	records = append(records, map[string]interface{}{"type": "Malformed JSON (single quotes)", "value": `{'a': 1}`})
	records = append(records, map[string]interface{}{"type": "Malformed JSON (unquoted keys)", "value": `{a: 1}`})
	records = append(records, map[string]interface{}{"type": "Malformed HTTP header (oversized)", "value": "X-Large: " + string(make([]byte, 10000))})
	records = append(records, map[string]interface{}{"type": "Malformed HTTP header (missing colon)", "value": "X-No-Colon value"})
	records = append(records, map[string]interface{}{"type": "Malformed HTTP header (null bytes)", "value": "X-Null: a\x00b"})
	records = append(records, map[string]interface{}{"type": "Invalid URL (missing scheme)", "value": "example.com"})
	records = append(records, map[string]interface{}{"type": "Invalid URL (port 99999)", "value": "http://example.com:99999"})
	records = append(records, map[string]interface{}{"type": "Invalid URL (unicode host)", "value": "http://ëxample.com"})
	records = append(records, map[string]interface{}{"type": "gRPC/protobuf invalid field numbers", "value": string([]byte{0x00, 0x01})})
	records = append(records, map[string]interface{}{"type": "gRPC/protobuf unknown field tags", "value": string([]byte{0x55, 0x55})})

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
		return chaosdata.Payload{}, fmt.Errorf("protocol: marshal payload: %w", err)
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
	chaosdata.Register(ProtocolGenerator{})
}
