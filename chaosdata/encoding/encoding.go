package encoding

import (
	"encoding/json"
	"fmt"

	"github.com/dwsmith1983/chaos-data/chaosdata"
)

type EncodingGenerator struct{}

func (EncodingGenerator) Name() string {
	return "encoding"
}

func (EncodingGenerator) Category() string {
	return "encoding"
}

func (g EncodingGenerator) Generate(opts chaosdata.GenerateOpts) (chaosdata.Payload, error) {
	records := []map[string]interface{}{}
	records = append(records, map[string]interface{}{"type": "Invalid UTF-8 byte sequences", "value": string([]byte{0xff, 0xfe, 0xfd})})
	records = append(records, map[string]interface{}{"type": "BOM markers (UTF-8)", "value": "\xef\xbb\xbftext"})
	records = append(records, map[string]interface{}{"type": "BOM markers (UTF-16 LE)", "value": "\xff\xfet\x00e\x00x\x00t\x00"})
	records = append(records, map[string]interface{}{"type": "BOM markers (UTF-16 BE)", "value": "\xfe\xff\x00t\x00e\x00x\x00t"})
	records = append(records, map[string]interface{}{"type": "overlong UTF-8 encodings", "value": string([]byte{0xc0, 0xaf})})
	records = append(records, map[string]interface{}{"type": "mixed encoding strings", "value": "utf8-and-\xff\xfe-utf16"})
	records = append(records, map[string]interface{}{"type": "Base64 padding edge cases", "value": "YWJjZA=="})
	records = append(records, map[string]interface{}{"type": "JSON snippet resembling encoding", "value": `{"encoding": "utf-8"}`})
	records = append(records, map[string]interface{}{"type": "XML snippet resembling encoding", "value": `<?xml version="1.0" encoding="ISO-8859-1"?>`})

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
		return chaosdata.Payload{}, fmt.Errorf("encoding: marshal payload: %w", err)
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
	chaosdata.Register(EncodingGenerator{})
}
