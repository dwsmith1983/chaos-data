// Package encoding provides a chaos data generator for encoding-related
// edge cases, including invalid UTF-8, BOM markers, overlong encodings,
// and format resemblance.
package encoding

import (
	"encoding/json"
	"fmt"

	"github.com/dwsmith1983/chaos-data/chaosdata"
)

// EncodingGenerator generates chaos data related to character encodings and formats.
type EncodingGenerator struct{}

// Name returns the name of the generator.
func (g EncodingGenerator) Name() string {
	return "encoding"
}

// Category returns the category of the generator.
func (g EncodingGenerator) Category() string {
	return "encoding"
}

// Generate produces a payload containing various encoding-related chaos cases.
func (g EncodingGenerator) Generate(opts chaosdata.GenerateOpts) (chaosdata.Payload, error) {
	records := encodingRecords()

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
			"generator": "encoding",
			"category":  "encoding",
		},
	}, nil
}

func encodingRecords() []map[string]interface{} {
	return []map[string]interface{}{
		// Invalid UTF-8
		rec("invalid_utf8_sequence", string([]byte{0xff, 0xfe, 0xfd})),
		rec("truncated_utf8", string([]byte{0xe2, 0x82})), // Partial Euro symbol

		// BOM Markers
		rec("bom_utf8", "\xEF\xBB\xBFcontent"),
		rec("bom_utf16le", "\xFF\xFEcontent"),
		rec("bom_utf16be", "\xFE\xFFcontent"),

		// Overlong UTF-8
		rec("overlong_utf8_a", string([]byte{0xc1, 0xa1})),

		// Mixed Encoding
		rec("mixed_encoding", "UTF-8 content mixed with \x80\x81\x82"),

		// Base64 Padding
		rec("base64_no_padding", "SGVsbG8gd29ybGQ"),   // "Hello world"
		rec("base64_one_padding", "SGVsbG8gd29ybGQ="),
		rec("base64_two_padding", "SGVsbG8gd29ybGQ=="),
		rec("base64_invalid_padding", "SGVsbG8gd29ybGQ==="),

		// Resembling other encodings/formats
		rec("resembles_json", `{"key": "value", "nested": [1, 2, 3]}`),
		rec("resembles_xml", `<?xml version="1.0"?><root><item id="1">text</item></root>`),
		rec("resembles_yaml", "key: value\nlist:\n  - item1\n  - item2"),
	}
}

func rec(typ string, value interface{}) map[string]interface{} {
	return map[string]interface{}{
		"type":  typ,
		"value": value,
	}
}

func init() {
	chaosdata.Register(EncodingGenerator{})
}
