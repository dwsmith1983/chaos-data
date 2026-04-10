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

// Generate produces encoding-related chaos data.
func (g EncodingGenerator) Generate(opts chaosdata.GenerateOpts) (chaosdata.Payload, error) {
	records := g.allRecords()

	if opts.Count > 0 && opts.Count < len(records) {
		if opts.Count < len(records) {
			records = records[:opts.Count]
		}
	}

	data, err := json.Marshal(records)
	if err != nil {
		return chaosdata.Payload{}, fmt.Errorf("failed to marshal encoding chaos data: %w", err)
	}

	return chaosdata.Payload{
		Data: data,
		Type: "application/json",
		Attributes: map[string]string{
			"category": g.Category(),
			"name":     g.Name(),
			"count":    fmt.Sprintf("%d", len(records)),
		},
	}, nil
}

func (g EncodingGenerator) allRecords() []map[string]interface{} {
	return []map[string]interface{}{
		// Invalid UTF-8 byte sequences
		rec("invalid_utf8_sequence", string([]byte{0xff, 0xfe, 0xfd})),
		rec("invalid_utf8_continuation", string([]byte{0xc3, 0x28})),

		// BOM markers
		rec("bom_utf8", string([]byte{0xef, 0xbb, 0xbf})+"UTF-8 BOM"),
		rec("bom_utf16_le", string([]byte{0xff, 0xfe, 0x55, 0x00, 0x54, 0x00, 0x46, 0x00, 0x2d, 0x00, 0x31, 0x00, 0x36, 0x00, 0x20, 0x00, 0x4c, 0x00, 0x45, 0x00})),
		rec("bom_utf16_be", string([]byte{0xfe, 0xff, 0x00, 0x55, 0x00, 0x54, 0x00, 0x46, 0x00, 0x00, 0x2d, 0x00, 0x31, 0x00, 0x36, 0x00, 0x20, 0x00, 0x42, 0x00, 0x45})),

		// Overlong UTF-8 encodings
		rec("overlong_utf8_slash", string([]byte{0xc0, 0xaf})),
		rec("overlong_utf8_dot", string([]byte{0xc0, 0xae})),

		// Mixed encoding strings
		rec("mixed_encoding_latin1", "Hello "+string([]byte{0xbd, 0xb2, 0x3d, 0xbc})),

		// Base64 padding edge cases
		rec("base64_no_padding", "SGVsbG8"),
		rec("base64_valid_padding", "SGVsbG8="),
		rec("base64_invalid_padding", "SGVsbG8=="),

		// Strings resembling other encodings/formats (JSON/XML snippets)
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
