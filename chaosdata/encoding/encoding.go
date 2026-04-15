package encoding

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/dwsmith1983/chaos-data/chaosdata"
)

// EncodingGenerator produces chaos data payloads containing encoding edge cases:
// invalid UTF-8 sequences, Unicode homoglyphs, bidirectional overrides, and
// zero-width characters.
type EncodingGenerator struct{}

func (EncodingGenerator) Name() string {
	return "encoding"
}

func (EncodingGenerator) Category() string {
	return "encoding"
}

func (g EncodingGenerator) Generate(opts chaosdata.GenerateOpts) (chaosdata.Payload, error) {
	records := []map[string]interface{}{}

	// Invalid UTF-8 sequences
	records = append(records, map[string]interface{}{
		"type": "Invalid UTF-8 byte sequences",
		"value": string([]byte{0xff, 0xfe, 0xfd}),
	})

	// Surrogate half (invalid)
	records = append(records, map[string]interface{}{
		"type":  "UTF-8 surrogate half (U+D800)",
		"value": string([]byte{0xED, 0xA0, 0x80}),
	})

	// Overlong encoding
	records = append(records, map[string]interface{}{
		"type":  "UTF-8 overlong NUL encoding",
		"value": string([]byte{0xC0, 0x80}),
	})

	// Truncated sequence
	records = append(records, map[string]interface{}{
		"type":  "UTF-8 truncated 3-byte sequence",
		"value": string([]byte{0xE2, 0x82}),
	})

	// BOM markers
	records = append(records, map[string]interface{}{
		"type":  "BOM markers (UTF-8)",
		"value": "\xef\xbb\xbftext",
	})
	records = append(records, map[string]interface{}{
		"type":  "BOM markers (UTF-16 LE)",
		"value": "\xff\xfet\x00e\x00x\x00t\x00",
	})
	records = append(records, map[string]interface{}{
		"type":  "BOM markers (UTF-16 BE)",
		"value": "\xfe\xff\x00t\x00e\x00x\x00t",
	})

	// Homoglyphs
	cyrillic_a := string([]byte{0xD0, 0xB0})
	cyrillic_e := string([]byte{0xD0, 0xB5})
	cyrillic_o := string([]byte{0xD0, 0xBE})
	cyrillic_p := string([]byte{0xD1, 0x80})
	cyrillic_c := string([]byte{0xD1, 0x81})

	records = append(records, map[string]interface{}{
		"type":  "Homoglyph - Cyrillic а (looks like Latin a)",
		"value": cyrillic_a + "dmin",
	})
	records = append(records, map[string]interface{}{
		"type":  "Homoglyph - Visual example (Cyrillic homoglyphs)",
		"value": cyrillic_e + "x" + cyrillic_a + "mpl" + cyrillic_e,
	})
	records = append(records, map[string]interface{}{
		"type":  "Homoglyph - Cyrillic с о р (looks like core)",
		"value": cyrillic_c + cyrillic_o + cyrillic_p + "e",
	})

	// Bidirectional override characters
	bidi_rlo := string([]byte{0xE2, 0x80, 0xAE})
	bidi_lro := string([]byte{0xE2, 0x80, 0xAD})
	bidi_rlm := string([]byte{0xE2, 0x80, 0x8F})
	bidi_pdf := string([]byte{0xE2, 0x80, 0xAC})

	records = append(records, map[string]interface{}{
		"type":  "Bidi override - RLO (renders as 'exe.txt')",
		"value": bidi_rlo + "txt.exe" + bidi_pdf,
	})
	records = append(records, map[string]interface{}{
		"type":  "Bidi override - RLM marker",
		"value": "safe" + bidi_rlm + "// evil code",
	})
	records = append(records, map[string]interface{}{
		"type":  "Bidi override - LRO",
		"value": bidi_lro + "normal" + bidi_pdf,
	})

	// Zero-width characters
	zwsp := string([]byte{0xE2, 0x80, 0x8B})
	zwnj := string([]byte{0xE2, 0x80, 0x8C})
	zwj := string([]byte{0xE2, 0x80, 0x8D})
	zwnbs := string([]byte{0xEF, 0xBB, 0xBF})

	records = append(records, map[string]interface{}{
		"type":  "Zero-width ZWSP (invisible space)",
		"value": "user" + zwsp + "name",
	})
	records = append(records, map[string]interface{}{
		"type":  "Zero-width ZWJ (joiner)",
		"value": "pass" + zwj + "word",
	})
	records = append(records, map[string]interface{}{
		"type":  "Zero-width ZWNJ (non-joiner)",
		"value": "secret" + zwnj + "!",
	})
	records = append(records, map[string]interface{}{
		"type":  "Zero-width BOM at start",
		"value": zwnbs + "abc" + zwsp,
	})

	// Additional encoding edge cases
	records = append(records, map[string]interface{}{
		"type":  "mixed encoding strings",
		"value": "utf8-and-\xff\xfe-utf16",
	})
	records = append(records, map[string]interface{}{
		"type":  "Base64 padding edge cases",
		"value": "YWJjZA==",
	})
	records = append(records, map[string]interface{}{
		"type":  "JSON snippet resembling encoding",
		"value": `{"encoding": "utf-8"}`,
	})
	records = append(records, map[string]interface{}{
		"type":  "XML snippet resembling encoding",
		"value": `<?xml version="1.0" encoding="ISO-8859-1"?>`,
	})

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

// Helper constants and functions for low-level byte testing

// InvalidUTF8SurrogateOffset is the byte offset of the first invalid byte
// (surrogate-half sequence 0xED 0xA0 0x80) in a payload containing invalid UTF-8.
const InvalidUTF8SurrogateOffset = 13

// InvalidUTF8OverlongOffset is the byte offset of the first invalid byte in
// the overlong NUL sequence (0xC0 0x80).
const InvalidUTF8OverlongOffset = 38

// InvalidUTF8TruncatedOffset is the byte offset of the first invalid byte in
// the truncated three-byte sequence (0xE2 0x82).
const InvalidUTF8TruncatedOffset = 60

// HomoglyphCyrillicA is the UTF-8 encoding of Cyrillic а (U+0430),
// a visual homoglyph of Latin a (U+0061).
var HomoglyphCyrillicA = []byte{0xD0, 0xB0}

// HomoglyphCyrillicE is the UTF-8 encoding of Cyrillic е (U+0435),
// a visual homoglyph of Latin e (U+0065).
var HomoglyphCyrillicE = []byte{0xD0, 0xB5}

// HomoglyphCyrillicO is the UTF-8 encoding of Cyrillic о (U+043E),
// a visual homoglyph of Latin o (U+006F).
var HomoglyphCyrillicO = []byte{0xD0, 0xBE}

// HomoglyphCyrillicP is the UTF-8 encoding of Cyrillic р (U+0440),
// a visual homoglyph of Latin p (U+0070).
var HomoglyphCyrillicP = []byte{0xD1, 0x80}

// HomoglyphCyrillicC is the UTF-8 encoding of Cyrillic с (U+0441),
// a visual homoglyph of Latin c (U+0063).
var HomoglyphCyrillicC = []byte{0xD1, 0x81}

// BidiRLO is the UTF-8 encoding of U+202E RIGHT-TO-LEFT OVERRIDE.
var BidiRLO = []byte{0xE2, 0x80, 0xAE}

// BidiLRO is the UTF-8 encoding of U+202D LEFT-TO-RIGHT OVERRIDE.
var BidiLRO = []byte{0xE2, 0x80, 0xAD}

// BidiRLM is the UTF-8 encoding of U+200F RIGHT-TO-LEFT MARK.
var BidiRLM = []byte{0xE2, 0x80, 0x8F}

// BidiPDF is the UTF-8 encoding of U+202C POP DIRECTIONAL FORMATTING.
var BidiPDF = []byte{0xE2, 0x80, 0xAC}

// ZeroWidthSP is the UTF-8 encoding of U+200B ZERO WIDTH SPACE.
var ZeroWidthSP = []byte{0xE2, 0x80, 0x8B}

// ZeroWidthNJ is the UTF-8 encoding of U+200C ZERO WIDTH NON-JOINER.
var ZeroWidthNJ = []byte{0xE2, 0x80, 0x8C}

// ZeroWidthJ is the UTF-8 encoding of U+200D ZERO WIDTH JOINER.
var ZeroWidthJ = []byte{0xE2, 0x80, 0x8D}

// ZeroWidthNBS is the UTF-8 encoding of U+FEFF ZERO WIDTH NO-BREAK SPACE (BOM).
var ZeroWidthNBS = []byte{0xEF, 0xBB, 0xBF}

// InvalidUTF8 returns raw bytes containing three categories of invalid UTF-8.
// Used for low-level testing.
func InvalidUTF8() []byte {
	var buf bytes.Buffer
	buf.WriteString(`{"surrogate":`)
	buf.Write([]byte{0xED, 0xA0, 0x80})
	buf.WriteString(`abcdefghi`)
	buf.WriteByte('"')
	buf.WriteByte(',')
	buf.WriteString(`"overlong":`)
	buf.Write([]byte{0xC0, 0x80})
	buf.WriteString(`ZZZZZZ`)
	buf.WriteByte('"')
	buf.WriteByte(',')
	buf.WriteString(`"truncated":`)
	buf.Write([]byte{0xE2, 0x82})
	buf.WriteByte('"')
	buf.WriteByte('}')
	return buf.Bytes()
}

// HomoglyphStrings returns raw bytes with Cyrillic homoglyphs.
// Used for low-level testing.
func HomoglyphStrings() []byte {
	var buf bytes.Buffer
	buf.WriteString(`{"username":"`)
	buf.Write(HomoglyphCyrillicA)
	buf.WriteString(`dmin"`)
	buf.WriteString(`,"domain":"`)
	buf.Write(HomoglyphCyrillicE)
	buf.WriteString(`x`)
	buf.Write(HomoglyphCyrillicA)
	buf.WriteString(`mpl`)
	buf.Write(HomoglyphCyrillicE)
	buf.WriteByte('"')
	buf.WriteString(`,"service":"`)
	buf.Write(HomoglyphCyrillicC)
	buf.Write(HomoglyphCyrillicO)
	buf.Write(HomoglyphCyrillicP)
	buf.WriteString(`e"`)
	buf.WriteByte('}')
	return buf.Bytes()
}

// BidiOverride returns raw bytes with bidirectional control characters.
// Used for low-level testing.
func BidiOverride() []byte {
	var buf bytes.Buffer
	buf.WriteString(`{"filename":"`)
	buf.Write(BidiRLO)
	buf.WriteString(`txt.exe`)
	buf.Write(BidiPDF)
	buf.WriteByte('"')
	buf.WriteString(`,"comment":"safe`)
	buf.Write(BidiRLM)
	buf.WriteString(`// evil code"`)
	buf.WriteString(`,"label":"`)
	buf.Write(BidiLRO)
	buf.WriteString(`normal`)
	buf.Write(BidiPDF)
	buf.WriteByte('"')
	buf.WriteByte('}')
	return buf.Bytes()
}

// ZeroWidthChars returns raw bytes with zero-width characters.
// Used for low-level testing.
func ZeroWidthChars() []byte {
	var buf bytes.Buffer
	buf.WriteString(`{"user`)
	buf.Write(ZeroWidthSP)
	buf.WriteString(`name":"admin"`)
	buf.WriteString(`,"pass`)
	buf.Write(ZeroWidthJ)
	buf.WriteString(`word":"secret`)
	buf.Write(ZeroWidthNJ)
	buf.WriteString(`!"`)
	buf.WriteString(`,"token":"`)
	buf.Write(ZeroWidthNBS)
	buf.WriteString(`abc`)
	buf.Write(ZeroWidthSP)
	buf.WriteByte('"')
	buf.WriteByte('}')
	return buf.Bytes()
}
