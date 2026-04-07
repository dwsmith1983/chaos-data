// Package encoding provides chaos data generators that produce raw []byte
// payloads containing encoding edge cases: invalid UTF-8 sequences, Unicode
// homoglyphs, bidirectional override characters, and zero-width characters.
//
// All generators are registered via init() and are deterministic — identical
// calls return byte-for-byte identical output.
package encoding

import (
	"bytes"
	"fmt"
	"sort"
)

// Generator is a function that produces a raw []byte chaos payload.
type Generator func() []byte

// registry holds all registered encoding generators keyed by name.
var registry = map[string]Generator{}

// Register adds a named Generator to the global registry.
// Panics on duplicate name to catch wiring errors at startup.
func Register(name string, g Generator) {
	if _, exists := registry[name]; exists {
		panic(fmt.Sprintf("encoding: duplicate generator registered: %q", name))
	}
	registry[name] = g
}

// Get returns the Generator registered under name and a boolean indicating
// whether it was found.
func Get(name string) (Generator, bool) {
	g, ok := registry[name]
	return g, ok
}

// Names returns the sorted list of registered generator names.
func Names() []string {
	names := make([]string, 0, len(registry))
	for k := range registry {
		names = append(names, k)
	}
	sort.Strings(names)
	return names
}

func init() {
	Register("InvalidUTF8", InvalidUTF8)
	Register("HomoglyphStrings", HomoglyphStrings)
	Register("BidiOverride", BidiOverride)
	Register("ZeroWidthChars", ZeroWidthChars)
}

// ---------------------------------------------------------------------------
// InvalidUTF8
// ---------------------------------------------------------------------------
//
// Produces a JSON object whose string values embed raw invalid UTF-8 byte
// sequences.  Three distinct invalid sequences are included at deterministic
// offsets so test code can assert on exact byte positions.
//
// Layout (bytes):
//
//	[0-12]  {"surrogate":          (13 bytes)
//	[13-15] 0xED 0xA0 0x80         surrogate half U+D800 — invalid in UTF-8
//	[16-24] abcdefghi              valid filler (9 bytes)
//	[25-25] "                      close surrogate value
//	[26-26] ,
//	[27-37] "overlong":            (11 bytes)
//	[38-39] 0xC0 0x80              overlong NUL encoding — invalid in UTF-8
//	[40-45] ZZZZZZ                 valid filler (6 bytes)
//	[46-46] "                      close overlong value
//	[47-47] ,
//	[48-59] "truncated":           (12 bytes)
//	[60-61] 0xE2 0x82              truncated 3-byte sequence — invalid UTF-8
//	[62-62] "                      close truncated value
//	[63-63] }

// InvalidUTF8SurrogateOffset is the byte offset of the first invalid byte
// (surrogate-half sequence 0xED 0xA0 0x80) in the payload returned by
// InvalidUTF8.
const InvalidUTF8SurrogateOffset = 13

// InvalidUTF8OverlongOffset is the byte offset of the first invalid byte in
// the overlong NUL sequence (0xC0 0x80) embedded in the InvalidUTF8 payload.
const InvalidUTF8OverlongOffset = 38

// InvalidUTF8TruncatedOffset is the byte offset of the first invalid byte in
// the truncated three-byte sequence (0xE2 0x82) embedded in the InvalidUTF8
// payload.
const InvalidUTF8TruncatedOffset = 60

// InvalidUTF8 returns a JSON payload that embeds three categories of invalid
// UTF-8 byte sequences at the offsets declared by the Offset constants above.
// The payload is assembled with explicit byte splices; offsets are stable
// regardless of Go version or platform.
func InvalidUTF8() []byte {
	var buf bytes.Buffer

	// Bytes 0-12: key prefix (13 bytes) → invalid bytes start at offset 13.
	buf.WriteString(`{"surrogate":`) // 13 bytes
	// Bytes 13-15: surrogate half — 0xED 0xA0 0x80 (U+D800, invalid UTF-8).
	buf.Write([]byte{0xED, 0xA0, 0x80})
	// Bytes 16-24: valid ASCII filler inside surrogate value (9 bytes).
	buf.WriteString(`abcdefghi`)
	// Byte 25: close surrogate value quote.
	buf.WriteByte('"')
	// Byte 26: comma.
	buf.WriteByte(',')
	// Bytes 27-37: "overlong": (11 bytes) → invalid bytes start at offset 38.
	buf.WriteString(`"overlong":`)
	// Bytes 38-39: overlong NUL — 0xC0 0x80 (U+0000 encoded with 2 bytes, invalid).
	buf.Write([]byte{0xC0, 0x80})
	// Bytes 40-45: valid ASCII filler inside overlong value (6 bytes).
	buf.WriteString(`ZZZZZZ`)
	// Byte 46: close overlong value quote.
	buf.WriteByte('"')
	// Byte 47: comma.
	buf.WriteByte(',')
	// Bytes 48-59: "truncated": (12 bytes) → invalid bytes start at offset 60.
	buf.WriteString(`"truncated":`)
	// Bytes 60-61: truncated 3-byte sequence — 0xE2 0x82 with no trailing byte.
	buf.Write([]byte{0xE2, 0x82})
	// Byte 62: close truncated value quote.
	buf.WriteByte('"')
	// Byte 63: close object.
	buf.WriteByte('}')

	return buf.Bytes()
}

// ---------------------------------------------------------------------------
// HomoglyphStrings
// ---------------------------------------------------------------------------
//
// Produces a JSON payload where string values contain Unicode characters that
// are visually identical (or near-identical) to their ASCII counterparts but
// map to different code points.  All substitutions use the Cyrillic block.
//
// Homoglyph substitutions:
//
//	Latin a (U+0061) → Cyrillic а (U+0430)  UTF-8: 0xD0 0xB0
//	Latin e (U+0065) → Cyrillic е (U+0435)  UTF-8: 0xD0 0xB5
//	Latin o (U+006F) → Cyrillic о (U+043E)  UTF-8: 0xD0 0xBE
//	Latin p (U+0070) → Cyrillic р (U+0440)  UTF-8: 0xD1 0x80
//	Latin c (U+0063) → Cyrillic с (U+0441)  UTF-8: 0xD1 0x81

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

// HomoglyphStrings returns a JSON payload where string values embed Cyrillic
// homoglyphs of common Latin letters.
//
// Fields produced:
//
//	"username": "<а>dmin"               leading 'a' is Cyrillic U+0430
//	"domain":   "<е>x<а>mpl<е>"        е(U+0435) x а(U+0430) m p l е(U+0435)
//	"service":  "<с><о><р>e"           с(U+0441) о(U+043E) р(U+0440) e
func HomoglyphStrings() []byte {
	var buf bytes.Buffer

	// "username": Cyrillic а followed by ASCII "dmin"
	buf.WriteString(`{"username":"`)
	buf.Write(HomoglyphCyrillicA) // U+0430 — looks like Latin 'a'
	buf.WriteString(`dmin"`)

	// "domain": visually "example" but with Cyrillic homoglyphs
	buf.WriteString(`,"domain":"`)
	buf.Write(HomoglyphCyrillicE) // U+0435 — looks like 'e'
	buf.WriteString(`x`)
	buf.Write(HomoglyphCyrillicA) // U+0430 — looks like 'a'
	buf.WriteString(`mpl`)
	buf.Write(HomoglyphCyrillicE) // U+0435 — looks like 'e'
	buf.WriteByte('"')

	// "service": visually "core" but с о р are Cyrillic
	buf.WriteString(`,"service":"`)
	buf.Write(HomoglyphCyrillicC) // U+0441 — looks like 'c'
	buf.Write(HomoglyphCyrillicO) // U+043E — looks like 'o'
	buf.Write(HomoglyphCyrillicP) // U+0440 — looks like 'p' / 'r' depending on font
	buf.WriteString(`e"`)

	buf.WriteByte('}')

	return buf.Bytes()
}

// ---------------------------------------------------------------------------
// BidiOverride
// ---------------------------------------------------------------------------
//
// Produces a JSON payload containing Unicode bidirectional control characters
// embedded inside field values.  These characters can cause text renderers to
// display content in a misleading order (the "Trojan Source" class of attack).
//
// Characters used:
//
//	U+202E RIGHT-TO-LEFT OVERRIDE (RLO)       UTF-8: 0xE2 0x80 0xAE
//	U+202D LEFT-TO-RIGHT OVERRIDE (LRO)       UTF-8: 0xE2 0x80 0xAD
//	U+200F RIGHT-TO-LEFT MARK      (RLM)       UTF-8: 0xE2 0x80 0x8F
//	U+202C POP DIRECTIONAL FORMATTING (PDF)   UTF-8: 0xE2 0x80 0xAC

// BidiRLO is the UTF-8 encoding of U+202E RIGHT-TO-LEFT OVERRIDE.
var BidiRLO = []byte{0xE2, 0x80, 0xAE}

// BidiLRO is the UTF-8 encoding of U+202D LEFT-TO-RIGHT OVERRIDE.
var BidiLRO = []byte{0xE2, 0x80, 0xAD}

// BidiRLM is the UTF-8 encoding of U+200F RIGHT-TO-LEFT MARK.
var BidiRLM = []byte{0xE2, 0x80, 0x8F}

// BidiPDF is the UTF-8 encoding of U+202C POP DIRECTIONAL FORMATTING.
var BidiPDF = []byte{0xE2, 0x80, 0xAC}

// BidiOverride returns a JSON payload whose values embed bidirectional control
// characters that can reverse or reorder the visual representation of text in
// vulnerable renderers.
//
// Fields:
//
//	"filename": "<RLO>txt.exe<PDF>"      renders as "exe.txt" under RTL override
//	"comment":  "safe<RLM>// evil code"  misleading directional marker
//	"label":    "<LRO>normal<PDF>"       left-to-right forced override
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

// ---------------------------------------------------------------------------
// ZeroWidthChars
// ---------------------------------------------------------------------------
//
// Produces a JSON payload where both field names and values contain zero-width
// Unicode characters.  These characters are invisible in most renderers but
// alter string identity, breaking naive equality checks and bytestring
// comparisons.
//
// Characters used:
//
//	U+200B ZERO WIDTH SPACE (ZWSP)            UTF-8: 0xE2 0x80 0x8B
//	U+200C ZERO WIDTH NON-JOINER (ZWNJ)       UTF-8: 0xE2 0x80 0x8C
//	U+200D ZERO WIDTH JOINER (ZWJ)            UTF-8: 0xE2 0x80 0x8D
//	U+FEFF ZERO WIDTH NO-BREAK SPACE / BOM    UTF-8: 0xEF 0xBB 0xBF

// ZeroWidthSP is the UTF-8 encoding of U+200B ZERO WIDTH SPACE.
var ZeroWidthSP = []byte{0xE2, 0x80, 0x8B}

// ZeroWidthNJ is the UTF-8 encoding of U+200C ZERO WIDTH NON-JOINER.
var ZeroWidthNJ = []byte{0xE2, 0x80, 0x8C}

// ZeroWidthJ is the UTF-8 encoding of U+200D ZERO WIDTH JOINER.
var ZeroWidthJ = []byte{0xE2, 0x80, 0x8D}

// ZeroWidthNBS is the UTF-8 encoding of U+FEFF ZERO WIDTH NO-BREAK SPACE (BOM).
var ZeroWidthNBS = []byte{0xEF, 0xBB, 0xBF}

// ZeroWidthChars returns a JSON payload where field names and values contain
// invisible zero-width Unicode characters.
//
// Fields:
//
//	"user<ZWSP>name":  "admin"            key has invisible ZWSP between 'user' and 'name'
//	"pass<ZWJ>word":   "secret<ZWNJ>!"   key has ZWJ; value has ZWNJ before '!'
//	"token":           "<BOM>abc<ZWSP>"  value starts with BOM, ends with ZWSP
func ZeroWidthChars() []byte {
	var buf bytes.Buffer

	// Key with ZWSP embedded: "user​name"
	buf.WriteString(`{"user`)
	buf.Write(ZeroWidthSP) // U+200B — invisible inside key
	buf.WriteString(`name":"admin"`)

	// Key with ZWJ embedded: "pass‍word"; value with ZWNJ: "secret‌!"
	buf.WriteString(`,"pass`)
	buf.Write(ZeroWidthJ) // U+200D — invisible inside key
	buf.WriteString(`word":"secret`)
	buf.Write(ZeroWidthNJ) // U+200C — invisible inside value
	buf.WriteString(`!"`)

	// Value starting with BOM and ending with ZWSP
	buf.WriteString(`,"token":"`)
	buf.Write(ZeroWidthNBS) // U+FEFF BOM — invisible at start of value
	buf.WriteString(`abc`)
	buf.Write(ZeroWidthSP) // U+200B — invisible at end of value
	buf.WriteByte('"')

	buf.WriteByte('}')

	return buf.Bytes()
}
