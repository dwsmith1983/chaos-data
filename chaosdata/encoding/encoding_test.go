package encoding

import (
	"bytes"
	"encoding/json"
	"testing"
	"unicode/utf8"

	"github.com/dwsmith1983/chaos-data/chaosdata"
)

// EncodingGenerator tests

func TestEncodingGenerator_Category(t *testing.T) {
	gen := &EncodingGenerator{}
	if gen.Category() != "encoding" {
		t.Errorf("expected 'encoding', got '%s'", gen.Category())
	}
}

func TestEncodingGenerator_Generate(t *testing.T) {
	gen := &EncodingGenerator{}
	vals, err := gen.Generate(chaosdata.GenerateOpts{Count: 1})
	if err != nil {
		t.Fatalf("Generate() err = %v", err)
	}

	expectedDesc := []string{
		"Invalid UTF-8 byte sequences",
		"UTF-8 surrogate half (U+D800)",
		"UTF-8 overlong NUL encoding",
		"UTF-8 truncated 3-byte sequence",
		"BOM markers (UTF-8)",
		"BOM markers (UTF-16 LE)",
		"BOM markers (UTF-16 BE)",
		"Homoglyph - Cyrillic а (looks like Latin a)",
		"Homoglyph - Visual example (Cyrillic homoglyphs)",
		"Homoglyph - Cyrillic с о р (looks like core)",
		"Bidi override - RLO (renders as 'exe.txt')",
		"Bidi override - RLM marker",
		"Bidi override - LRO",
		"Zero-width ZWSP (invisible space)",
		"Zero-width ZWJ (joiner)",
		"Zero-width ZWNJ (non-joiner)",
		"Zero-width BOM at start",
		"mixed encoding strings",
		"Base64 padding edge cases",
		"JSON snippet resembling encoding",
		"XML snippet resembling encoding",
	}

	found := make(map[string]bool)
	var parsed []map[string]any
	if err := json.Unmarshal(vals.Data, &parsed); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}
	for _, v := range parsed {
		if typ, ok := v["type"].(string); ok {
			found[typ] = true
		}
	}

	for _, desc := range expectedDesc {
		if !found[desc] {
			t.Errorf("Missing expected chaos value with description: %s", desc)
		}
	}
}

// Low-level byte generator tests

func TestInvalidUTF8_IsNotValidUTF8(t *testing.T) {
	payload := InvalidUTF8()
	if utf8.Valid(payload) {
		t.Error("InvalidUTF8() returned a valid UTF-8 sequence; want invalid")
	}
}

func TestInvalidUTF8_SurrogateHalfAtOffset(t *testing.T) {
	payload := InvalidUTF8()

	off := InvalidUTF8SurrogateOffset
	want := []byte{0xED, 0xA0, 0x80}

	if len(payload) < off+len(want) {
		t.Fatalf("payload too short: len=%d, need at least %d", len(payload), off+len(want))
	}
	got := payload[off : off+len(want)]
	if !bytes.Equal(got, want) {
		t.Errorf("bytes at surrogate offset %d = %#v; want %#v", off, got, want)
	}
}

func TestInvalidUTF8_OverlongAtOffset(t *testing.T) {
	payload := InvalidUTF8()

	off := InvalidUTF8OverlongOffset
	want := []byte{0xC0, 0x80}

	if len(payload) < off+len(want) {
		t.Fatalf("payload too short: len=%d, need at least %d", len(payload), off+len(want))
	}
	got := payload[off : off+len(want)]
	if !bytes.Equal(got, want) {
		t.Errorf("bytes at overlong offset %d = %#v; want %#v", off, got, want)
	}
}

func TestInvalidUTF8_TruncatedAtOffset(t *testing.T) {
	payload := InvalidUTF8()

	off := InvalidUTF8TruncatedOffset
	want := []byte{0xE2, 0x82}

	if len(payload) < off+len(want) {
		t.Fatalf("payload too short: len=%d, need at least %d", len(payload), off+len(want))
	}
	got := payload[off : off+len(want)]
	if !bytes.Equal(got, want) {
		t.Errorf("bytes at truncated offset %d = %#v; want %#v", off, got, want)
	}
}

func TestInvalidUTF8_Determinism(t *testing.T) {
	a := InvalidUTF8()
	b := InvalidUTF8()
	if !bytes.Equal(a, b) {
		t.Error("InvalidUTF8() is not deterministic: two calls returned different results")
	}
}

func TestInvalidUTF8_NonEmpty(t *testing.T) {
	payload := InvalidUTF8()
	if len(payload) == 0 {
		t.Error("InvalidUTF8() returned empty payload")
	}
}

func TestInvalidUTF8_ContainsAllThreeSequences(t *testing.T) {
	payload := InvalidUTF8()

	tests := []struct {
		name string
		seq  []byte
	}{
		{"surrogate half U+D800", []byte{0xED, 0xA0, 0x80}},
		{"overlong NUL", []byte{0xC0, 0x80}},
		{"truncated 3-byte", []byte{0xE2, 0x82}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if !bytes.Contains(payload, tt.seq) {
				t.Errorf("payload does not contain %s sequence %#v", tt.name, tt.seq)
			}
		})
	}
}

func TestHomoglyphStrings_IsValidUTF8(t *testing.T) {
	payload := HomoglyphStrings()
	if !utf8.Valid(payload) {
		t.Error("HomoglyphStrings() returned invalid UTF-8; homoglyphs must be valid Unicode")
	}
}

func TestHomoglyphStrings_ContainsCyrillicA(t *testing.T) {
	payload := HomoglyphStrings()
	if !bytes.Contains(payload, HomoglyphCyrillicA) {
		t.Errorf("payload does not contain Cyrillic а (U+0430, %#v)", HomoglyphCyrillicA)
	}
}

func TestHomoglyphStrings_ContainsCyrillicE(t *testing.T) {
	payload := HomoglyphStrings()
	if !bytes.Contains(payload, HomoglyphCyrillicE) {
		t.Errorf("payload does not contain Cyrillic е (U+0435, %#v)", HomoglyphCyrillicE)
	}
}

func TestHomoglyphStrings_ContainsCyrillicO(t *testing.T) {
	payload := HomoglyphStrings()
	if !bytes.Contains(payload, HomoglyphCyrillicO) {
		t.Errorf("payload does not contain Cyrillic о (U+043E, %#v)", HomoglyphCyrillicO)
	}
}

func TestHomoglyphStrings_ContainsCyrillicP(t *testing.T) {
	payload := HomoglyphStrings()
	if !bytes.Contains(payload, HomoglyphCyrillicP) {
		t.Errorf("payload does not contain Cyrillic р (U+0440, %#v)", HomoglyphCyrillicP)
	}
}

func TestHomoglyphStrings_ContainsCyrillicC(t *testing.T) {
	payload := HomoglyphStrings()
	if !bytes.Contains(payload, HomoglyphCyrillicC) {
		t.Errorf("payload does not contain Cyrillic с (U+0441, %#v)", HomoglyphCyrillicC)
	}
}

func TestHomoglyphStrings_DoesNotEqualNaiveASCII(t *testing.T) {
	payload := HomoglyphStrings()

	asciiAdmin := []byte("admin")
	key := []byte(`"username":"`)
	idx := bytes.Index(payload, key)
	if idx == -1 {
		t.Fatal(`payload does not contain "username":`)
	}
	valueStart := idx + len(key)
	if valueStart >= len(payload) {
		t.Fatal("payload truncated after username key")
	}
	if payload[valueStart] == 0x61 {
		t.Error(`username value starts with ASCII 'a' (0x61); want Cyrillic а (0xD0 0xB0)`)
	}
	window := len(asciiAdmin)
	if valueStart+window <= len(payload) {
		if bytes.Equal(payload[valueStart:valueStart+window], asciiAdmin) {
			t.Error("username value contains plain ASCII 'admin'; homoglyph substitution not applied")
		}
	}
}

func TestHomoglyphStrings_Determinism(t *testing.T) {
	a := HomoglyphStrings()
	b := HomoglyphStrings()
	if !bytes.Equal(a, b) {
		t.Error("HomoglyphStrings() is not deterministic")
	}
}

func TestHomoglyphStrings_NonEmpty(t *testing.T) {
	if len(HomoglyphStrings()) == 0 {
		t.Error("HomoglyphStrings() returned empty payload")
	}
}

func TestBidiOverride_IsValidUTF8(t *testing.T) {
	payload := BidiOverride()
	if !utf8.Valid(payload) {
		t.Error("BidiOverride() returned invalid UTF-8; bidi control chars must be valid Unicode")
	}
}

func TestBidiOverride_ContainsRLO(t *testing.T) {
	payload := BidiOverride()
	if !bytes.Contains(payload, BidiRLO) {
		t.Errorf("payload does not contain RLO (U+202E, %#v)", BidiRLO)
	}
}

func TestBidiOverride_ContainsLRO(t *testing.T) {
	payload := BidiOverride()
	if !bytes.Contains(payload, BidiLRO) {
		t.Errorf("payload does not contain LRO (U+202D, %#v)", BidiLRO)
	}
}

func TestBidiOverride_ContainsRLM(t *testing.T) {
	payload := BidiOverride()
	if !bytes.Contains(payload, BidiRLM) {
		t.Errorf("payload does not contain RLM (U+200F, %#v)", BidiRLM)
	}
}

func TestBidiOverride_ContainsPDF(t *testing.T) {
	payload := BidiOverride()
	if !bytes.Contains(payload, BidiPDF) {
		t.Errorf("payload does not contain PDF (U+202C, %#v)", BidiPDF)
	}
}

func TestBidiOverride_RLOBeforeFilename(t *testing.T) {
	payload := BidiOverride()
	key := []byte(`"filename":"`)
	idx := bytes.Index(payload, key)
	if idx == -1 {
		t.Fatal(`payload does not contain "filename":`)
	}
	valueStart := idx + len(key)
	if !bytes.HasPrefix(payload[valueStart:], BidiRLO) {
		t.Errorf("filename value does not start with RLO; got %#v", payload[valueStart:valueStart+3])
	}
}

func TestBidiOverride_Determinism(t *testing.T) {
	a := BidiOverride()
	b := BidiOverride()
	if !bytes.Equal(a, b) {
		t.Error("BidiOverride() is not deterministic")
	}
}

func TestBidiOverride_NonEmpty(t *testing.T) {
	if len(BidiOverride()) == 0 {
		t.Error("BidiOverride() returned empty payload")
	}
}

func TestZeroWidthChars_IsValidUTF8(t *testing.T) {
	payload := ZeroWidthChars()
	if !utf8.Valid(payload) {
		t.Error("ZeroWidthChars() returned invalid UTF-8; zero-width chars must be valid Unicode")
	}
}

func TestZeroWidthChars_ContainsZWSPInKey(t *testing.T) {
	payload := ZeroWidthChars()
	want := append(append([]byte(`"user`), ZeroWidthSP...), []byte(`name"`)...)
	if !bytes.Contains(payload, want) {
		t.Errorf("payload does not contain ZWSP in username key; want subsequence %#v", want)
	}
}

func TestZeroWidthChars_ContainsZWJInKey(t *testing.T) {
	payload := ZeroWidthChars()
	want := append(append([]byte(`"pass`), ZeroWidthJ...), []byte(`word"`)...)
	if !bytes.Contains(payload, want) {
		t.Errorf("payload does not contain ZWJ in password key; want subsequence %#v", want)
	}
}

func TestZeroWidthChars_ContainsZWNJInValue(t *testing.T) {
	payload := ZeroWidthChars()
	if !bytes.Contains(payload, ZeroWidthNJ) {
		t.Errorf("payload does not contain ZWNJ (U+200C, %#v)", ZeroWidthNJ)
	}
}

func TestZeroWidthChars_ContainsBOMInValue(t *testing.T) {
	payload := ZeroWidthChars()
	if !bytes.Contains(payload, ZeroWidthNBS) {
		t.Errorf("payload does not contain BOM (U+FEFF, %#v)", ZeroWidthNBS)
	}
}

func TestZeroWidthChars_TokenValueStartsWithBOM(t *testing.T) {
	payload := ZeroWidthChars()
	key := []byte(`"token":"`)
	idx := bytes.Index(payload, key)
	if idx == -1 {
		t.Fatal(`payload does not contain "token":`)
	}
	valueStart := idx + len(key)
	if !bytes.HasPrefix(payload[valueStart:], ZeroWidthNBS) {
		t.Errorf("token value does not start with BOM; got bytes %#v", payload[valueStart:valueStart+3])
	}
}

func TestZeroWidthChars_ContainsZWSP(t *testing.T) {
	payload := ZeroWidthChars()
	if !bytes.Contains(payload, ZeroWidthSP) {
		t.Errorf("payload does not contain ZWSP (U+200B, %#v)", ZeroWidthSP)
	}
}

func TestZeroWidthChars_Determinism(t *testing.T) {
	a := ZeroWidthChars()
	b := ZeroWidthChars()
	if !bytes.Equal(a, b) {
		t.Error("ZeroWidthChars() is not deterministic")
	}
}

func TestZeroWidthChars_NonEmpty(t *testing.T) {
	if len(ZeroWidthChars()) == 0 {
		t.Error("ZeroWidthChars() returned empty payload")
	}
}

func FuzzEncodingGenerator_Helpers(f *testing.F) {
	f.Add([]byte("test"))
	f.Fuzz(func(t *testing.T, b []byte) {
		// Valid fuzzer to ensure no panics
	})
}
