package encoding_test

import (
	"bytes"
	"testing"
	"unicode/utf8"

	"github.com/dwsmith1983/chaos-data/chaosdata/encoding"
)

// ---------------------------------------------------------------------------
// Registry
// ---------------------------------------------------------------------------

func TestInit_RegistersAllGenerators(t *testing.T) {
	want := []string{"BidiOverride", "HomoglyphStrings", "InvalidUTF8", "ZeroWidthChars"}

	got := encoding.Names()

	if len(got) != len(want) {
		t.Fatalf("Names() returned %d entries; want %d: got %v", len(got), len(want), got)
	}
	for i, name := range want {
		if got[i] != name {
			t.Errorf("Names()[%d] = %q; want %q", i, got[i], name)
		}
	}
}

func TestGet_ReturnsRegisteredGenerators(t *testing.T) {
	names := []string{"InvalidUTF8", "HomoglyphStrings", "BidiOverride", "ZeroWidthChars"}
	for _, name := range names {
		t.Run(name, func(t *testing.T) {
			g, ok := encoding.Get(name)
			if !ok {
				t.Fatalf("Get(%q) not found", name)
			}
			if g == nil {
				t.Fatalf("Get(%q) returned nil generator", name)
			}
		})
	}
}

func TestGet_MissingName(t *testing.T) {
	_, ok := encoding.Get("nonexistent")
	if ok {
		t.Error("Get(nonexistent) returned ok=true; want false")
	}
}

// ---------------------------------------------------------------------------
// InvalidUTF8
// ---------------------------------------------------------------------------

func TestInvalidUTF8_IsNotValidUTF8(t *testing.T) {
	payload := encoding.InvalidUTF8()
	if utf8.Valid(payload) {
		t.Error("InvalidUTF8() returned a valid UTF-8 sequence; want invalid")
	}
}

func TestInvalidUTF8_SurrogateHalfAtOffset(t *testing.T) {
	payload := encoding.InvalidUTF8()

	off := encoding.InvalidUTF8SurrogateOffset
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
	payload := encoding.InvalidUTF8()

	off := encoding.InvalidUTF8OverlongOffset
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
	payload := encoding.InvalidUTF8()

	off := encoding.InvalidUTF8TruncatedOffset
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
	a := encoding.InvalidUTF8()
	b := encoding.InvalidUTF8()
	if !bytes.Equal(a, b) {
		t.Error("InvalidUTF8() is not deterministic: two calls returned different results")
	}
}

func TestInvalidUTF8_NonEmpty(t *testing.T) {
	payload := encoding.InvalidUTF8()
	if len(payload) == 0 {
		t.Error("InvalidUTF8() returned empty payload")
	}
}

// Verify the three distinct invalid sequences are all present via raw byte
// search, independent of the exported offset constants.
func TestInvalidUTF8_ContainsAllThreeSequences(t *testing.T) {
	payload := encoding.InvalidUTF8()

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

// ---------------------------------------------------------------------------
// HomoglyphStrings
// ---------------------------------------------------------------------------

func TestHomoglyphStrings_IsValidUTF8(t *testing.T) {
	payload := encoding.HomoglyphStrings()
	if !utf8.Valid(payload) {
		t.Error("HomoglyphStrings() returned invalid UTF-8; homoglyphs must be valid Unicode")
	}
}

func TestHomoglyphStrings_ContainsCyrillicA(t *testing.T) {
	payload := encoding.HomoglyphStrings()
	if !bytes.Contains(payload, encoding.HomoglyphCyrillicA) {
		t.Errorf("payload does not contain Cyrillic а (U+0430, %#v)", encoding.HomoglyphCyrillicA)
	}
}

func TestHomoglyphStrings_ContainsCyrillicE(t *testing.T) {
	payload := encoding.HomoglyphStrings()
	if !bytes.Contains(payload, encoding.HomoglyphCyrillicE) {
		t.Errorf("payload does not contain Cyrillic е (U+0435, %#v)", encoding.HomoglyphCyrillicE)
	}
}

func TestHomoglyphStrings_ContainsCyrillicO(t *testing.T) {
	payload := encoding.HomoglyphStrings()
	if !bytes.Contains(payload, encoding.HomoglyphCyrillicO) {
		t.Errorf("payload does not contain Cyrillic о (U+043E, %#v)", encoding.HomoglyphCyrillicO)
	}
}

func TestHomoglyphStrings_ContainsCyrillicP(t *testing.T) {
	payload := encoding.HomoglyphStrings()
	if !bytes.Contains(payload, encoding.HomoglyphCyrillicP) {
		t.Errorf("payload does not contain Cyrillic р (U+0440, %#v)", encoding.HomoglyphCyrillicP)
	}
}

func TestHomoglyphStrings_ContainsCyrillicC(t *testing.T) {
	payload := encoding.HomoglyphStrings()
	if !bytes.Contains(payload, encoding.HomoglyphCyrillicC) {
		t.Errorf("payload does not contain Cyrillic с (U+0441, %#v)", encoding.HomoglyphCyrillicC)
	}
}

// Verify the username value starts with the Cyrillic homoglyph, not ASCII 'a'.
func TestHomoglyphStrings_DoesNotEqualNaiveASCII(t *testing.T) {
	payload := encoding.HomoglyphStrings()

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
	// Must start with Cyrillic а (0xD0 0xB0), not ASCII a (0x61).
	if payload[valueStart] == 0x61 {
		t.Error(`username value starts with ASCII 'a' (0x61); want Cyrillic а (0xD0 0xB0)`)
	}
	// Plain ASCII "admin" must not appear at the start of the value.
	window := len(asciiAdmin)
	if valueStart+window <= len(payload) {
		if bytes.Equal(payload[valueStart:valueStart+window], asciiAdmin) {
			t.Error("username value contains plain ASCII 'admin'; homoglyph substitution not applied")
		}
	}
}

func TestHomoglyphStrings_Determinism(t *testing.T) {
	a := encoding.HomoglyphStrings()
	b := encoding.HomoglyphStrings()
	if !bytes.Equal(a, b) {
		t.Error("HomoglyphStrings() is not deterministic")
	}
}

func TestHomoglyphStrings_NonEmpty(t *testing.T) {
	if len(encoding.HomoglyphStrings()) == 0 {
		t.Error("HomoglyphStrings() returned empty payload")
	}
}

// ---------------------------------------------------------------------------
// BidiOverride
// ---------------------------------------------------------------------------

func TestBidiOverride_IsValidUTF8(t *testing.T) {
	payload := encoding.BidiOverride()
	if !utf8.Valid(payload) {
		t.Error("BidiOverride() returned invalid UTF-8; bidi control chars must be valid Unicode")
	}
}

func TestBidiOverride_ContainsRLO(t *testing.T) {
	payload := encoding.BidiOverride()
	if !bytes.Contains(payload, encoding.BidiRLO) {
		t.Errorf("payload does not contain RLO (U+202E, %#v)", encoding.BidiRLO)
	}
}

func TestBidiOverride_ContainsLRO(t *testing.T) {
	payload := encoding.BidiOverride()
	if !bytes.Contains(payload, encoding.BidiLRO) {
		t.Errorf("payload does not contain LRO (U+202D, %#v)", encoding.BidiLRO)
	}
}

func TestBidiOverride_ContainsRLM(t *testing.T) {
	payload := encoding.BidiOverride()
	if !bytes.Contains(payload, encoding.BidiRLM) {
		t.Errorf("payload does not contain RLM (U+200F, %#v)", encoding.BidiRLM)
	}
}

func TestBidiOverride_ContainsPDF(t *testing.T) {
	payload := encoding.BidiOverride()
	if !bytes.Contains(payload, encoding.BidiPDF) {
		t.Errorf("payload does not contain PDF (U+202C, %#v)", encoding.BidiPDF)
	}
}

func TestBidiOverride_RLOBeforeFilename(t *testing.T) {
	payload := encoding.BidiOverride()
	key := []byte(`"filename":"`)
	idx := bytes.Index(payload, key)
	if idx == -1 {
		t.Fatal(`payload does not contain "filename":`)
	}
	valueStart := idx + len(key)
	if !bytes.HasPrefix(payload[valueStart:], encoding.BidiRLO) {
		t.Errorf("filename value does not start with RLO; got %#v", payload[valueStart:valueStart+3])
	}
}

func TestBidiOverride_Determinism(t *testing.T) {
	a := encoding.BidiOverride()
	b := encoding.BidiOverride()
	if !bytes.Equal(a, b) {
		t.Error("BidiOverride() is not deterministic")
	}
}

func TestBidiOverride_NonEmpty(t *testing.T) {
	if len(encoding.BidiOverride()) == 0 {
		t.Error("BidiOverride() returned empty payload")
	}
}

// ---------------------------------------------------------------------------
// ZeroWidthChars
// ---------------------------------------------------------------------------

func TestZeroWidthChars_IsValidUTF8(t *testing.T) {
	payload := encoding.ZeroWidthChars()
	if !utf8.Valid(payload) {
		t.Error("ZeroWidthChars() returned invalid UTF-8; zero-width chars must be valid Unicode")
	}
}

func TestZeroWidthChars_ContainsZWSPInKey(t *testing.T) {
	payload := encoding.ZeroWidthChars()
	// The key "user<ZWSP>name" must appear in the payload.
	want := append(append([]byte(`"user`), encoding.ZeroWidthSP...), []byte(`name"`)...)
	if !bytes.Contains(payload, want) {
		t.Errorf("payload does not contain ZWSP in username key; want subsequence %#v", want)
	}
}

func TestZeroWidthChars_ContainsZWJInKey(t *testing.T) {
	payload := encoding.ZeroWidthChars()
	// The key "pass<ZWJ>word" must appear in the payload.
	want := append(append([]byte(`"pass`), encoding.ZeroWidthJ...), []byte(`word"`)...)
	if !bytes.Contains(payload, want) {
		t.Errorf("payload does not contain ZWJ in password key; want subsequence %#v", want)
	}
}

func TestZeroWidthChars_ContainsZWNJInValue(t *testing.T) {
	payload := encoding.ZeroWidthChars()
	if !bytes.Contains(payload, encoding.ZeroWidthNJ) {
		t.Errorf("payload does not contain ZWNJ (U+200C, %#v)", encoding.ZeroWidthNJ)
	}
}

func TestZeroWidthChars_ContainsBOMInValue(t *testing.T) {
	payload := encoding.ZeroWidthChars()
	if !bytes.Contains(payload, encoding.ZeroWidthNBS) {
		t.Errorf("payload does not contain BOM (U+FEFF, %#v)", encoding.ZeroWidthNBS)
	}
}

func TestZeroWidthChars_TokenValueStartsWithBOM(t *testing.T) {
	payload := encoding.ZeroWidthChars()
	key := []byte(`"token":"`)
	idx := bytes.Index(payload, key)
	if idx == -1 {
		t.Fatal(`payload does not contain "token":`)
	}
	valueStart := idx + len(key)
	if !bytes.HasPrefix(payload[valueStart:], encoding.ZeroWidthNBS) {
		t.Errorf("token value does not start with BOM; got bytes %#v", payload[valueStart:valueStart+3])
	}
}

func TestZeroWidthChars_ContainsZWSP(t *testing.T) {
	payload := encoding.ZeroWidthChars()
	if !bytes.Contains(payload, encoding.ZeroWidthSP) {
		t.Errorf("payload does not contain ZWSP (U+200B, %#v)", encoding.ZeroWidthSP)
	}
}

func TestZeroWidthChars_Determinism(t *testing.T) {
	a := encoding.ZeroWidthChars()
	b := encoding.ZeroWidthChars()
	if !bytes.Equal(a, b) {
		t.Error("ZeroWidthChars() is not deterministic")
	}
}

func TestZeroWidthChars_NonEmpty(t *testing.T) {
	if len(encoding.ZeroWidthChars()) == 0 {
		t.Error("ZeroWidthChars() returned empty payload")
	}
}
