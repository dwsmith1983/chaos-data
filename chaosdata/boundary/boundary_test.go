package boundary_test

import (
	"encoding/json"
	"fmt"
	"math"
	"testing"

	"github.com/dwsmith1983/chaos-data/chaosdata"
	"github.com/dwsmith1983/chaos-data/chaosdata/boundary"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func requireNoError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// decodePayload unmarshals a Payload's Data field into a slice of record maps.
func decodePayload(t *testing.T, p chaosdata.Payload) []map[string]interface{} {
	t.Helper()
	var records []map[string]interface{}
	if err := json.Unmarshal(p.Data, &records); err != nil {
		t.Fatalf("failed to unmarshal payload: %v", err)
	}
	return records
}

// indexByType builds a map from record "type" → record for fast lookup.
func indexByType(records []map[string]interface{}) map[string]map[string]interface{} {
	idx := make(map[string]map[string]interface{}, len(records))
	for _, r := range records {
		if typ, ok := r["type"].(string); ok {
			idx[typ] = r
		}
	}
	return idx
}

// ---------------------------------------------------------------------------
// Interface compliance (compile-time)
// ---------------------------------------------------------------------------

var _ chaosdata.ChaosGenerator = boundary.ChaosGenerator{}

// ---------------------------------------------------------------------------
// Metadata
// ---------------------------------------------------------------------------

func TestName(t *testing.T) {
	g := boundary.ChaosGenerator{}
	if g.Name() != "boundary" {
		t.Errorf("Name() = %q; want %q", g.Name(), "boundary")
	}
}

func TestCategory(t *testing.T) {
	g := boundary.ChaosGenerator{}
	if g.Category() != "boundary" {
		t.Errorf("Category() = %q; want %q", g.Category(), "boundary")
	}
}

// ---------------------------------------------------------------------------
// Generate — basic shape
// ---------------------------------------------------------------------------

func TestGenerate_ReturnsNonEmptyPayload(t *testing.T) {
	g := boundary.ChaosGenerator{}
	p, err := g.Generate(chaosdata.GenerateOpts{Count: 1})
	requireNoError(t, err)

	if len(p.Data) == 0 {
		t.Error("expected non-empty Data")
	}
	if p.Type != "application/json" {
		t.Errorf("Type = %q; want %q", p.Type, "application/json")
	}
	if p.Attributes["generator"] != "boundary" {
		t.Errorf("Attributes[generator] = %q; want %q", p.Attributes["generator"], "boundary")
	}
}

func TestGenerate_CountZeroDefaultsToOne(t *testing.T) {
	g := boundary.ChaosGenerator{}
	p0, err := g.Generate(chaosdata.GenerateOpts{Count: 0})
	requireNoError(t, err)
	p1, err := g.Generate(chaosdata.GenerateOpts{Count: 1})
	requireNoError(t, err)

	records0 := decodePayload(t, p0)
	records1 := decodePayload(t, p1)
	if len(records0) != len(records1) {
		t.Errorf("Count=0 produced %d records; want %d (same as Count=1)", len(records0), len(records1))
	}
}

func TestGenerate_CountMultipliesRecords(t *testing.T) {
	g := boundary.ChaosGenerator{}
	p1, _ := g.Generate(chaosdata.GenerateOpts{Count: 1})
	p3, _ := g.Generate(chaosdata.GenerateOpts{Count: 3})

	r1 := decodePayload(t, p1)
	r3 := decodePayload(t, p3)
	if len(r3) != 3*len(r1) {
		t.Errorf("Count=3 produced %d records; want %d (3×%d)", len(r3), 3*len(r1), len(r1))
	}
}

// ---------------------------------------------------------------------------
// Signed integer boundary presence
// ---------------------------------------------------------------------------

func TestGenerate_Int8Boundaries(t *testing.T) {
	g := boundary.ChaosGenerator{}
	p, err := g.Generate(chaosdata.GenerateOpts{Count: 1})
	requireNoError(t, err)
	idx := indexByType(decodePayload(t, p))

	tests := []struct {
		typ  string
		want float64
	}{
		{"int8_min", float64(math.MinInt8)},
		{"int8_max", float64(math.MaxInt8)},
		{"int8_min_plus1", float64(math.MinInt8 + 1)},
		{"int8_max_minus1", float64(math.MaxInt8 - 1)},
	}
	for _, tt := range tests {
		t.Run(tt.typ, func(t *testing.T) {
			r, ok := idx[tt.typ]
			if !ok {
				t.Fatalf("record %q not found in payload", tt.typ)
			}
			got, ok := r["value"].(float64)
			if !ok {
				t.Fatalf("value for %q is not a number: %T %v", tt.typ, r["value"], r["value"])
			}
			if got != tt.want {
				t.Errorf("value = %v; want %v", got, tt.want)
			}
		})
	}
}

func TestGenerate_Int16Boundaries(t *testing.T) {
	g := boundary.ChaosGenerator{}
	p, _ := g.Generate(chaosdata.GenerateOpts{Count: 1})
	idx := indexByType(decodePayload(t, p))

	tests := []struct {
		typ  string
		want float64
	}{
		{"int16_min", float64(math.MinInt16)},
		{"int16_max", float64(math.MaxInt16)},
		{"int16_min_plus1", float64(math.MinInt16 + 1)},
		{"int16_max_minus1", float64(math.MaxInt16 - 1)},
	}
	for _, tt := range tests {
		t.Run(tt.typ, func(t *testing.T) {
			r, ok := idx[tt.typ]
			if !ok {
				t.Fatalf("record %q not found", tt.typ)
			}
			if r["value"].(float64) != tt.want {
				t.Errorf("value = %v; want %v", r["value"], tt.want)
			}
		})
	}
}

func TestGenerate_Int32Boundaries(t *testing.T) {
	g := boundary.ChaosGenerator{}
	p, _ := g.Generate(chaosdata.GenerateOpts{Count: 1})
	idx := indexByType(decodePayload(t, p))

	tests := []struct {
		typ  string
		want float64
	}{
		{"int32_min", float64(math.MinInt32)},
		{"int32_max", float64(math.MaxInt32)},
		{"int32_min_plus1", float64(math.MinInt32 + 1)},
		{"int32_max_minus1", float64(math.MaxInt32 - 1)},
	}
	for _, tt := range tests {
		t.Run(tt.typ, func(t *testing.T) {
			r, ok := idx[tt.typ]
			if !ok {
				t.Fatalf("record %q not found", tt.typ)
			}
			if r["value"].(float64) != tt.want {
				t.Errorf("value = %v; want %v", r["value"], tt.want)
			}
		})
	}
}

func TestGenerate_Int64Boundaries(t *testing.T) {
	g := boundary.ChaosGenerator{}
	p, _ := g.Generate(chaosdata.GenerateOpts{Count: 1})
	idx := indexByType(decodePayload(t, p))

	// int64 extremes exceed JSON float64 precision; verify presence and numeric type.
	tests := []struct {
		typ string
	}{
		{"int64_min"},
		{"int64_max"},
		{"int64_min_plus1"},
		{"int64_max_minus1"},
	}
	for _, tt := range tests {
		t.Run(tt.typ, func(t *testing.T) {
			r, ok := idx[tt.typ]
			if !ok {
				t.Fatalf("record %q not found", tt.typ)
			}
			if _, ok := r["value"].(float64); !ok {
				t.Errorf("expected float64 value for %q, got %T", tt.typ, r["value"])
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Unsigned boundary presence
// ---------------------------------------------------------------------------

func TestGenerate_UintBoundaries(t *testing.T) {
	g := boundary.ChaosGenerator{}
	p, _ := g.Generate(chaosdata.GenerateOpts{Count: 1})
	idx := indexByType(decodePayload(t, p))

	tests := []struct {
		typ  string
		want float64
	}{
		{"uint8_max", float64(math.MaxUint8)},
		{"uint8_max_minus1", float64(math.MaxUint8 - 1)},
		{"uint16_max", float64(math.MaxUint16)},
		{"uint16_max_minus1", float64(math.MaxUint16 - 1)},
		{"uint32_max", float64(math.MaxUint32)},
		{"uint32_max_minus1", float64(math.MaxUint32 - 1)},
	}
	for _, tt := range tests {
		t.Run(tt.typ, func(t *testing.T) {
			r, ok := idx[tt.typ]
			if !ok {
				t.Fatalf("record %q not found", tt.typ)
			}
			if r["value"].(float64) != tt.want {
				t.Errorf("value = %v; want %v", r["value"], tt.want)
			}
		})
	}
}

func TestGenerate_Uint64BoundariesAsStrings(t *testing.T) {
	g := boundary.ChaosGenerator{}
	p, _ := g.Generate(chaosdata.GenerateOpts{Count: 1})
	idx := indexByType(decodePayload(t, p))

	tests := []struct {
		typ  string
		want string
	}{
		{"uint64_min", "0"},
		{"uint64_min_plus1", "1"},
		{"uint64_max", "18446744073709551615"},
		{"uint64_max_minus1", "18446744073709551614"},
	}
	for _, tt := range tests {
		t.Run(tt.typ, func(t *testing.T) {
			r, ok := idx[tt.typ]
			if !ok {
				t.Fatalf("record %q not found", tt.typ)
			}
			got, ok := r["value"].(string)
			if !ok {
				t.Fatalf("expected string value for %q, got %T", tt.typ, r["value"])
			}
			if got != tt.want {
				t.Errorf("value = %q; want %q", got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Powers of two ±1
// ---------------------------------------------------------------------------

func TestGenerate_PowersOfTwo(t *testing.T) {
	g := boundary.ChaosGenerator{}
	p, _ := g.Generate(chaosdata.GenerateOpts{Count: 1})
	idx := indexByType(decodePayload(t, p))

	cases := []struct {
		typ  string
		want float64
	}{
		{"pow2_1", 2},
		{"pow2_1_minus1", 1},
		{"pow2_1_plus1", 3},
		{"pow2_8", 256},
		{"pow2_8_minus1", 255},
		{"pow2_8_plus1", 257},
		{"pow2_16", 65536},
		{"pow2_16_minus1", 65535},
		{"pow2_16_plus1", 65537},
		{"pow2_32", float64(int64(1) << 32)},
		{"pow2_32_minus1", float64(int64(1)<<32 - 1)},
		{"pow2_32_plus1", float64(int64(1)<<32 + 1)},
	}
	for _, tt := range cases {
		t.Run(tt.typ, func(t *testing.T) {
			r, ok := idx[tt.typ]
			if !ok {
				t.Fatalf("record %q not found", tt.typ)
			}
			if r["value"].(float64) != tt.want {
				t.Errorf("value = %v; want %v", r["value"], tt.want)
			}
		})
	}
}

func TestGenerate_PowersOfTwoPresent_Exp1To63(t *testing.T) {
	g := boundary.ChaosGenerator{}
	p, _ := g.Generate(chaosdata.GenerateOpts{Count: 1})
	idx := indexByType(decodePayload(t, p))

	for exp := 1; exp <= 63; exp++ {
		key := fmt.Sprintf("pow2_%d", exp)
		if _, ok := idx[key]; !ok {
			t.Errorf("record %q not found in payload", key)
		}
		keyM := fmt.Sprintf("pow2_%d_minus1", exp)
		if _, ok := idx[keyM]; !ok {
			t.Errorf("record %q not found in payload", keyM)
		}
		if exp < 63 {
			keyP := fmt.Sprintf("pow2_%d_plus1", exp)
			if _, ok := idx[keyP]; !ok {
				t.Errorf("record %q not found in payload", keyP)
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Leap year dates
// ---------------------------------------------------------------------------

func TestGenerate_LeapYearDates(t *testing.T) {
	g := boundary.ChaosGenerator{}
	p, _ := g.Generate(chaosdata.GenerateOpts{Count: 1})
	idx := indexByType(decodePayload(t, p))

	tests := []struct {
		typ  string
		want string
	}{
		{"leap_feb29_2000", "2000-02-29"},
		{"leap_feb29_2024", "2024-02-29"},
		{"leap_feb28_2020", "2020-02-28"},
		{"leap_mar01_2016", "2016-03-01"},
		{"non_leap_feb28_2023", "2023-02-28"},
		{"non_leap_mar01_2023", "2023-03-01"},
		{"non_leap_invalid_feb29_2023", "2023-02-29"},
	}
	for _, tt := range tests {
		t.Run(tt.typ, func(t *testing.T) {
			r, ok := idx[tt.typ]
			if !ok {
				t.Fatalf("record %q not found", tt.typ)
			}
			got, ok := r["value"].(string)
			if !ok {
				t.Fatalf("expected string value for %q, got %T", tt.typ, r["value"])
			}
			if got != tt.want {
				t.Errorf("value = %q; want %q", got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Epoch timestamps
// ---------------------------------------------------------------------------

func TestGenerate_EpochTimestamps(t *testing.T) {
	g := boundary.ChaosGenerator{}
	p, _ := g.Generate(chaosdata.GenerateOpts{Count: 1})
	idx := indexByType(decodePayload(t, p))

	tests := []struct {
		typ  string
		want float64
	}{
		{"epoch_zero", 0},
		{"epoch_minus1", -1},
		{"epoch_int32_max_y2038", float64(math.MaxInt32)},
		{"epoch_int32_max_plus1", float64(math.MaxInt32 + 1)},
		{"epoch_int32_min", float64(math.MinInt32)},
	}
	for _, tt := range tests {
		t.Run(tt.typ, func(t *testing.T) {
			r, ok := idx[tt.typ]
			if !ok {
				t.Fatalf("record %q not found", tt.typ)
			}
			if r["value"].(float64) != tt.want {
				t.Errorf("value = %v; want %v", r["value"], tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Timezone edge cases
// ---------------------------------------------------------------------------

func TestGenerate_TimezoneEdgeCases(t *testing.T) {
	g := boundary.ChaosGenerator{}
	p, _ := g.Generate(chaosdata.GenerateOpts{Count: 1})
	idx := indexByType(decodePayload(t, p))

	tests := []struct {
		typ  string
		want string
	}{
		{"tz_utc", "2024-01-15T12:00:00+00:00"},
		{"tz_utc_plus12", "2024-01-15T12:00:00+12:00"},
		{"tz_utc_minus12", "2024-01-15T12:00:00-12:00"},
		{"tz_utc_plus1330", "2024-01-15T12:00:00+13:30"},
		{"tz_utc_minus1130", "2024-01-15T12:00:00-11:30"},
		{"tz_utc_plus0530", "2024-01-15T12:00:00+05:30"},
		{"tz_utc_plus0545", "2024-01-15T12:00:00+05:45"},
		{"tz_utc_plus0630", "2024-01-15T12:00:00+06:30"},
		{"tz_utc_plus0930", "2024-01-15T12:00:00+09:30"},
		{"tz_utc_plus1030", "2024-01-15T12:00:00+10:30"},
		{"tz_utc_minus0930", "2024-01-15T12:00:00-09:30"},
		{"tz_utc_minus0330", "2024-01-15T12:00:00-03:30"},
	}
	for _, tt := range tests {
		t.Run(tt.typ, func(t *testing.T) {
			r, ok := idx[tt.typ]
			if !ok {
				t.Fatalf("record %q not found", tt.typ)
			}
			got, ok := r["value"].(string)
			if !ok {
				t.Fatalf("expected string value for %q, got %T", tt.typ, r["value"])
			}
			if got != tt.want {
				t.Errorf("value = %q; want %q", got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// String length boundaries
// ---------------------------------------------------------------------------

func TestGenerate_StringLengthBoundaries(t *testing.T) {
	g := boundary.ChaosGenerator{}
	p, _ := g.Generate(chaosdata.GenerateOpts{Count: 1})
	idx := indexByType(decodePayload(t, p))

	tests := []struct {
		typ  string
		wlen int
	}{
		{"string_len_0", 0},
		{"string_len_1", 1},
		{"string_len_127", 127},
		{"string_len_128", 128},
		{"string_len_254", 254},
		{"string_len_255", 255},
		{"string_len_256", 256},
		{"string_len_1023", 1023},
		{"string_len_1024", 1024},
		{"string_len_65534", 65534},
		{"string_len_65535", 65535},
		{"string_len_65536", 65536},
	}
	for _, tt := range tests {
		t.Run(tt.typ, func(t *testing.T) {
			r, ok := idx[tt.typ]
			if !ok {
				t.Fatalf("record %q not found", tt.typ)
			}
			got, ok := r["value"].(string)
			if !ok {
				t.Fatalf("expected string value for %q, got %T", tt.typ, r["value"])
			}
			if len(got) != tt.wlen {
				t.Errorf("len(value) = %d; want %d", len(got), tt.wlen)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Array length boundaries
// ---------------------------------------------------------------------------

func TestGenerate_ArrayLengthBoundaries(t *testing.T) {
	g := boundary.ChaosGenerator{}
	p, _ := g.Generate(chaosdata.GenerateOpts{Count: 1})
	idx := indexByType(decodePayload(t, p))

	tests := []struct {
		typ  string
		wlen int
	}{
		{"array_len_0", 0},
		{"array_len_1", 1},
		{"array_len_127", 127},
		{"array_len_128", 128},
		{"array_len_254", 254},
		{"array_len_255", 255},
		{"array_len_256", 256},
		{"array_len_65534", 65534},
		{"array_len_65535", 65535},
		{"array_len_65536", 65536},
	}
	for _, tt := range tests {
		t.Run(tt.typ, func(t *testing.T) {
			r, ok := idx[tt.typ]
			if !ok {
				t.Fatalf("record %q not found", tt.typ)
			}
			arr, ok := r["value"].([]interface{})
			if !ok {
				t.Fatalf("expected array value for %q, got %T", tt.typ, r["value"])
			}
			if len(arr) != tt.wlen {
				t.Errorf("len(value) = %d; want %d", len(arr), tt.wlen)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Registration
// ---------------------------------------------------------------------------

func TestRegistration(t *testing.T) {
	// Import side-effect registers the generator; verify it appears in All().
	all := chaosdata.All()
	found := false
	for _, g := range all {
		if g.Name() == "boundary" && g.Category() == "boundary" {
			found = true
			break
		}
	}
	if !found {
		t.Error("boundary.ChaosGenerator not found in chaosdata registry after import")
	}
}
