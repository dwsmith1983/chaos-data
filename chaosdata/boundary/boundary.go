// Package boundary provides a chaos data generator for off-by-one and
// boundary condition payloads covering integer limits, unsigned boundaries,
// powers of two, date/time edge cases, timezone offsets, and string/array
// length boundaries.
package boundary

import (
	"encoding/json"
	"fmt"
	"math"

	"github.com/dwsmith1983/chaos-data/chaosdata"
)

// ---------------------------------------------------------------------------
// Generator
// ---------------------------------------------------------------------------

// ChaosGenerator implements chaosdata.ChaosGenerator for boundary values.
type ChaosGenerator struct{}

// Name returns the unique name of this generator.
func (ChaosGenerator) Name() string { return "boundary" }

// Category returns the category this generator belongs to.
func (ChaosGenerator) Category() string { return "boundary" }

// Generate produces a Payload containing JSON-encoded boundary condition
// records. Each record is an object with a "type" and a "value" field.
// opts.Count controls the number of copies of the full boundary set to emit
// (minimum 1); opts.Size is ignored because boundary values are enumerated
// exhaustively.
func (g ChaosGenerator) Generate(opts chaosdata.GenerateOpts) (chaosdata.Payload, error) {
	records := boundaryRecords()

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
		return chaosdata.Payload{}, fmt.Errorf("boundary: marshal payload: %w", err)
	}

	return chaosdata.Payload{
		Data:     data,
		Type:     "application/json",
		Attributes: map[string]string{
			"generator": g.Name(),
			"category":  g.Category(),
			"records":   fmt.Sprintf("%d", len(all)),
		},
	}, nil
}

// ---------------------------------------------------------------------------
// Boundary record construction
// ---------------------------------------------------------------------------

func rec(typ string, value interface{}) map[string]interface{} {
	return map[string]interface{}{
		"type":  typ,
		"value": value,
	}
}

// boundaryRecords returns the canonical set of boundary condition records.
func boundaryRecords() []map[string]interface{} {
	records := []map[string]interface{}{}

	// Integer signed boundaries: int8
	records = append(records,
		rec("int8_min", int8(math.MinInt8)),           // -128
		rec("int8_max", int8(math.MaxInt8)),            // 127
		rec("int8_min_plus1", int8(math.MinInt8+1)),    // -127
		rec("int8_max_minus1", int8(math.MaxInt8-1)),   // 126
	)

	// Integer signed boundaries: int16
	records = append(records,
		rec("int16_min", int16(math.MinInt16)),          // -32768
		rec("int16_max", int16(math.MaxInt16)),          // 32767
		rec("int16_min_plus1", int16(math.MinInt16+1)),  // -32767
		rec("int16_max_minus1", int16(math.MaxInt16-1)), // 32766
	)

	// Integer signed boundaries: int32
	records = append(records,
		rec("int32_min", int32(math.MinInt32)),          // -2147483648
		rec("int32_max", int32(math.MaxInt32)),          // 2147483647
		rec("int32_min_plus1", int32(math.MinInt32+1)),  // -2147483647
		rec("int32_max_minus1", int32(math.MaxInt32-1)), // 2147483646
	)

	// Integer signed boundaries: int64
	records = append(records,
		rec("int64_min", int64(math.MinInt64)),          // -9223372036854775808
		rec("int64_max", int64(math.MaxInt64)),          // 9223372036854775807
		rec("int64_min_plus1", int64(math.MinInt64+1)),  // -9223372036854775807
		rec("int64_max_minus1", int64(math.MaxInt64-1)), // 9223372036854775806
	)

	// Unsigned boundaries: uint8
	records = append(records,
		rec("uint8_min", uint64(0)),
		rec("uint8_max", uint64(math.MaxUint8)),          // 255
		rec("uint8_max_minus1", uint64(math.MaxUint8-1)), // 254
		rec("uint8_min_plus1", uint64(1)),
	)

	// Unsigned boundaries: uint16
	records = append(records,
		rec("uint16_min", uint64(0)),
		rec("uint16_max", uint64(math.MaxUint16)),          // 65535
		rec("uint16_max_minus1", uint64(math.MaxUint16-1)), // 65534
		rec("uint16_min_plus1", uint64(1)),
	)

	// Unsigned boundaries: uint32
	records = append(records,
		rec("uint32_min", uint64(0)),
		rec("uint32_max", uint64(math.MaxUint32)),          // 4294967295
		rec("uint32_max_minus1", uint64(math.MaxUint32-1)), // 4294967294
		rec("uint32_min_plus1", uint64(1)),
	)

	// Unsigned boundaries: uint64 (encoded as strings to avoid JSON precision loss)
	records = append(records,
		rec("uint64_min", "0"),
		rec("uint64_max", fmt.Sprintf("%d", uint64(math.MaxUint64))),
		rec("uint64_max_minus1", fmt.Sprintf("%d", uint64(math.MaxUint64-1))),
		rec("uint64_min_plus1", "1"),
	)

	// Powers of two ±1 (up to 2^63)
	for exp := 1; exp <= 63; exp++ {
		pow := int64(1) << exp
		records = append(records,
			rec(fmt.Sprintf("pow2_%d", exp), pow),
			rec(fmt.Sprintf("pow2_%d_minus1", exp), pow-1),
		)
		if exp < 63 { // pow+1 overflows int64 at exp==63
			records = append(records,
				rec(fmt.Sprintf("pow2_%d_plus1", exp), pow+1),
			)
		}
	}

	// Leap year dates: Feb 29 (known leap years)
	leapYears := []int{2000, 2004, 2008, 2012, 2016, 2020, 2024}
	for _, y := range leapYears {
		records = append(records,
			rec(fmt.Sprintf("leap_feb29_%d", y), fmt.Sprintf("%04d-02-29", y)),
			rec(fmt.Sprintf("leap_feb28_%d", y), fmt.Sprintf("%04d-02-28", y)),
			rec(fmt.Sprintf("leap_mar01_%d", y), fmt.Sprintf("%04d-03-01", y)),
		)
	}

	// Non-leap year Feb boundaries
	records = append(records,
		rec("non_leap_feb28_2023", "2023-02-28"),
		rec("non_leap_mar01_2023", "2023-03-01"),
		// Feb 29 does not exist in 2023 — intentional boundary value
		rec("non_leap_invalid_feb29_2023", "2023-02-29"),
	)

	// Epoch timestamps
	records = append(records,
		rec("epoch_zero", int64(0)),
		rec("epoch_minus1", int64(-1)),
		rec("epoch_int32_max_y2038", int64(math.MaxInt32)), // 2147483647 — Y2038 boundary
		rec("epoch_int32_max_plus1", int64(math.MaxInt32+1)),
		rec("epoch_int32_min", int64(math.MinInt32)),
		rec("epoch_unix_nanos_max", int64(math.MaxInt64)),
		rec("epoch_unix_nanos_min", int64(math.MinInt64)),
	)

	// Timezone edge cases
	tzOffsets := []struct {
		label  string
		offset string
	}{
		{"utc", "+00:00"},
		{"utc_plus12", "+12:00"},
		{"utc_minus12", "-12:00"},
		{"utc_plus1330", "+13:30"}, // Samoa / Tonga edge
		{"utc_minus1130", "-11:30"},
		{"utc_plus0530", "+05:30"}, // India Standard Time half-hour offset
		{"utc_plus0545", "+05:45"}, // Nepal half-hour offset
		{"utc_plus0630", "+06:30"}, // Myanmar half-hour offset
		{"utc_plus0930", "+09:30"}, // ACST half-hour offset
		{"utc_plus1030", "+10:30"}, // LHST half-hour offset
		{"utc_minus0930", "-09:30"}, // Marquesas Islands
		{"utc_minus0330", "-03:30"}, // Newfoundland Standard Time
	}
	for _, tz := range tzOffsets {
		records = append(records,
			rec(fmt.Sprintf("tz_%s", tz.label), fmt.Sprintf("2024-01-15T12:00:00%s", tz.offset)),
		)
	}

	// String length boundaries
	strLengths := []int{0, 1, 127, 128, 254, 255, 256, 1023, 1024, 65534, 65535, 65536}
	for _, l := range strLengths {
		var s string
		if l > 0 {
			buf := make([]byte, l)
			for i := range buf {
				buf[i] = 'a'
			}
			s = string(buf)
		}
		records = append(records,
			rec(fmt.Sprintf("string_len_%d", l), s),
		)
	}

	// Array length boundaries
	arrLengths := []int{0, 1, 127, 128, 254, 255, 256, 65534, 65535, 65536}
	for _, l := range arrLengths {
		arr := make([]int, l)
		for i := range arr {
			arr[i] = i
		}
		records = append(records,
			rec(fmt.Sprintf("array_len_%d", l), arr),
		)
	}

	return records
}

// ---------------------------------------------------------------------------
// init: self-registration
// ---------------------------------------------------------------------------

func init() {
	chaosdata.Register(ChaosGenerator{})
}
