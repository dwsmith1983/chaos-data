package temporal_test

import (
	"bytes"
	"testing"

	"github.com/dwsmith1983/chaos-data/chaosdata/temporal"
)

// ---------------------------------------------------------------------------
// Helper
// ---------------------------------------------------------------------------

func requireNoError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Registry
// ---------------------------------------------------------------------------

func TestRegisterAndLookup(t *testing.T) {
	names := []string{
		"edge-case-timestamps",
		"temporal-ordering-anomalies",
		"mixed-format-timestamps",
	}
	for _, name := range names {
		t.Run(name, func(t *testing.T) {
			g, ok := temporal.Lookup(name)
			if !ok {
				t.Fatalf("Lookup(%q) returned false; generator not registered", name)
			}
			if g.Name() != name {
				t.Errorf("Name() = %q, want %q", g.Name(), name)
			}
		})
	}
}

func TestAll_ContainsAllGenerators(t *testing.T) {
	all := temporal.All()
	want := map[string]bool{
		"edge-case-timestamps":       true,
		"temporal-ordering-anomalies": true,
		"mixed-format-timestamps":    true,
	}
	for name := range want {
		if _, ok := all[name]; !ok {
			t.Errorf("All() missing generator %q", name)
		}
	}
	if len(all) != len(want) {
		t.Errorf("All() returned %d generators, want %d", len(all), len(want))
	}
}

// ---------------------------------------------------------------------------
// EdgeCaseTimestamps
// ---------------------------------------------------------------------------

func TestEdgeCaseTimestamps_Name(t *testing.T) {
	g := temporal.EdgeCaseTimestamps{}
	if got := g.Name(); got != "edge-case-timestamps" {
		t.Errorf("Name() = %q, want %q", got, "edge-case-timestamps")
	}
}

func TestEdgeCaseTimestamps_CountMatchesOpts(t *testing.T) {
	tests := []struct {
		name  string
		count int
		want  int
	}{
		{"zero defaults to one", 0, 1},
		{"negative defaults to one", -5, 1},
		{"exact count", 5, 5},
		{"count larger than pool", 30, 30},
	}
	g := temporal.EdgeCaseTimestamps{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			payloads, err := g.Generate(temporal.Options{Count: tt.count})
			requireNoError(t, err)
			if len(payloads) != tt.want {
				t.Errorf("len(payloads) = %d, want %d", len(payloads), tt.want)
			}
		})
	}
}

func TestEdgeCaseTimestamps_ByteContentLiterals(t *testing.T) {
	tests := []struct {
		name     string
		idx      int
		contains []string
	}{
		{
			name: "pre-epoch record has negative unix value",
			idx:  0,
			contains: []string{
				"pre-epoch",
				"1969-12-31",
				`"unix":-1`,
			},
		},
		{
			name: "2038 boundary record",
			idx:  2,
			contains: []string{
				"2038-boundary",
				"2038-01-19",
				"2147483647",
			},
		},
		{
			name: "leap second record contains 23:59:60",
			idx:  4,
			contains: []string{
				"leap-second",
				"23:59:60",
				"positive-leap-second",
			},
		},
		{
			name: "DST spring-forward record",
			idx:  5,
			contains: []string{
				"dst-spring-forward",
				"02:30:00",
				"non-existent-wall-clock-time",
			},
		},
		{
			name: "DST fall-back record",
			idx:  6,
			contains: []string{
				"dst-fall-back",
				"01:30:00",
				"ambiguous-wall-clock-time",
			},
		},
		{
			name: "year 9999 record",
			idx:  7,
			contains: []string{
				"year-9999",
				"9999-12-31",
				"253402300799",
			},
		},
		{
			name: "nanosecond precision record",
			idx:  8,
			contains: []string{
				"nanosecond-precision",
				".123456789",
			},
		},
		{
			name: "positive timezone offset record",
			idx:  10,
			contains: []string{
				"tz-positive-offset",
				"+05:30",
			},
		},
		{
			name: "negative timezone offset record",
			idx:  11,
			contains: []string{
				"tz-negative-offset",
				"-07:00",
			},
		},
		{
			name: "UTC Z record",
			idx:  12,
			contains: []string{
				"tz-utc-z",
				`"timestamp":"2024-06-15T12:00:00Z"`,
			},
		},
		{
			name: "unix epoch record",
			idx:  13,
			contains: []string{
				"unix-epoch",
				"1970-01-01",
				`"unix":0`,
			},
		},
	}

	g := temporal.EdgeCaseTimestamps{}
	// Request enough payloads to cover the highest index used above (idx 13 → need 14).
	payloads, err := g.Generate(temporal.Options{Count: 14})
	requireNoError(t, err)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			payload := payloads[tt.idx]
			for _, lit := range tt.contains {
				if !bytes.Contains(payload, []byte(lit)) {
					t.Errorf("payload[%d] does not contain %q\n  payload: %s", tt.idx, lit, payload)
				}
			}
		})
	}
}

func TestEdgeCaseTimestamps_Deterministic(t *testing.T) {
	g := temporal.EdgeCaseTimestamps{}
	opts := temporal.Options{Count: 15, Seed: 42}

	first, err := g.Generate(opts)
	requireNoError(t, err)
	second, err := g.Generate(opts)
	requireNoError(t, err)

	if len(first) != len(second) {
		t.Fatalf("len mismatch: %d vs %d", len(first), len(second))
	}
	for i := range first {
		if !bytes.Equal(first[i], second[i]) {
			t.Errorf("payload[%d] differs between runs", i)
		}
	}
}

func TestEdgeCaseTimestamps_PayloadsAreValidJSONBytes(t *testing.T) {
	g := temporal.EdgeCaseTimestamps{}
	payloads, err := g.Generate(temporal.Options{Count: 15})
	requireNoError(t, err)
	for i, p := range payloads {
		if len(p) == 0 {
			t.Errorf("payload[%d] is empty", i)
		}
		if p[0] != '{' {
			t.Errorf("payload[%d] does not start with '{': %s", i, p)
		}
		if p[len(p)-1] != '}' {
			t.Errorf("payload[%d] does not end with '}': %s", i, p)
		}
	}
}

// ---------------------------------------------------------------------------
// TemporalOrderingAnomalies
// ---------------------------------------------------------------------------

func TestTemporalOrderingAnomalies_Name(t *testing.T) {
	g := temporal.TemporalOrderingAnomalies{}
	if got := g.Name(); got != "temporal-ordering-anomalies" {
		t.Errorf("Name() = %q, want %q", got, "temporal-ordering-anomalies")
	}
}

func TestTemporalOrderingAnomalies_CountMatchesOpts(t *testing.T) {
	tests := []struct {
		name  string
		count int
		want  int
	}{
		{"zero defaults to one", 0, 1},
		{"negative defaults to one", -3, 1},
		{"exact small count", 4, 4},
		{"larger count", 20, 20},
	}
	g := temporal.TemporalOrderingAnomalies{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			payloads, err := g.Generate(temporal.Options{Count: tt.count, Seed: 1})
			requireNoError(t, err)
			if len(payloads) != tt.want {
				t.Errorf("len(payloads) = %d, want %d", len(payloads), tt.want)
			}
		})
	}
}

func TestTemporalOrderingAnomalies_AllKindsPresent(t *testing.T) {
	g := temporal.TemporalOrderingAnomalies{}
	// 4 kinds cycle in order, so 4 payloads guarantees one of each.
	payloads, err := g.Generate(temporal.Options{Count: 4, Seed: 99})
	requireNoError(t, err)

	kinds := []string{"out-of-order", "duplicate", "late-arrival", "future-timestamp"}
	for i, kind := range kinds {
		if !bytes.Contains(payloads[i], []byte(kind)) {
			t.Errorf("payload[%d] does not contain kind %q\n  payload: %s", i, kind, payloads[i])
		}
	}
}

func TestTemporalOrderingAnomalies_ByteContentLiterals(t *testing.T) {
	tests := []struct {
		name     string
		kind     string
		contains []string
	}{
		{
			name: "out-of-order has expected_after field",
			kind: "out-of-order",
			contains: []string{
				`"kind":"out-of-order"`,
				"expected_after",
				"received_at",
			},
		},
		{
			name: "duplicate has duplicate:true field",
			kind: "duplicate",
			contains: []string{
				`"kind":"duplicate"`,
				`"duplicate":true`,
			},
		},
		{
			name: "late-arrival has latency_seconds",
			kind: "late-arrival",
			contains: []string{
				`"kind":"late-arrival"`,
				"event_timestamp",
				"latency_seconds",
			},
		},
		{
			name: "future-timestamp references far future year",
			kind: "future-timestamp",
			contains: []string{
				`"kind":"future-timestamp"`,
				"2099",
				"future_seconds",
			},
		},
	}

	g := temporal.TemporalOrderingAnomalies{}
	// Cycle repeats so 4 payloads covers all four kinds in order.
	payloads, err := g.Generate(temporal.Options{Count: 4, Seed: 7})
	requireNoError(t, err)

	kindIndex := map[string]int{
		"out-of-order":     0,
		"duplicate":        1,
		"late-arrival":     2,
		"future-timestamp": 3,
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			idx := kindIndex[tt.kind]
			payload := payloads[idx]
			for _, lit := range tt.contains {
				if !bytes.Contains(payload, []byte(lit)) {
					t.Errorf("payload[%d] (%s) does not contain %q\n  payload: %s", idx, tt.kind, lit, payload)
				}
			}
		})
	}
}

func TestTemporalOrderingAnomalies_Deterministic(t *testing.T) {
	g := temporal.TemporalOrderingAnomalies{}
	opts := temporal.Options{Count: 16, Seed: 12345}

	first, err := g.Generate(opts)
	requireNoError(t, err)
	second, err := g.Generate(opts)
	requireNoError(t, err)

	if len(first) != len(second) {
		t.Fatalf("len mismatch: %d vs %d", len(first), len(second))
	}
	for i := range first {
		if !bytes.Equal(first[i], second[i]) {
			t.Errorf("payload[%d] differs between runs (seed=%d)", i, opts.Seed)
		}
	}
}

func TestTemporalOrderingAnomalies_DifferentSeeds_ProduceDifferentOutput(t *testing.T) {
	g := temporal.TemporalOrderingAnomalies{}
	a, err := g.Generate(temporal.Options{Count: 8, Seed: 1})
	requireNoError(t, err)
	b, err := g.Generate(temporal.Options{Count: 8, Seed: 2})
	requireNoError(t, err)

	// At least one payload should differ between seeds.
	anyDiff := false
	for i := range a {
		if !bytes.Equal(a[i], b[i]) {
			anyDiff = true
			break
		}
	}
	if !anyDiff {
		t.Error("different seeds produced identical output for all payloads")
	}
}

func TestTemporalOrderingAnomalies_PayloadsAreNonEmpty(t *testing.T) {
	g := temporal.TemporalOrderingAnomalies{}
	payloads, err := g.Generate(temporal.Options{Count: 12, Seed: 0})
	requireNoError(t, err)
	for i, p := range payloads {
		if len(p) == 0 {
			t.Errorf("payload[%d] is empty", i)
		}
	}
}

// ---------------------------------------------------------------------------
// MixedFormatTimestamps
// ---------------------------------------------------------------------------

func TestMixedFormatTimestamps_Name(t *testing.T) {
	g := temporal.MixedFormatTimestamps{}
	if got := g.Name(); got != "mixed-format-timestamps" {
		t.Errorf("Name() = %q, want %q", got, "mixed-format-timestamps")
	}
}

func TestMixedFormatTimestamps_CountMatchesOpts(t *testing.T) {
	tests := []struct {
		name  string
		count int
		want  int
	}{
		{"zero defaults to one", 0, 1},
		{"negative defaults to one", -1, 1},
		{"exact pool size", 8, 8},
		{"larger than pool", 20, 20},
	}
	g := temporal.MixedFormatTimestamps{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			payloads, err := g.Generate(temporal.Options{Count: tt.count})
			requireNoError(t, err)
			if len(payloads) != tt.want {
				t.Errorf("len(payloads) = %d, want %d", len(payloads), tt.want)
			}
		})
	}
}

func TestMixedFormatTimestamps_ByteContentLiterals(t *testing.T) {
	tests := []struct {
		name     string
		idx      int
		contains []string
	}{
		{
			name: "payload 0 has all four format keys and unix int/float",
			idx:  0,
			contains: []string{
				"iso8601",
				"rfc3339",
				"rfc2822",
				"unix_int",
				"unix_float",
				"2024-06-15T12:00:00Z",
				"Sat, 15 Jun 2024",
				"1718445600",
			},
		},
		{
			name: "payload 1 contains nanosecond precision in iso8601 and rfc3339",
			idx:  1,
			contains: []string{
				".123456789",
				"unix_float",
				"1718445600.123456789",
			},
		},
		{
			name: "payload 2 contains positive tz offset +05:30",
			idx:  2,
			contains: []string{
				"+05:30",
				"rfc2822",
				"unix_int",
			},
		},
		{
			name: "payload 3 contains negative tz offset -07:00",
			idx:  3,
			contains: []string{
				"-07:00",
				"unix_float",
			},
		},
		{
			name: "payload 4 is unix epoch with unix_int:0",
			idx:  4,
			contains: []string{
				"1970-01-01",
				`"unix_int":0`,
				`"unix_float":0.0`,
				"Thu, 01 Jan 1970",
			},
		},
		{
			name: "payload 5 contains 2038 boundary",
			idx:  5,
			contains: []string{
				"2038-01-19",
				"2147483647",
			},
		},
		{
			name: "payload 6 has millisecond precision float",
			idx:  6,
			contains: []string{
				"1735689599.999",
				"23:59:59",
			},
		},
		{
			name: "payload 7 is leap day 2024",
			idx:  7,
			contains: []string{
				"2024-02-29",
				"Thu, 29 Feb 2024",
				"1709164800",
			},
		},
	}

	g := temporal.MixedFormatTimestamps{}
	payloads, err := g.Generate(temporal.Options{Count: 8})
	requireNoError(t, err)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			payload := payloads[tt.idx]
			for _, lit := range tt.contains {
				if !bytes.Contains(payload, []byte(lit)) {
					t.Errorf("payload[%d] does not contain %q\n  payload: %s", tt.idx, lit, payload)
				}
			}
		})
	}
}

func TestMixedFormatTimestamps_Deterministic(t *testing.T) {
	g := temporal.MixedFormatTimestamps{}
	opts := temporal.Options{Count: 16, Seed: 999}

	first, err := g.Generate(opts)
	requireNoError(t, err)
	second, err := g.Generate(opts)
	requireNoError(t, err)

	if len(first) != len(second) {
		t.Fatalf("len mismatch: %d vs %d", len(first), len(second))
	}
	for i := range first {
		if !bytes.Equal(first[i], second[i]) {
			t.Errorf("payload[%d] differs between runs", i)
		}
	}
}

func TestMixedFormatTimestamps_DifferentSeedsProduceSameOutput(t *testing.T) {
	// Pool is static, so seed is irrelevant for this generator.
	g := temporal.MixedFormatTimestamps{}
	a, err := g.Generate(temporal.Options{Count: 8, Seed: 1})
	requireNoError(t, err)
	b, err := g.Generate(temporal.Options{Count: 8, Seed: 9999})
	requireNoError(t, err)

	for i := range a {
		if !bytes.Equal(a[i], b[i]) {
			t.Errorf("payload[%d] differs across seeds (pool is static, should be identical)", i)
		}
	}
}

func TestMixedFormatTimestamps_PayloadsAreValidJSONBytes(t *testing.T) {
	g := temporal.MixedFormatTimestamps{}
	payloads, err := g.Generate(temporal.Options{Count: 8})
	requireNoError(t, err)
	for i, p := range payloads {
		if len(p) == 0 {
			t.Errorf("payload[%d] is empty", i)
		}
		if p[0] != '{' {
			t.Errorf("payload[%d] does not start with '{': %s", i, p)
		}
		if p[len(p)-1] != '}' {
			t.Errorf("payload[%d] does not end with '}': %s", i, p)
		}
	}
}

// ---------------------------------------------------------------------------
// Interface compliance (compile-time)
// ---------------------------------------------------------------------------

var (
	_ temporal.ChaosGenerator = temporal.EdgeCaseTimestamps{}
	_ temporal.ChaosGenerator = temporal.TemporalOrderingAnomalies{}
	_ temporal.ChaosGenerator = temporal.MixedFormatTimestamps{}
)
