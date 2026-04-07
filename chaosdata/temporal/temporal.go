// Package temporal provides chaos data generators for timestamp-related edge
// cases, temporal ordering anomalies, and mixed timestamp format payloads.
package temporal

import (
	"fmt"
	"math/rand"
)

// ChaosGenerator is the interface all temporal generators implement.
// Generate returns raw []byte payloads; count controls how many records are
// produced. seed makes the output deterministic when the same value is reused.
type ChaosGenerator interface {
	Generate(opts Options) ([][]byte, error)
	Name() string
}

// Options controls generator behaviour.
type Options struct {
	// Count is the number of payloads to produce. Defaults to 1.
	Count int
	// Seed initialises the PRNG so callers can request deterministic output.
	Seed int64
}

func (o Options) countOrDefault() int {
	if o.Count <= 0 {
		return 1
	}
	return o.Count
}

// registry holds every registered ChaosGenerator, keyed by Name().
var registry = map[string]ChaosGenerator{}

// Register adds a generator to the global registry. It is called from init()
// functions so generators self-register at program start.
func Register(g ChaosGenerator) {
	registry[g.Name()] = g
}

// All returns every registered generator in an unspecified order.
func All() map[string]ChaosGenerator {
	out := make(map[string]ChaosGenerator, len(registry))
	for k, v := range registry {
		out[k] = v
	}
	return out
}

// Lookup returns a registered generator by name, and a bool indicating whether
// it was found.
func Lookup(name string) (ChaosGenerator, bool) {
	g, ok := registry[name]
	return g, ok
}

// ---------------------------------------------------------------------------
// EdgeCaseTimestamps
// ---------------------------------------------------------------------------

// EdgeCaseTimestamps emits hand-crafted JSON payloads that exercise
// pathological timestamp values: pre-epoch, the 2038 int32 boundary, leap
// seconds, DST transitions, year 9999, nanosecond precision, and multiple
// timezone offset variants.  The payloads are returned as raw []byte so
// consumers can pipe them directly into parsers without a round-trip through
// any intermediate representation.
type EdgeCaseTimestamps struct{}

func (e EdgeCaseTimestamps) Name() string { return "edge-case-timestamps" }

// edgeCasePool is the full catalogue of hand-crafted edge-case records.  Each
// entry is valid JSON and is returned verbatim; no formatting is applied so
// that byte-level assertions in tests stay stable.
var edgeCasePool = [][]byte{
	// Pre-epoch: 1 second before Unix epoch (negative Unix timestamp).
	[]byte(`{"event":"pre-epoch","timestamp":"1969-12-31T23:59:59Z","unix":-1}`),
	// Deep pre-epoch: well before Unix epoch.
	[]byte(`{"event":"pre-epoch-deep","timestamp":"1900-01-01T00:00:00Z","unix":-2208988800}`),
	// 2038 boundary: last second representable as a signed 32-bit Unix timestamp.
	[]byte(`{"event":"2038-boundary","timestamp":"2038-01-19T03:14:07Z","unix":2147483647}`),
	// One second after the 2038 boundary (int32 overflow territory).
	[]byte(`{"event":"2038-overflow","timestamp":"2038-01-19T03:14:08Z","unix":2147483648}`),
	// Leap second: 23:59:60 UTC on 2016-12-31.
	[]byte(`{"event":"leap-second","timestamp":"2016-12-31T23:59:60Z","note":"positive-leap-second"}`),
	// DST spring-forward gap: 02:30 EST which does not exist on 2024-03-10.
	[]byte(`{"event":"dst-spring-forward","timestamp":"2024-03-10T02:30:00-05:00","note":"non-existent-wall-clock-time"}`),
	// DST fall-back ambiguity: 01:30 EST which is ambiguous on 2024-11-03.
	[]byte(`{"event":"dst-fall-back","timestamp":"2024-11-03T01:30:00-05:00","note":"ambiguous-wall-clock-time"}`),
	// Year 9999: maximum representable year in RFC 3339.
	[]byte(`{"event":"year-9999","timestamp":"9999-12-31T23:59:59Z","unix":253402300799}`),
	// Nanosecond precision.
	[]byte(`{"event":"nanosecond-precision","timestamp":"2024-06-15T12:00:00.123456789Z","unix":1718445600}`),
	// Sub-microsecond precision.
	[]byte(`{"event":"sub-microsecond","timestamp":"2024-06-15T12:00:00.000000001Z","unix":1718445600}`),
	// Positive timezone offset (+05:30 IST).
	[]byte(`{"event":"tz-positive-offset","timestamp":"2024-06-15T17:30:00+05:30","unix":1718445600}`),
	// Negative timezone offset (-07:00 MST).
	[]byte(`{"event":"tz-negative-offset","timestamp":"2024-06-15T05:00:00-07:00","unix":1718445600}`),
	// UTC explicit Z.
	[]byte(`{"event":"tz-utc-z","timestamp":"2024-06-15T12:00:00Z","unix":1718445600}`),
	// Zero timestamp (Unix epoch).
	[]byte(`{"event":"unix-epoch","timestamp":"1970-01-01T00:00:00Z","unix":0}`),
	// Max nanosecond offset within a second.
	[]byte(`{"event":"max-nano-in-second","timestamp":"2024-01-01T00:00:00.999999999Z","unix":1704067200}`),
}

// Generate returns up to opts.Count payloads cycling through the edge-case
// pool. When opts.Count exceeds len(edgeCasePool) the pool repeats from the
// beginning so the caller always receives exactly the requested count.
func (e EdgeCaseTimestamps) Generate(opts Options) ([][]byte, error) {
	n := opts.countOrDefault()
	out := make([][]byte, n)
	for i := 0; i < n; i++ {
		src := edgeCasePool[i%len(edgeCasePool)]
		dst := make([]byte, len(src))
		copy(dst, src)
		out[i] = dst
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// TemporalOrderingAnomalies
// ---------------------------------------------------------------------------

// TemporalOrderingAnomalies produces a stream of JSON event records that
// contain ordering anomalies: out-of-order delivery, duplicated sequence
// numbers, late-arriving events, and future-dated timestamps.
type TemporalOrderingAnomalies struct{}

func (t TemporalOrderingAnomalies) Name() string { return "temporal-ordering-anomalies" }

// anomalyKinds enumerates the four anomaly categories.
var anomalyKinds = []string{
	"out-of-order",
	"duplicate",
	"late-arrival",
	"future-timestamp",
}

// Generate produces opts.Count JSON records.  The anomaly kind cycles across
// the four categories, and the PRNG (seeded by opts.Seed) is used to produce
// varied sequence numbers and offset values while keeping output deterministic.
func (t TemporalOrderingAnomalies) Generate(opts Options) ([][]byte, error) {
	n := opts.countOrDefault()
	r := rand.New(rand.NewSource(opts.Seed)) //nolint:gosec // deterministic chaos data, not crypto
	out := make([][]byte, n)

	for i := 0; i < n; i++ {
		kind := anomalyKinds[i%len(anomalyKinds)]
		seq := r.Intn(10000)
		var payload []byte

		switch kind {
		case "out-of-order":
			// seq is intentionally lower than the previous value would be.
			earlier := seq - r.Intn(500) - 1
			payload = []byte(fmt.Sprintf(
				`{"kind":"out-of-order","seq":%d,"expected_after":%d,"timestamp":"2024-06-15T12:00:00Z","received_at":"2024-06-15T12:05:00Z"}`,
				earlier, seq,
			))
		case "duplicate":
			payload = []byte(fmt.Sprintf(
				`{"kind":"duplicate","seq":%d,"timestamp":"2024-06-15T12:00:00Z","duplicate":true}`,
				seq,
			))
		case "late-arrival":
			// Event timestamp is far in the past relative to receipt time.
			lateSeconds := r.Intn(86400) + 3600
			payload = []byte(fmt.Sprintf(
				`{"kind":"late-arrival","seq":%d,"event_timestamp":"2024-06-14T12:00:00Z","received_at":"2024-06-15T12:00:00Z","latency_seconds":%d}`,
				seq, lateSeconds,
			))
		case "future-timestamp":
			// Timestamp is ahead of the current logical clock.
			futureSeconds := r.Intn(3600) + 60
			payload = []byte(fmt.Sprintf(
				`{"kind":"future-timestamp","seq":%d,"timestamp":"2099-01-01T00:00:00Z","future_seconds":%d}`,
				seq, futureSeconds,
			))
		}

		out[i] = payload
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// MixedFormatTimestamps
// ---------------------------------------------------------------------------

// MixedFormatTimestamps emits JSON payloads where a single logical event
// carries the same instant expressed in multiple timestamp formats: ISO 8601,
// RFC 3339, RFC 2822, and Unix time as both integer and floating-point.  This
// stresses parsers that must normalise disparate formats.
type MixedFormatTimestamps struct{}

func (m MixedFormatTimestamps) Name() string { return "mixed-format-timestamps" }

// mixedFormatPool is a catalogue of payloads where each record carries several
// representations of the same instant.
var mixedFormatPool = [][]byte{
	[]byte(`{"event":"mixed-formats-1","iso8601":"2024-06-15T12:00:00Z","rfc3339":"2024-06-15T12:00:00Z","rfc2822":"Sat, 15 Jun 2024 12:00:00 +0000","unix_int":1718445600,"unix_float":1718445600.0}`),
	[]byte(`{"event":"mixed-formats-2","iso8601":"2024-06-15T12:00:00.123456789Z","rfc3339":"2024-06-15T12:00:00.123456789Z","rfc2822":"Sat, 15 Jun 2024 12:00:00 +0000","unix_int":1718445600,"unix_float":1718445600.123456789}`),
	[]byte(`{"event":"mixed-formats-3","iso8601":"2024-06-15T17:30:00+05:30","rfc3339":"2024-06-15T17:30:00+05:30","rfc2822":"Sat, 15 Jun 2024 12:00:00 +0000","unix_int":1718445600,"unix_float":1718445600.0}`),
	[]byte(`{"event":"mixed-formats-4","iso8601":"2024-06-15T05:00:00-07:00","rfc3339":"2024-06-15T05:00:00-07:00","rfc2822":"Sat, 15 Jun 2024 12:00:00 +0000","unix_int":1718445600,"unix_float":1718445600.0}`),
	[]byte(`{"event":"mixed-formats-5","iso8601":"1970-01-01T00:00:00Z","rfc3339":"1970-01-01T00:00:00Z","rfc2822":"Thu, 01 Jan 1970 00:00:00 +0000","unix_int":0,"unix_float":0.0}`),
	[]byte(`{"event":"mixed-formats-6","iso8601":"2038-01-19T03:14:07Z","rfc3339":"2038-01-19T03:14:07Z","rfc2822":"Tue, 19 Jan 2038 03:14:07 +0000","unix_int":2147483647,"unix_float":2147483647.0}`),
	[]byte(`{"event":"mixed-formats-7","iso8601":"2024-12-31T23:59:59.999Z","rfc3339":"2024-12-31T23:59:59.999Z","rfc2822":"Tue, 31 Dec 2024 23:59:59 +0000","unix_int":1735689599,"unix_float":1735689599.999}`),
	[]byte(`{"event":"mixed-formats-8","iso8601":"2024-02-29T00:00:00Z","rfc3339":"2024-02-29T00:00:00Z","rfc2822":"Thu, 29 Feb 2024 00:00:00 +0000","unix_int":1709164800,"unix_float":1709164800.0}`),
}

// Generate returns opts.Count payloads cycling through the mixed-format pool.
// opts.Seed is accepted for interface consistency but pool entries are static,
// so output is always deterministic regardless of seed value.
func (m MixedFormatTimestamps) Generate(opts Options) ([][]byte, error) {
	n := opts.countOrDefault()
	out := make([][]byte, n)
	for i := 0; i < n; i++ {
		src := mixedFormatPool[i%len(mixedFormatPool)]
		dst := make([]byte, len(src))
		copy(dst, src)
		out[i] = dst
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// init: self-registration
// ---------------------------------------------------------------------------

func init() {
	Register(EdgeCaseTimestamps{})
	Register(TemporalOrderingAnomalies{})
	Register(MixedFormatTimestamps{})
}
