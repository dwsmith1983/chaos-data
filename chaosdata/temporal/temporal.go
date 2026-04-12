// Package temporal provides chaos data generators for timestamp-related edge
// cases, temporal ordering anomalies, and mixed timestamp format payloads.
package temporal

import (
	"encoding/json"
	"fmt"
	"math/rand"

	"github.com/dwsmith1983/chaos-data/chaosdata"
)

// ---------------------------------------------------------------------------
// EdgeCaseTimestamps
// ---------------------------------------------------------------------------

type EdgeCaseTimestamps struct{}

func (e EdgeCaseTimestamps) Name() string     { return "edge-case-timestamps" }
func (e EdgeCaseTimestamps) Category() string { return "temporal" }

var edgeCasePool = []map[string]interface{}{
	{"event": "pre-epoch", "timestamp": "1969-12-31T23:59:59Z", "unix": -1},
	{"event": "pre-epoch-deep", "timestamp": "1900-01-01T00:00:00Z", "unix": -2208988800},
	{"event": "2038-boundary", "timestamp": "2038-01-19T03:14:07Z", "unix": 2147483647},
	{"event": "2038-overflow", "timestamp": "2038-01-19T03:14:08Z", "unix": 2147483648},
	{"event": "leap-second", "timestamp": "2016-12-31T23:59:60Z", "note": "positive-leap-second"},
	{"event": "dst-spring-forward", "timestamp": "2024-03-10T02:30:00-05:00", "note": "non-existent-wall-clock-time"},
	{"event": "dst-fall-back", "timestamp": "2024-11-03T01:30:00-05:00", "note": "ambiguous-wall-clock-time"},
	{"event": "year-9999", "timestamp": "9999-12-31T23:59:59Z", "unix": 253402300799},
	{"event": "nanosecond-precision", "timestamp": "2024-06-15T12:00:00.123456789Z", "unix": 1718445600},
	{"event": "sub-microsecond", "timestamp": "2024-06-15T12:00:00.000000001Z", "unix": 1718445600},
	{"event": "tz-positive-offset", "timestamp": "2024-06-15T17:30:00+05:30", "unix": 1718445600},
	{"event": "tz-negative-offset", "timestamp": "2024-06-15T05:00:00-07:00", "unix": 1718445600},
	{"event": "tz-utc-z", "timestamp": "2024-06-15T12:00:00Z", "unix": 1718445600},
	{"event": "unix-epoch", "timestamp": "1970-01-01T00:00:00Z", "unix": 0},
	{"event": "max-nano-in-second", "timestamp": "2024-01-01T00:00:00.999999999Z", "unix": 1704067200},
}

func (e EdgeCaseTimestamps) Generate(opts chaosdata.GenerateOpts) (chaosdata.Payload, error) {
	count := opts.Count
	if count < 1 {
		count = 1
	}

	all := make([]map[string]interface{}, 0, len(edgeCasePool)*count)
	for i := 0; i < count; i++ {
		all = append(all, edgeCasePool...)
	}

	data, err := json.Marshal(all)
	if err != nil {
		return chaosdata.Payload{}, fmt.Errorf("temporal: marshal edge-case: %w", err)
	}

	return chaosdata.Payload{
		Data: data,
		Type: "application/json",
		Attributes: map[string]string{
			"generator": e.Name(),
			"category":  e.Category(),
		},
	}, nil
}

// ---------------------------------------------------------------------------
// TemporalOrderingAnomalies
// ---------------------------------------------------------------------------

type TemporalOrderingAnomalies struct{}

func (t TemporalOrderingAnomalies) Name() string     { return "temporal-ordering-anomalies" }
func (t TemporalOrderingAnomalies) Category() string { return "temporal" }

func (t TemporalOrderingAnomalies) Generate(opts chaosdata.GenerateOpts) (chaosdata.Payload, error) {
	count := opts.Count
	if count < 1 {
		count = 1
	}

	r := rand.New(rand.NewSource(0))
	kinds := []string{"out-of-order", "duplicate", "late-arrival", "future-timestamp"}
	records := make([]map[string]interface{}, 0, count)

	for i := 0; i < count; i++ {
		kind := kinds[i%len(kinds)]
		seq := r.Intn(10000)
		var rec map[string]interface{}

		switch kind {
		case "out-of-order":
			earlier := seq - r.Intn(500) - 1
			rec = map[string]interface{}{
				"kind":           "out-of-order",
				"seq":            earlier,
				"expected_after": seq,
				"timestamp":      "2024-06-15T12:00:00Z",
				"received_at":    "2024-06-15T12:05:00Z",
			}
		case "duplicate":
			rec = map[string]interface{}{
				"kind":      "duplicate",
				"seq":       seq,
				"timestamp": "2024-06-15T12:00:00Z",
				"duplicate": true,
			}
		case "late-arrival":
			lateSeconds := r.Intn(86400) + 3600
			rec = map[string]interface{}{
				"kind":            "late-arrival",
				"seq":             seq,
				"event_timestamp": "2024-06-14T12:00:00Z",
				"received_at":     "2024-06-15T12:00:00Z",
				"latency_seconds": lateSeconds,
			}
		case "future-timestamp":
			futureSeconds := r.Intn(3600) + 60
			rec = map[string]interface{}{
				"kind":           "future-timestamp",
				"seq":            seq,
				"timestamp":      "2099-01-01T00:00:00Z",
				"future_seconds": futureSeconds,
			}
		}
		records = append(records, rec)
	}

	data, err := json.Marshal(records)
	if err != nil {
		return chaosdata.Payload{}, fmt.Errorf("temporal: marshal anomalies: %w", err)
	}

	return chaosdata.Payload{
		Data: data,
		Type: "application/json",
		Attributes: map[string]string{
			"generator": t.Name(),
			"category":  t.Category(),
		},
	}, nil
}

// ---------------------------------------------------------------------------
// MixedFormatTimestamps
// ---------------------------------------------------------------------------

type MixedFormatTimestamps struct{}

func (m MixedFormatTimestamps) Name() string     { return "mixed-format-timestamps" }
func (m MixedFormatTimestamps) Category() string { return "temporal" }

var mixedFormatPool = []map[string]interface{}{
	{"event": "mixed-formats-1", "iso8601": "2024-06-15T12:00:00Z", "rfc3339": "2024-06-15T12:00:00Z", "rfc2822": "Sat, 15 Jun 2024 12:00:00 +0000", "unix_int": 1718445600, "unix_float": 1718445600.0},
	{"event": "mixed-formats-2", "iso8601": "2024-06-15T12:00:00.123456789Z", "rfc3339": "2024-06-15T12:00:00.123456789Z", "rfc2822": "Sat, 15 Jun 2024 12:00:00 +0000", "unix_int": 1718445600, "unix_float": 1718445600.123456789},
	{"event": "mixed-formats-3", "iso8601": "2024-06-15T17:30:00+05:30", "rfc3339": "2024-06-15T17:30:00+05:30", "rfc2822": "Sat, 15 Jun 2024 12:00:00 +0000", "unix_int": 1718445600, "unix_float": 1718445600.0},
	{"event": "mixed-formats-4", "iso8601": "2024-06-15T05:00:00-07:00", "rfc3339": "2024-06-15T05:00:00-07:00", "rfc2822": "Sat, 15 Jun 2024 12:00:00 +0000", "unix_int": 1718445600, "unix_float": 1718445600.0},
	{"event": "mixed-formats-5", "iso8601": "1970-01-01T00:00:00Z", "rfc3339": "1970-01-01T00:00:00Z", "rfc2822": "Thu, 01 Jan 1970 00:00:00 +0000", "unix_int": 0, "unix_float": 0.0},
	{"event": "mixed-formats-6", "iso8601": "2038-01-19T03:14:07Z", "rfc3339": "2038-01-19T03:14:07Z", "rfc2822": "Tue, 19 Jan 2038 03:14:07 +0000", "unix_int": 2147483647, "unix_float": 2147483647.0},
	{"event": "mixed-formats-7", "iso8601": "2024-12-31T23:59:59.999Z", "rfc3339": "2024-12-31T23:59:59.999Z", "rfc2822": "Tue, 31 Dec 2024 23:59:59 +0000", "unix_int": 1735689599, "unix_float": 1735689599.999},
	{"event": "mixed-formats-8", "iso8601": "2024-02-29T00:00:00Z", "rfc3339": "2024-02-29T00:00:00Z", "rfc2822": "Thu, 29 Feb 2024 00:00:00 +0000", "unix_int": 1709164800, "unix_float": 1709164800.0},
}

func (m MixedFormatTimestamps) Generate(opts chaosdata.GenerateOpts) (chaosdata.Payload, error) {
	count := opts.Count
	if count < 1 {
		count = 1
	}

	all := make([]map[string]interface{}, 0, len(mixedFormatPool)*count)
	for i := 0; i < count; i++ {
		all = append(all, mixedFormatPool...)
	}

	data, err := json.Marshal(all)
	if err != nil {
		return chaosdata.Payload{}, fmt.Errorf("temporal: marshal mixed-format: %w", err)
	}

	return chaosdata.Payload{
		Data: data,
		Type: "application/json",
		Attributes: map[string]string{
			"generator": m.Name(),
			"category":  m.Category(),
		},
	}, nil
}

// ---------------------------------------------------------------------------
// init: self-registration
// ---------------------------------------------------------------------------

func init() {
	chaosdata.Register(EdgeCaseTimestamps{})
	chaosdata.Register(TemporalOrderingAnomalies{})
	chaosdata.Register(MixedFormatTimestamps{})
}
