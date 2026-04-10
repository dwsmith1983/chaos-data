package concurrency_test

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/dwsmith1983/chaos-data/chaosdata/concurrency"
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

// record mirrors the JSON envelope produced by every generator.
type record struct {
	Generator  string `json:"generator"`
	SeqID      int64  `json:"seq_id"`
	UUID       string `json:"uuid"`
	EventTime  string `json:"event_time"`
	HashKey    string `json:"hash_key"`
	Payload    string `json:"payload"`
	PartialTag string `json:"partial_tag,omitempty"`
}

func parseAll(t *testing.T, payloads [][]byte) []record {
	t.Helper()
	records := make([]record, len(payloads))
	for i, p := range payloads {
		if err := json.Unmarshal(p, &records[i]); err != nil {
			t.Fatalf("payload[%d] is not valid JSON (%v): %s", i, err, p)
		}
	}
	return records
}

// ---------------------------------------------------------------------------
// Registry
// ---------------------------------------------------------------------------

func TestRegistry_AllRegistered(t *testing.T) {
	all := concurrency.All()
	const want = 7
	if len(all) != want {
		t.Errorf("registry: got %d generators; want %d", len(all), want)
	}
}

func TestRegistry_CategoryIsAlwaysConcurrency(t *testing.T) {
	for _, g := range concurrency.All() {
		if g.Category() != "concurrency" {
			t.Errorf("generator %q: category = %q; want \"concurrency\"", g.Name(), g.Category())
		}
	}
}

func TestRegistry_NamesAreUnique(t *testing.T) {
	seen := make(map[string]struct{})
	for _, g := range concurrency.All() {
		if _, dup := seen[g.Name()]; dup {
			t.Errorf("duplicate generator name: %q", g.Name())
		}
		seen[g.Name()] = struct{}{}
	}
}

// ---------------------------------------------------------------------------
// Shared structural invariants
// ---------------------------------------------------------------------------

func TestAllGenerators_StructuralInvariants(t *testing.T) {
	counts := []int{1, 4, 8, 16}

	for _, g := range concurrency.All() {
		g := g
		t.Run(g.Name(), func(t *testing.T) {
			for _, n := range counts {
				n := n
				t.Run(strings.ReplaceAll(t.Name(), "/", "_")+"_count="+itoa(n), func(t *testing.T) {
					payloads, err := g.Generate(concurrency.Options{Count: n})
					requireNoError(t, err)

					if len(payloads) != n {
						t.Fatalf("got %d payloads; want %d", len(payloads), n)
					}

					for i, p := range payloads {
						if len(p) == 0 {
							t.Errorf("payload[%d] is empty", i)
						}
						if !bytes.HasPrefix(p, []byte("{")) {
							t.Errorf("payload[%d] does not start with '{': %s", i, p)
						}
						if !bytes.HasSuffix(p, []byte("}")) {
							t.Errorf("payload[%d] does not end with '}': %s", i, p)
						}
						var r record
						if err := json.Unmarshal(p, &r); err != nil {
							t.Errorf("payload[%d] invalid JSON: %v — %s", i, err, p)
						}
						if r.Generator == "" {
							t.Errorf("payload[%d] has empty generator field", i)
						}
						if r.EventTime == "" {
							t.Errorf("payload[%d] has empty event_time field", i)
						}
					}
				})
			}
		})
	}
}

func TestAllGenerators_DefaultCountIs8(t *testing.T) {
	for _, g := range concurrency.All() {
		g := g
		t.Run(g.Name(), func(t *testing.T) {
			payloads, err := g.Generate(concurrency.Options{})
			requireNoError(t, err)
			if len(payloads) != 8 {
				t.Errorf("default count: got %d payloads; want 8", len(payloads))
			}
		})
	}
}

// ---------------------------------------------------------------------------
// IdenticalTimestamps
// ---------------------------------------------------------------------------

func TestIdenticalTimestamps_AllTimestampsEqual(t *testing.T) {
	payloads, err := concurrency.IdenticalTimestamps{}.Generate(concurrency.Options{Count: 10})
	requireNoError(t, err)
	records := parseAll(t, payloads)

	first := records[0].EventTime
	if first == "" {
		t.Fatal("event_time is empty")
	}
	for i, r := range records {
		if r.EventTime != first {
			t.Errorf("record[%d].EventTime = %q; want %q (identical to record[0])", i, r.EventTime, first)
		}
	}
}

func TestIdenticalTimestamps_UUIDIsConstant(t *testing.T) {
	payloads, err := concurrency.IdenticalTimestamps{}.Generate(concurrency.Options{Count: 5})
	requireNoError(t, err)
	records := parseAll(t, payloads)

	first := records[0].UUID
	for i, r := range records {
		if r.UUID != first {
			t.Errorf("record[%d].UUID = %q; want %q", i, r.UUID, first)
		}
	}
}

func TestIdenticalTimestamps_SeqIDsAreDistinct(t *testing.T) {
	payloads, err := concurrency.IdenticalTimestamps{}.Generate(concurrency.Options{Count: 8})
	requireNoError(t, err)
	records := parseAll(t, payloads)

	seen := make(map[int64]int)
	for i, r := range records {
		if prev, dup := seen[r.SeqID]; dup {
			t.Errorf("record[%d].SeqID = %d duplicates record[%d]", i, r.SeqID, prev)
		}
		seen[r.SeqID] = i
	}
}

// ---------------------------------------------------------------------------
// MonotonicDecreasingSeqIDs
// ---------------------------------------------------------------------------

func TestMonotonicDecreasingSeqIDs_StrictlyDecreasing(t *testing.T) {
	payloads, err := concurrency.MonotonicDecreasingSeqIDs{}.Generate(concurrency.Options{Count: 8})
	requireNoError(t, err)
	records := parseAll(t, payloads)

	for i := 1; i < len(records); i++ {
		if records[i].SeqID >= records[i-1].SeqID {
			t.Errorf("seq_id not decreasing at index %d: %d >= %d",
				i, records[i].SeqID, records[i-1].SeqID)
		}
	}
}

func TestMonotonicDecreasingSeqIDs_FirstIsN(t *testing.T) {
	n := 6
	payloads, err := concurrency.MonotonicDecreasingSeqIDs{}.Generate(concurrency.Options{Count: n})
	requireNoError(t, err)
	records := parseAll(t, payloads)

	if records[0].SeqID != int64(n) {
		t.Errorf("first seq_id = %d; want %d", records[0].SeqID, n)
	}
	if records[n-1].SeqID != 1 {
		t.Errorf("last seq_id = %d; want 1", records[n-1].SeqID)
	}
}

// ---------------------------------------------------------------------------
// DuplicateUUIDs
// ---------------------------------------------------------------------------

func TestDuplicateUUIDs_AllUUIDsIdentical(t *testing.T) {
	payloads, err := concurrency.DuplicateUUIDs{}.Generate(concurrency.Options{Count: 10})
	requireNoError(t, err)
	records := parseAll(t, payloads)

	first := records[0].UUID
	if first == "" {
		t.Fatal("uuid is empty")
	}
	for i, r := range records {
		if r.UUID != first {
			t.Errorf("record[%d].UUID = %q; want %q", i, r.UUID, first)
		}
	}
}

func TestDuplicateUUIDs_SeqIDsAreUnique(t *testing.T) {
	payloads, err := concurrency.DuplicateUUIDs{}.Generate(concurrency.Options{Count: 8})
	requireNoError(t, err)
	records := parseAll(t, payloads)

	seen := make(map[int64]bool)
	for i, r := range records {
		if seen[r.SeqID] {
			t.Errorf("record[%d].SeqID %d is a duplicate", i, r.SeqID)
		}
		seen[r.SeqID] = true
	}
}

func TestDuplicateUUIDs_TimestampsAreDistinct(t *testing.T) {
	payloads, err := concurrency.DuplicateUUIDs{}.Generate(concurrency.Options{Count: 6})
	requireNoError(t, err)
	records := parseAll(t, payloads)

	seen := make(map[string]bool)
	for i, r := range records {
		if seen[r.EventTime] {
			t.Errorf("record[%d].EventTime %q is a duplicate", i, r.EventTime)
		}
		seen[r.EventTime] = true
	}
}

// ---------------------------------------------------------------------------
// InterleavedPartialWrites
// ---------------------------------------------------------------------------

func TestInterleavedPartialWrites_SplitMarkerPresent(t *testing.T) {
	payloads, err := concurrency.InterleavedPartialWrites{}.Generate(concurrency.Options{Count: 8})
	requireNoError(t, err)
	records := parseAll(t, payloads)

	for i, r := range records {
		if !strings.Contains(r.Payload, "<SPLIT>") {
			t.Errorf("record[%d].Payload missing <SPLIT> marker: %q", i, r.Payload)
		}
	}
}

func TestInterleavedPartialWrites_PartialTagIsSet(t *testing.T) {
	payloads, err := concurrency.InterleavedPartialWrites{}.Generate(concurrency.Options{Count: 4})
	requireNoError(t, err)
	records := parseAll(t, payloads)

	for i, r := range records {
		if r.PartialTag == "" {
			t.Errorf("record[%d].PartialTag is empty; want \"split\"", i)
		}
	}
}

func TestInterleavedPartialWrites_SplitDividesTwoHalves(t *testing.T) {
	payloads, err := concurrency.InterleavedPartialWrites{}.Generate(concurrency.Options{Count: 4})
	requireNoError(t, err)
	records := parseAll(t, payloads)

	for i, r := range records {
		parts := strings.SplitN(r.Payload, "<SPLIT>", 2)
		if len(parts) != 2 {
			t.Errorf("record[%d].Payload split produced %d parts; want 2", i, len(parts))
			continue
		}
		if parts[0] == "" {
			t.Errorf("record[%d].Payload: first half is empty", i)
		}
		if parts[1] == "" {
			t.Errorf("record[%d].Payload: second half is empty", i)
		}
	}
}

// ---------------------------------------------------------------------------
// OutOfOrderSequenceNumbers
// ---------------------------------------------------------------------------

func TestOutOfOrderSequenceNumbers_NotSorted(t *testing.T) {
	n := 8
	payloads, err := concurrency.OutOfOrderSequenceNumbers{}.Generate(concurrency.Options{Count: n})
	requireNoError(t, err)
	records := parseAll(t, payloads)

	sorted := true
	for i := 1; i < len(records); i++ {
		if records[i].SeqID < records[i-1].SeqID {
			sorted = false
			break
		}
	}
	if sorted {
		ids := make([]int64, len(records))
		for i, r := range records {
			ids[i] = r.SeqID
		}
		t.Errorf("seq_ids appear sorted (not shuffled): %v", ids)
	}
}

func TestOutOfOrderSequenceNumbers_ContainsAllValues(t *testing.T) {
	n := 8
	payloads, err := concurrency.OutOfOrderSequenceNumbers{}.Generate(concurrency.Options{Count: n})
	requireNoError(t, err)
	records := parseAll(t, payloads)

	present := make(map[int64]bool, n)
	for _, r := range records {
		present[r.SeqID] = true
	}
	for i := 0; i < n; i++ {
		if !present[int64(i)] {
			t.Errorf("seq_id %d is missing from the shuffled output", i)
		}
	}
}

func TestOutOfOrderSequenceNumbers_DeterministicWithSameSeed(t *testing.T) {
	opts := concurrency.Options{Count: 8, Seed: 42}
	p1, err := concurrency.OutOfOrderSequenceNumbers{}.Generate(opts)
	requireNoError(t, err)
	p2, err := concurrency.OutOfOrderSequenceNumbers{}.Generate(opts)
	requireNoError(t, err)

	for i := range p1 {
		if !bytes.Equal(p1[i], p2[i]) {
			t.Errorf("payload[%d] differs between runs with same seed", i)
		}
	}
}

func TestOutOfOrderSequenceNumbers_MinCountIsTwo(t *testing.T) {
	// Count=1 is coerced to 2 to guarantee a non-trivial shuffle.
	payloads, err := concurrency.OutOfOrderSequenceNumbers{}.Generate(concurrency.Options{Count: 1})
	requireNoError(t, err)
	if len(payloads) < 2 {
		t.Errorf("got %d payloads for count=1; want at least 2", len(payloads))
	}
}

// ---------------------------------------------------------------------------
// HashCollisionInducing
// ---------------------------------------------------------------------------

func TestHashCollisionInducing_AllSameHashKey(t *testing.T) {
	payloads, err := concurrency.HashCollisionInducing{}.Generate(concurrency.Options{Count: 10})
	requireNoError(t, err)
	records := parseAll(t, payloads)

	first := records[0].HashKey
	if first == "" {
		t.Fatal("hash_key is empty")
	}
	for i, r := range records {
		if r.HashKey != first {
			t.Errorf("record[%d].HashKey = %q; want %q", i, r.HashKey, first)
		}
	}
}

func TestHashCollisionInducing_SeqIDsAreUnique(t *testing.T) {
	payloads, err := concurrency.HashCollisionInducing{}.Generate(concurrency.Options{Count: 8})
	requireNoError(t, err)
	records := parseAll(t, payloads)

	seen := make(map[int64]bool)
	for i, r := range records {
		if seen[r.SeqID] {
			t.Errorf("record[%d].SeqID %d is a duplicate", i, r.SeqID)
		}
		seen[r.SeqID] = true
	}
}

// ---------------------------------------------------------------------------
// NearSimultaneousTimestamps
// ---------------------------------------------------------------------------

func TestNearSimultaneousTimestamps_DifferByOneNanosecond(t *testing.T) {
	payloads, err := concurrency.NearSimultaneousTimestamps{}.Generate(concurrency.Options{Count: 8})
	requireNoError(t, err)
	records := parseAll(t, payloads)

	for i := 1; i < len(records); i++ {
		prev, err := time.Parse(time.RFC3339Nano, records[i-1].EventTime)
		if err != nil {
			t.Fatalf("record[%d].EventTime parse error: %v", i-1, err)
		}
		curr, err := time.Parse(time.RFC3339Nano, records[i].EventTime)
		if err != nil {
			t.Fatalf("record[%d].EventTime parse error: %v", i, err)
		}
		diff := curr.Sub(prev)
		if diff != time.Nanosecond {
			t.Errorf("record[%d] → record[%d] time diff = %v; want 1ns", i-1, i, diff)
		}
	}
}

func TestNearSimultaneousTimestamps_TimestampsAreMonotonicallyIncreasing(t *testing.T) {
	payloads, err := concurrency.NearSimultaneousTimestamps{}.Generate(concurrency.Options{Count: 8})
	requireNoError(t, err)
	records := parseAll(t, payloads)

	for i := 1; i < len(records); i++ {
		prev, _ := time.Parse(time.RFC3339Nano, records[i-1].EventTime)
		curr, _ := time.Parse(time.RFC3339Nano, records[i].EventTime)
		if !curr.After(prev) {
			t.Errorf("record[%d].EventTime %q is not after record[%d].EventTime %q",
				i, records[i].EventTime, i-1, records[i-1].EventTime)
		}
	}
}

func TestNearSimultaneousTimestamps_TruncatedToMilliAreEqual(t *testing.T) {
	// Millisecond-resolution consumers will see identical timestamps for the
	// first 1,000,000 records; even for small n they must all match when
	// the epoch second boundary is far away.
	payloads, err := concurrency.NearSimultaneousTimestamps{}.Generate(concurrency.Options{Count: 8})
	requireNoError(t, err)
	records := parseAll(t, payloads)

	first, err := time.Parse(time.RFC3339Nano, records[0].EventTime)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	firstMs := first.Truncate(time.Millisecond)

	for i, r := range records {
		ts, err := time.Parse(time.RFC3339Nano, r.EventTime)
		if err != nil {
			t.Fatalf("record[%d] parse error: %v", i, err)
		}
		if ts.Truncate(time.Millisecond) != firstMs {
			t.Errorf("record[%d] millisecond-truncated time differs from record[0]", i)
		}
	}
}

// ---------------------------------------------------------------------------
// Interface compliance (compile-time)
// ---------------------------------------------------------------------------

var (
	_ concurrency.ChaosGenerator = concurrency.IdenticalTimestamps{}
	_ concurrency.ChaosGenerator = concurrency.MonotonicDecreasingSeqIDs{}
	_ concurrency.ChaosGenerator = concurrency.DuplicateUUIDs{}
	_ concurrency.ChaosGenerator = concurrency.InterleavedPartialWrites{}
	_ concurrency.ChaosGenerator = concurrency.OutOfOrderSequenceNumbers{}
	_ concurrency.ChaosGenerator = concurrency.HashCollisionInducing{}
	_ concurrency.ChaosGenerator = concurrency.NearSimultaneousTimestamps{}
)

// ---------------------------------------------------------------------------
// itoa helper (avoids importing strconv in test file)
// ---------------------------------------------------------------------------

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := false
	if n < 0 {
		neg = true
		n = -n
	}
	buf := [20]byte{}
	pos := len(buf)
	for n > 0 {
		pos--
		buf[pos] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		pos--
		buf[pos] = '-'
	}
	return string(buf[pos:])
}
