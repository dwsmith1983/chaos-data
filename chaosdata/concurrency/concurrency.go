// Package concurrency provides chaos data generators that produce passive
// payloads designed to expose race conditions in consumers. All generators
// produce deterministic output; they perform no concurrent operations
// themselves.
package concurrency

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/dwsmith1983/chaos-data/chaosdata"
)

// ---------------------------------------------------------------------------
// Package-level registry (mirrors temporal package convention)
// ---------------------------------------------------------------------------

var (
	reg   []ChaosGenerator
	regMu sync.RWMutex
)

// ChaosGenerator is the interface all concurrency generators implement.
type ChaosGenerator interface {
	Generate(opts Options) ([][]byte, error)
	Name() string
	Category() string
}

// Options controls generation behaviour.
type Options struct {
	// Count is the number of payloads to produce. Defaults to 8 when zero.
	Count int
	// Seed makes output deterministic; 0 uses the zero-value seed (valid).
	Seed int64
}

// Register adds a generator to the package-level registry.
func Register(g ChaosGenerator) {
	regMu.Lock()
	defer regMu.Unlock()
	reg = append(reg, g)
}

// All returns a copy of all registered generators.
func All() []ChaosGenerator {
	regMu.RLock()
	defer regMu.RUnlock()
	out := make([]ChaosGenerator, len(reg))
	copy(out, reg)
	return out
}

// ---------------------------------------------------------------------------
// Shared payload envelope
// ---------------------------------------------------------------------------

// record is the JSON envelope emitted by every generator. Fields are chosen
// to maximise the surface area for consumer race conditions.
type record struct {
	Generator  string `json:"generator"`
	SeqID      int64  `json:"seq_id"`
	UUID       string `json:"uuid"`
	EventTime  string `json:"event_time"`
	HashKey    string `json:"hash_key"`
	Payload    string `json:"payload"`
	PartialTag string `json:"partial_tag,omitempty"`
}

func mustMarshal(r record) []byte {
	b, err := json.Marshal(r)
	if err != nil {
		panic(fmt.Sprintf("concurrency: marshal failed: %v", err))
	}
	return b
}

func defaultCount(opts Options) int {
	if opts.Count <= 0 {
		return 8
	}
	return opts.Count
}

// ---------------------------------------------------------------------------
// 1. IdenticalTimestamps
//    Every payload carries the exact same timestamp string, ensuring that
//    consumers sorting or deduplicating by time will see a total tie.
// ---------------------------------------------------------------------------

// IdenticalTimestamps produces payloads that all share one frozen timestamp.
type IdenticalTimestamps struct{}

func (IdenticalTimestamps) Name() string     { return "identical-timestamps" }
func (IdenticalTimestamps) Category() string { return "concurrency" }

func (IdenticalTimestamps) Generate(opts Options) ([][]byte, error) {
	n := defaultCount(opts)
	frozen := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339Nano)
	out := make([][]byte, n)
	for i := range out {
		out[i] = mustMarshal(record{
			Generator: "identical-timestamps",
			SeqID:     int64(i),
			UUID:      "00000000-0000-0000-0000-000000000001",
			EventTime: frozen,
			HashKey:   "hk-identical",
			Payload:   fmt.Sprintf("item-%d", i),
		})
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// 2. MonotonicDecreasingSeqIDs
//    Sequence IDs count downward (n, n-1, …, 1), so any consumer that
//    assumes monotonically increasing IDs will misorder or drop events.
// ---------------------------------------------------------------------------

// MonotonicDecreasingSeqIDs produces payloads with strictly descending IDs.
type MonotonicDecreasingSeqIDs struct{}

func (MonotonicDecreasingSeqIDs) Name() string     { return "monotonic-decreasing-seq-ids" }
func (MonotonicDecreasingSeqIDs) Category() string { return "concurrency" }

func (MonotonicDecreasingSeqIDs) Generate(opts Options) ([][]byte, error) {
	n := defaultCount(opts)
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	out := make([][]byte, n)
	for i := range out {
		out[i] = mustMarshal(record{
			Generator: "monotonic-decreasing-seq-ids",
			SeqID:     int64(n - i), // n, n-1, …, 1
			UUID:      fmt.Sprintf("00000000-0000-0000-0000-%012d", i+1),
			EventTime: base.Add(time.Duration(i) * time.Second).Format(time.RFC3339Nano),
			HashKey:   "hk-decreasing",
			Payload:   fmt.Sprintf("item-%d", i),
		})
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// 3. DuplicateUUIDs
//    Every payload reuses the same UUID, provoking idempotency checks and
//    deduplication logic to handle collisions.
// ---------------------------------------------------------------------------

// DuplicateUUIDs produces payloads that all share one fixed UUID.
type DuplicateUUIDs struct{}

func (DuplicateUUIDs) Name() string     { return "duplicate-uuids" }
func (DuplicateUUIDs) Category() string { return "concurrency" }

func (DuplicateUUIDs) Generate(opts Options) ([][]byte, error) {
	n := defaultCount(opts)
	const sharedUUID = "deadbeef-dead-beef-dead-beefdeadbeef"
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	out := make([][]byte, n)
	for i := range out {
		out[i] = mustMarshal(record{
			Generator: "duplicate-uuids",
			SeqID:     int64(i),
			UUID:      sharedUUID,
			EventTime: base.Add(time.Duration(i) * time.Second).Format(time.RFC3339Nano),
			HashKey:   fmt.Sprintf("hk-%d", i),
			Payload:   fmt.Sprintf("item-%d", i),
		})
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// 4. InterleavedPartialWrites
//    Every payload is a JSON object where the "payload" field value is split
//    mid-field by a sentinel marker ("<SPLIT>") that represents where a
//    partial write boundary occurs. Consumers that concatenate or stream
//    byte slices without framing will receive malformed JSON.
// ---------------------------------------------------------------------------

// InterleavedPartialWrites produces payloads split at a mid-field boundary.
type InterleavedPartialWrites struct{}

func (InterleavedPartialWrites) Name() string     { return "interleaved-partial-writes" }
func (InterleavedPartialWrites) Category() string { return "concurrency" }

func (InterleavedPartialWrites) Generate(opts Options) ([][]byte, error) {
	n := defaultCount(opts)
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	out := make([][]byte, n)
	for i := range out {
		// Build the full JSON, then insert the split marker inside the payload
		// field value so that naive concatenation produces invalid JSON.
		r := record{
			Generator:  "interleaved-partial-writes",
			SeqID:      int64(i),
			UUID:       fmt.Sprintf("00000000-0000-0000-0000-%012d", i+1),
			EventTime:  base.Add(time.Duration(i) * time.Second).Format(time.RFC3339Nano),
			HashKey:    fmt.Sprintf("hk-%d", i),
			Payload:    fmt.Sprintf("first-half-<SPLIT>-second-half-%d", i),
			PartialTag: "split",
		}
		out[i] = mustMarshal(r)
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// 5. OutOfOrderSequenceNumbers
//    Payloads are emitted with sequence IDs that have been deliberately
//    shuffled (using a fixed permutation derived from the seed) so consumers
//    relying on in-order delivery observe gaps and inversions.
// ---------------------------------------------------------------------------

// OutOfOrderSequenceNumbers produces payloads with shuffled seq IDs.
type OutOfOrderSequenceNumbers struct{}

func (OutOfOrderSequenceNumbers) Name() string     { return "out-of-order-sequence-numbers" }
func (OutOfOrderSequenceNumbers) Category() string { return "concurrency" }

func (OutOfOrderSequenceNumbers) Generate(opts Options) ([][]byte, error) {
	n := defaultCount(opts)
	if n < 2 {
		n = 2
	}

	// Build a fixed shuffle of [0, n). The permutation must be deterministic
	// and must never equal the identity (sorted) order for n >= 2.
	seqIDs := shuffledSeqIDs(n, opts.Seed)

	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	out := make([][]byte, n)
	for i := range out {
		out[i] = mustMarshal(record{
			Generator: "out-of-order-sequence-numbers",
			SeqID:     seqIDs[i],
			UUID:      fmt.Sprintf("00000000-0000-0000-0000-%012d", i+1),
			EventTime: base.Add(time.Duration(i) * time.Second).Format(time.RFC3339Nano),
			HashKey:   fmt.Sprintf("hk-%d", i),
			Payload:   fmt.Sprintf("item-%d", i),
		})
	}
	return out, nil
}

// shuffledSeqIDs returns n int64 values that are a fixed shuffle of [0, n).
// It uses Fisher-Yates with an LCG PRNG so output is deterministic for a
// given (n, seed) pair without external dependencies.
func shuffledSeqIDs(n int, seed int64) []int64 {
	ids := make([]int64, n)
	for i := range ids {
		ids[i] = int64(i)
	}
	// LCG constants from Knuth / Numerical Recipes (mod 2^64).
	// We use uint64 arithmetic throughout to avoid signed-overflow UB.
	state := uint64(seed) ^ 0x9e3779b97f4a7c15 //nolint:gosec // intentional: non-crypto PRNG
	swap := func(a, b int) { ids[a], ids[b] = ids[b], ids[a] }
	for i := n - 1; i > 0; i-- {
		// advance LCG
		state = state*6364136223846793005 + 1442695040888963407
		j := int(state>>33) % (i + 1)
		swap(i, j)
	}
	// Guarantee the result is never the identity permutation when n >= 2.
	if n >= 2 && ids[0] == 0 && ids[n-1] == int64(n-1) {
		ids[0], ids[n-1] = ids[n-1], ids[0]
	}
	return ids
}

// ---------------------------------------------------------------------------
// 6. HashCollisionInducing
//    All payloads share the same hash_key value, so any consumer that
//    partitions or shards by hash key will funnel every record into the same
//    bucket, inducing maximum contention.
// ---------------------------------------------------------------------------

// HashCollisionInducing produces payloads that all map to one hash bucket.
type HashCollisionInducing struct{}

func (HashCollisionInducing) Name() string     { return "hash-collision-inducing" }
func (HashCollisionInducing) Category() string { return "concurrency" }

func (HashCollisionInducing) Generate(opts Options) ([][]byte, error) {
	n := defaultCount(opts)
	// "AAAAAAAAAA" hashes to the same bucket under many naive modulo schemes.
	const collisionKey = "AAAAAAAAAA"
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	out := make([][]byte, n)
	for i := range out {
		out[i] = mustMarshal(record{
			Generator: "hash-collision-inducing",
			SeqID:     int64(i),
			UUID:      fmt.Sprintf("00000000-0000-0000-0000-%012d", i+1),
			EventTime: base.Add(time.Duration(i) * time.Second).Format(time.RFC3339Nano),
			HashKey:   collisionKey,
			Payload:   fmt.Sprintf("item-%d", i),
		})
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// 7. NearSimultaneousTimestamps
//    Adjacent payloads differ in event_time by exactly 1 nanosecond. Any
//    consumer that truncates timestamps to microsecond or millisecond
//    resolution will see duplicate timestamps, triggering last-write-wins
//    races or silent drops.
// ---------------------------------------------------------------------------

// NearSimultaneousTimestamps produces payloads whose timestamps differ by 1 ns.
type NearSimultaneousTimestamps struct{}

func (NearSimultaneousTimestamps) Name() string     { return "near-simultaneous-timestamps" }
func (NearSimultaneousTimestamps) Category() string { return "concurrency" }

func (NearSimultaneousTimestamps) Generate(opts Options) ([][]byte, error) {
	n := defaultCount(opts)
	epoch := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	out := make([][]byte, n)
	for i := range out {
		// Each payload is exactly 1 nanosecond after the previous one.
		ts := epoch.Add(time.Duration(i) * time.Nanosecond)
		out[i] = mustMarshal(record{
			Generator: "near-simultaneous-timestamps",
			SeqID:     int64(i),
			UUID:      fmt.Sprintf("00000000-0000-0000-0000-%012d", i+1),
			EventTime: ts.Format(time.RFC3339Nano),
			HashKey:   fmt.Sprintf("hk-%d", i),
			Payload:   fmt.Sprintf("item-%d", i),
		})
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// chaosdata.ChaosGenerator adapters
//
// Each type above implements the package-local ChaosGenerator interface.
// The adapters below wrap them so they also satisfy chaosdata.ChaosGenerator,
// allowing registration in the top-level registry via init().
// ---------------------------------------------------------------------------

type globalAdapter struct {
	inner ChaosGenerator
}

func (a globalAdapter) Name() string     { return a.inner.Name() }
func (a globalAdapter) Category() string { return a.inner.Category() }
func (a globalAdapter) Generate(opts chaosdata.GenerateOpts) (chaosdata.Payload, error) {
	localOpts := Options{Count: opts.Count}
	payloads, err := a.inner.Generate(localOpts)
	if err != nil {
		return chaosdata.Payload{}, err
	}

	// Flatten [][]byte into a single newline-delimited byte slice.
	var total int
	for _, p := range payloads {
		total += len(p) + 1
	}
	flat := make([]byte, 0, total)
	for i, p := range payloads {
		flat = append(flat, p...)
		if i < len(payloads)-1 {
			flat = append(flat, '\n')
		}
	}

	attrs := make(map[string]string, len(opts.Tags)+1)
	for k, v := range opts.Tags {
		attrs[k] = v
	}
	attrs["count"] = fmt.Sprintf("%d", len(payloads))

	return chaosdata.Payload{
		Data:       flat,
		Type:       a.inner.Name(),
		Attributes: attrs,
	}, nil
}

// ---------------------------------------------------------------------------
// init: register all generators in both registries
// ---------------------------------------------------------------------------

func init() {
	generators := []ChaosGenerator{
		IdenticalTimestamps{},
		MonotonicDecreasingSeqIDs{},
		DuplicateUUIDs{},
		InterleavedPartialWrites{},
		OutOfOrderSequenceNumbers{},
		HashCollisionInducing{},
		NearSimultaneousTimestamps{},
	}

	for _, g := range generators {
		Register(g)
		chaosdata.Register(globalAdapter{inner: g})
	}
}
