package mutation

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// DuplicateMutation writes a data object twice: once to the original key and
// once to a key with a ".dup" suffix, simulating duplicate data delivery.
type DuplicateMutation struct{}

// Type returns "duplicate".
func (d *DuplicateMutation) Type() string { return "duplicate" }

// Apply reads the object and writes it to both the original key and a
// duplicate key with a ".dup" suffix.
// Params:
//   - "dup_pct" (optional, default "100"): percentage of records to duplicate in JSONL.
//   - "exact" (optional, default "true"): "true" for exact duplicate, "false" for near-duplicate.
func (d *DuplicateMutation) Apply(ctx context.Context, obj types.DataObject, transport adapter.DataTransport, params map[string]string, clock adapter.Clock) (types.MutationRecord, error) {
	reader, err := transport.Read(ctx, obj.Key)
	if err != nil {
		err = fmt.Errorf("duplicate mutation: read failed: %w", err)
		return types.MutationRecord{Applied: false, Mutation: "duplicate", Error: err.Error()}, err
	}
	defer reader.Close()

	data, err := io.ReadAll(reader)
	if err != nil {
		err = fmt.Errorf("duplicate mutation: read data failed: %w", err)
		return types.MutationRecord{Applied: false, Mutation: "duplicate", Error: err.Error()}, err
	}

	// Write to the original key.
	if err := transport.Write(ctx, obj.Key, bytes.NewReader(data)); err != nil {
		err = fmt.Errorf("duplicate mutation: write original failed: %w", err)
		return types.MutationRecord{Applied: false, Mutation: "duplicate", Error: err.Error()}, err
	}

	// Write to the duplicate key.
	dupKey := obj.Key + ".dup"
	if err := transport.Write(ctx, dupKey, bytes.NewReader(data)); err != nil {
		err = fmt.Errorf("duplicate mutation: write duplicate failed: %w", err)
		return types.MutationRecord{Applied: false, Mutation: "duplicate", Error: err.Error()}, err
	}

	return types.MutationRecord{
		ObjectKey: obj.Key,
		Mutation:  "duplicate",
		Params:    params,
		Applied:   true,
		Timestamp: clock.Now(),
	}, nil
}
