package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// ReplayFromManifest loads a JSONL manifest and replays the exact same mutations
// in the same order. This is used to reproduce specific failure patterns.
func (e *Engine) ReplayFromManifest(ctx context.Context, manifest []byte) ([]types.MutationRecord, error) {
	events, err := parseManifest(manifest)
	if err != nil {
		return nil, fmt.Errorf("replay: %w", err)
	}

	var records []types.MutationRecord

	for i, event := range events {
		if err := ctx.Err(); err != nil {
			return records, fmt.Errorf("replay: %w", err)
		}

		// Look up the mutation by type from the registry.
		m, err := e.mutations.Get(event.Mutation)
		if err != nil {
			return records, fmt.Errorf("replay: event %d (%s): %w", i, event.ID, err)
		}

		// Build a DataObject from the event target.
		obj := types.DataObject{
			Key: event.Target,
		}

		// Apply the mutation with the original params.
		record, err := m.Apply(ctx, obj, e.transport, event.Params, e.clock)
		if err != nil {
			return records, fmt.Errorf("replay: event %d (%s): apply %q: %w", i, event.ID, event.Mutation, err)
		}

		records = append(records, record)
	}

	return records, nil
}

// parseManifest parses a JSONL-formatted byte slice into a slice of ChaosEvent.
// Each JSON object in the stream must be a valid ChaosEvent.
func parseManifest(data []byte) ([]types.ChaosEvent, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	var events []types.ChaosEvent
	for dec.More() {
		var event types.ChaosEvent
		if err := dec.Decode(&event); err != nil {
			return nil, fmt.Errorf("parse manifest event: %w", err)
		}
		events = append(events, event)
	}
	return events, nil
}
