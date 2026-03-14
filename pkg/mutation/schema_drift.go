package mutation

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// SchemaDriftMutation modifies the schema of JSONL records by adding columns,
// removing columns, or changing column types.
type SchemaDriftMutation struct{}

// Type returns "schema-drift".
func (s *SchemaDriftMutation) Type() string { return "schema-drift" }

// Apply reads JSONL data and modifies its schema based on the provided params.
// Params:
//   - "add_columns" (optional): comma-separated column names to add with null values.
//   - "remove_columns" (optional): comma-separated column names to remove.
//   - "change_types" (optional): comma-separated "col:newtype" pairs.
//     Currently only "col:string" is supported, converting any value to its string representation.
func (s *SchemaDriftMutation) Apply(ctx context.Context, obj types.DataObject, transport adapter.DataTransport, params map[string]string) (types.MutationRecord, error) {
	reader, err := transport.Read(ctx, obj.Key)
	if err != nil {
		err = fmt.Errorf("schema-drift mutation: read failed: %w", err)
		return types.MutationRecord{Applied: false, Mutation: "schema-drift", Error: err.Error()}, err
	}
	defer reader.Close()

	data, err := io.ReadAll(reader)
	if err != nil {
		err = fmt.Errorf("schema-drift mutation: read data failed: %w", err)
		return types.MutationRecord{Applied: false, Mutation: "schema-drift", Error: err.Error()}, err
	}

	records, err := parseJSONLRecords(data)
	if err != nil {
		err = fmt.Errorf("schema-drift mutation: parse failed: %w", err)
		return types.MutationRecord{Applied: false, Mutation: "schema-drift", Error: err.Error()}, err
	}

	// Add columns.
	if addCols := params["add_columns"]; addCols != "" {
		cols := strings.Split(addCols, ",")
		for i, rec := range records {
			updated := copyRecord(rec)
			for _, col := range cols {
				col = strings.TrimSpace(col)
				if col != "" {
					updated[col] = nil
				}
			}
			records[i] = updated
		}
	}

	// Remove columns.
	if removeCols := params["remove_columns"]; removeCols != "" {
		cols := strings.Split(removeCols, ",")
		for i, rec := range records {
			updated := copyRecord(rec)
			for _, col := range cols {
				col = strings.TrimSpace(col)
				delete(updated, col)
			}
			records[i] = updated
		}
	}

	// Change types.
	if changeTypes := params["change_types"]; changeTypes != "" {
		pairs := strings.Split(changeTypes, ",")
		for i, rec := range records {
			updated := copyRecord(rec)
			for _, pair := range pairs {
				parts := strings.SplitN(strings.TrimSpace(pair), ":", 2)
				if len(parts) != 2 {
					continue
				}
				col := parts[0]
				newType := parts[1]
				if newType == "string" {
					if val, ok := updated[col]; ok {
						updated[col] = fmt.Sprintf("%v", val)
					}
				}
			}
			records[i] = updated
		}
	}

	output, err := marshalJSONL(records)
	if err != nil {
		err = fmt.Errorf("schema-drift mutation: marshal failed: %w", err)
		return types.MutationRecord{Applied: false, Mutation: "schema-drift", Error: err.Error()}, err
	}

	if err := transport.Write(ctx, obj.Key, bytes.NewReader(output)); err != nil {
		err = fmt.Errorf("schema-drift mutation: write failed: %w", err)
		return types.MutationRecord{Applied: false, Mutation: "schema-drift", Error: err.Error()}, err
	}

	return types.MutationRecord{
		ObjectKey: obj.Key,
		Mutation:  "schema-drift",
		Params:    params,
		Applied:   true,
		Timestamp: time.Now(),
	}, nil
}

// copyRecord returns a shallow copy of the record map.
func copyRecord(rec map[string]interface{}) map[string]interface{} {
	out := make(map[string]interface{}, len(rec))
	for k, v := range rec {
		out[k] = v
	}
	return out
}
