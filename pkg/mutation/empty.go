package mutation

import (
	"bufio"
	"bytes"
	"context"
	"fmt"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// EmptyMutation replaces the contents of a data object with empty data,
// optionally preserving the header line.
type EmptyMutation struct{}

// Type returns "empty".
func (e *EmptyMutation) Type() string { return "empty" }

// Apply replaces the object content with empty data.
// Params:
//   - "preserve_header" (optional, default "false"): "true" to keep the first line.
func (e *EmptyMutation) Apply(ctx context.Context, obj types.DataObject, transport adapter.DataTransport, params map[string]string, clock adapter.Clock) (types.MutationRecord, error) {
	preserveHeader := params["preserve_header"] == "true"

	var writeData []byte

	if preserveHeader {
		reader, err := transport.Read(ctx, obj.Key)
		if err != nil {
			err = fmt.Errorf("empty mutation: read failed: %w", err)
			return types.MutationRecord{Applied: false, Mutation: "empty", Error: err.Error()}, err
		}
		defer reader.Close()

		scanner := bufio.NewScanner(reader)
		if scanner.Scan() {
			writeData = append([]byte(scanner.Text()), '\n')
		}
	}

	if err := transport.Write(ctx, obj.Key, bytes.NewReader(writeData)); err != nil {
		err = fmt.Errorf("empty mutation: write failed: %w", err)
		return types.MutationRecord{Applied: false, Mutation: "empty", Error: err.Error()}, err
	}

	return types.MutationRecord{
		ObjectKey: obj.Key,
		Mutation:  "empty",
		Params:    params,
		Applied:   true,
		Timestamp: clock.Now(),
	}, nil
}
