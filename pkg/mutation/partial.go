package mutation

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// PartialMutation delivers only a percentage of the original data bytes,
// simulating truncated or incomplete data delivery.
type PartialMutation struct{}

// Type returns "partial".
func (p *PartialMutation) Type() string { return "partial" }

// Apply reads the object and writes only a percentage of its bytes.
// Params:
//   - "delivery_pct" (required): percentage of bytes to deliver (0-100).
func (p *PartialMutation) Apply(ctx context.Context, obj types.DataObject, transport adapter.DataTransport, params map[string]string) (types.MutationRecord, error) {
	pctStr, ok := params["delivery_pct"]
	if !ok || pctStr == "" {
		err := fmt.Errorf("partial mutation: missing required param \"delivery_pct\"")
		return types.MutationRecord{Applied: false, Mutation: "partial", Error: err.Error()}, err
	}

	pct, err := strconv.Atoi(pctStr)
	if err != nil {
		err = fmt.Errorf("partial mutation: invalid delivery_pct %q: %w", pctStr, err)
		return types.MutationRecord{Applied: false, Mutation: "partial", Error: err.Error()}, err
	}
	if pct < 0 || pct > 100 {
		err := fmt.Errorf("partial mutation: delivery_pct must be 0-100, got %d", pct)
		return types.MutationRecord{Applied: false, Mutation: "partial", Error: err.Error()}, err
	}

	reader, err := transport.Read(ctx, obj.Key)
	if err != nil {
		err = fmt.Errorf("partial mutation: read failed: %w", err)
		return types.MutationRecord{Applied: false, Mutation: "partial", Error: err.Error()}, err
	}
	defer reader.Close()

	data, err := io.ReadAll(reader)
	if err != nil {
		err = fmt.Errorf("partial mutation: read data failed: %w", err)
		return types.MutationRecord{Applied: false, Mutation: "partial", Error: err.Error()}, err
	}

	deliverBytes := int64(pct) * int64(len(data)) / 100
	truncated := data[:deliverBytes]

	if err := transport.Write(ctx, obj.Key, bytes.NewReader(truncated)); err != nil {
		err = fmt.Errorf("partial mutation: write failed: %w", err)
		return types.MutationRecord{Applied: false, Mutation: "partial", Error: err.Error()}, err
	}

	return types.MutationRecord{
		ObjectKey: obj.Key,
		Mutation:  "partial",
		Params:    params,
		Applied:   true,
		Timestamp: time.Now(),
	}, nil
}
