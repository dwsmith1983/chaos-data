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

// MultiDayMutation writes copies of a data object under multiple date-prefixed
// keys, simulating data delivered across multiple days.
type MultiDayMutation struct{}

// Type returns "multi-day".
func (m *MultiDayMutation) Type() string { return "multi-day" }

// Apply reads the object and writes it to date-prefixed keys for each day.
// Params:
//   - "days" (required): comma-separated date strings like "2024-01-15,2024-01-16".
//   - "prefix" (optional): key prefix to prepend before the date partition.
func (m *MultiDayMutation) Apply(ctx context.Context, obj types.DataObject, transport adapter.DataTransport, params map[string]string) (types.MutationRecord, error) {
	daysStr, ok := params["days"]
	if !ok || daysStr == "" {
		err := fmt.Errorf("multi-day mutation: missing required param \"days\"")
		return types.MutationRecord{Applied: false, Mutation: "multi-day", Error: err.Error()}, err
	}

	reader, err := transport.Read(ctx, obj.Key)
	if err != nil {
		err = fmt.Errorf("multi-day mutation: read failed: %w", err)
		return types.MutationRecord{Applied: false, Mutation: "multi-day", Error: err.Error()}, err
	}
	defer reader.Close()

	data, err := io.ReadAll(reader)
	if err != nil {
		err = fmt.Errorf("multi-day mutation: read data failed: %w", err)
		return types.MutationRecord{Applied: false, Mutation: "multi-day", Error: err.Error()}, err
	}

	days := strings.Split(daysStr, ",")
	prefix := params["prefix"]

	for _, day := range days {
		day = strings.TrimSpace(day)
		var writeKey string
		if prefix != "" {
			writeKey = fmt.Sprintf("%s/date=%s/%s", prefix, day, obj.Key)
		} else {
			writeKey = fmt.Sprintf("date=%s/%s", day, obj.Key)
		}
		if err := transport.Write(ctx, writeKey, bytes.NewReader(data)); err != nil {
			err = fmt.Errorf("multi-day mutation: write failed for day %s: %w", day, err)
			return types.MutationRecord{Applied: false, Mutation: "multi-day", Error: err.Error()}, err
		}
	}

	return types.MutationRecord{
		ObjectKey: obj.Key,
		Mutation:  "multi-day",
		Params:    params,
		Applied:   true,
		Timestamp: time.Now(),
	}, nil
}
