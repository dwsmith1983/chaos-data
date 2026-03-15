package mutation

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// OutOfOrderMutation delays an older partition so that newer data arrives first,
// simulating out-of-order data delivery from upstream systems.
type OutOfOrderMutation struct{}

// Type returns "out-of-order".
func (o *OutOfOrderMutation) Type() string { return "out-of-order" }

// Apply executes the out-of-order mutation.
// Required params:
//   - "delay_older_by": Go duration string for how long to hold the older partition.
//   - "partition_field": the Hive-style partition field name (e.g. "par_hour").
//   - "older_value": the value of the older partition that should be delayed (e.g. "14").
//   - "newer_value": the value of the newer partition that passes through (e.g. "15").
//
// If obj.Key contains "partition_field=older_value", transport.Hold is called and
// Applied=true is returned. Otherwise, Applied=false is returned with no error —
// the newer partition passes through normally.
func (o *OutOfOrderMutation) Apply(ctx context.Context, obj types.DataObject, transport adapter.DataTransport, params map[string]string) (types.MutationRecord, error) {
	delayStr, ok := params["delay_older_by"]
	if !ok || delayStr == "" {
		err := fmt.Errorf("out-of-order mutation: missing required param \"delay_older_by\"")
		return types.MutationRecord{Applied: false, Mutation: "out-of-order", Error: err.Error()}, err
	}

	partitionField, ok := params["partition_field"]
	if !ok || partitionField == "" {
		err := fmt.Errorf("out-of-order mutation: missing required param \"partition_field\"")
		return types.MutationRecord{Applied: false, Mutation: "out-of-order", Error: err.Error()}, err
	}

	olderValue, ok := params["older_value"]
	if !ok || olderValue == "" {
		err := fmt.Errorf("out-of-order mutation: missing required param \"older_value\"")
		return types.MutationRecord{Applied: false, Mutation: "out-of-order", Error: err.Error()}, err
	}

	// newer_value is validated for completeness — the mutation only acts on the older
	// partition, but callers are expected to supply both values so the intent is explicit.
	_, ok = params["newer_value"]
	if !ok {
		err := fmt.Errorf("out-of-order mutation: missing required param \"newer_value\"")
		return types.MutationRecord{Applied: false, Mutation: "out-of-order", Error: err.Error()}, err
	}

	duration, err := time.ParseDuration(delayStr)
	if err != nil {
		err = fmt.Errorf("out-of-order mutation: invalid delay_older_by %q: %w", delayStr, err)
		return types.MutationRecord{Applied: false, Mutation: "out-of-order", Error: err.Error()}, err
	}

	// Check if this object belongs to the older partition using Hive-style key matching.
	// We must verify a delimiter (or end-of-string) follows the match to avoid false
	// positives like "par_hour=140" matching "par_hour=14".
	olderPartition := partitionField + "=" + olderValue
	idx := strings.Index(obj.Key, olderPartition)
	afterPos := idx + len(olderPartition)
	if idx < 0 || (afterPos < len(obj.Key) && !isPartitionDelimiter(obj.Key[afterPos])) {
		// Newer partition or unrelated object — pass through without error.
		return types.MutationRecord{
			ObjectKey: obj.Key,
			Mutation:  "out-of-order",
			Params:    params,
			Applied:   false,
			Timestamp: time.Now(),
		}, nil
	}

	releaseAt := time.Now().Add(duration)
	if err := transport.Hold(ctx, obj.Key, releaseAt); err != nil {
		err = fmt.Errorf("out-of-order mutation: hold failed: %w", err)
		return types.MutationRecord{Applied: false, Mutation: "out-of-order", Error: err.Error()}, err
	}

	return types.MutationRecord{
		ObjectKey: obj.Key,
		Mutation:  "out-of-order",
		Params:    params,
		Applied:   true,
		Timestamp: time.Now(),
	}, nil
}

// isPartitionDelimiter reports whether b is a Hive-style partition path delimiter.
// Valid delimiters are '/', '_', '.', and '-', which separate partition values from
// subsequent path components or filename parts.
func isPartitionDelimiter(b byte) bool {
	return b == '/' || b == '_' || b == '.' || b == '-'
}
