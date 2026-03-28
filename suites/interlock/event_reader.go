package interlocksuite

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	chaosaws "github.com/dwsmith1983/chaos-data/adapters/aws"
)

// InterlockEventRecord represents an Interlock-emitted event.
type InterlockEventRecord struct {
	PipelineID string                 `json:"pipeline_id"`
	EventType  string                 `json:"event_type"`
	Timestamp  time.Time              `json:"timestamp"`
	Detail     map[string]interface{} `json:"detail,omitempty"`
}

// InterlockEventReader reads Interlock-emitted events.
type InterlockEventReader interface {
	ReadEvents(ctx context.Context, pipeline string, eventType string) ([]InterlockEventRecord, error)
	Reset()
}

// ---------------------------------------------------------------------------
// Local (in-memory) implementation
// ---------------------------------------------------------------------------

// LocalEventReader stores events in memory. Thread-safe.
// The LocalInterlockEvaluator writes events here after rule evaluation.
type LocalEventReader struct {
	mu     sync.Mutex
	events []InterlockEventRecord
}

// NewLocalEventReader returns a new empty LocalEventReader.
func NewLocalEventReader() *LocalEventReader {
	return &LocalEventReader{}
}

// Emit adds an event. Called by LocalInterlockEvaluator.
func (r *LocalEventReader) Emit(event InterlockEventRecord) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.events = append(r.events, event)
}

// ReadEvents returns events matching the pipeline and optional event type filter.
// If eventType is empty, returns all events for the pipeline.
func (r *LocalEventReader) ReadEvents(_ context.Context, pipeline string, eventType string) ([]InterlockEventRecord, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	var matched []InterlockEventRecord
	for _, e := range r.events {
		if e.PipelineID != pipeline {
			continue
		}
		if eventType != "" && e.EventType != eventType {
			continue
		}
		matched = append(matched, e)
	}

	return matched, nil
}

// Reset clears all events. Used between scenarios for isolation.
func (r *LocalEventReader) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.events = nil
}

// ---------------------------------------------------------------------------
// AWS (DynamoDB) implementation
// ---------------------------------------------------------------------------

// AWSEventReader reads interlock events from a DynamoDB events table.
// The events table is populated by Interlock's event-sink Lambda from EventBridge.
type AWSEventReader struct {
	api       chaosaws.DynamoDBAPI
	tableName string
}

// NewAWSEventReader returns a new AWSEventReader that queries the given table.
func NewAWSEventReader(api chaosaws.DynamoDBAPI, tableName string) *AWSEventReader {
	return &AWSEventReader{api: api, tableName: tableName}
}

// ReadEvents queries the DynamoDB events table for events matching the pipeline.
// If eventType is non-empty, a FilterExpression restricts results to that type.
func (r *AWSEventReader) ReadEvents(ctx context.Context, pipeline string, eventType string) ([]InterlockEventRecord, error) {
	pk := chaosaws.InterlockEventPK(pipeline)
	keyExpr := "PK = :pk"
	exprValues := map[string]dynamodbtypes.AttributeValue{
		":pk": &dynamodbtypes.AttributeValueMemberS{Value: pk},
	}

	input := &dynamodb.QueryInput{
		TableName:                 &r.tableName,
		KeyConditionExpression:    &keyExpr,
		ExpressionAttributeValues: exprValues,
	}

	if eventType != "" {
		filterExpr := "eventType = :et"
		input.FilterExpression = &filterExpr
		input.ExpressionAttributeValues[":et"] = &dynamodbtypes.AttributeValueMemberS{Value: eventType}
	}

	out, err := r.api.Query(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("aws event reader query: %w", err)
	}

	records := make([]InterlockEventRecord, 0, len(out.Items))
	for _, item := range out.Items {
		rec, err := unmarshalInterlockEvent(item)
		if err != nil {
			return nil, fmt.Errorf("aws event reader unmarshal: %w", err)
		}
		records = append(records, rec)
	}
	return records, nil
}

// Reset is a no-op for AWSEventReader — events are managed externally.
func (r *AWSEventReader) Reset() {}

// unmarshalInterlockEvent converts a DynamoDB item map to an InterlockEventRecord.
func unmarshalInterlockEvent(item map[string]dynamodbtypes.AttributeValue) (InterlockEventRecord, error) {
	var rec InterlockEventRecord

	if v, ok := item["pipelineID"]; ok {
		if sv, ok := v.(*dynamodbtypes.AttributeValueMemberS); ok {
			rec.PipelineID = sv.Value
		}
	}
	if v, ok := item["eventType"]; ok {
		if sv, ok := v.(*dynamodbtypes.AttributeValueMemberS); ok {
			rec.EventType = sv.Value
		}
	}
	if v, ok := item["timestamp"]; ok {
		if sv, ok := v.(*dynamodbtypes.AttributeValueMemberS); ok {
			t, err := time.Parse(time.RFC3339Nano, sv.Value)
			if err != nil {
				return InterlockEventRecord{}, fmt.Errorf("parse timestamp: %w", err)
			}
			rec.Timestamp = t
		}
	}
	if v, ok := item["detail"]; ok {
		if mv, ok := v.(*dynamodbtypes.AttributeValueMemberM); ok {
			rec.Detail = make(map[string]interface{}, len(mv.Value))
			for k, av := range mv.Value {
				if sv, ok := av.(*dynamodbtypes.AttributeValueMemberS); ok {
					rec.Detail[k] = sv.Value
				}
			}
		}
	}

	return rec, nil
}
