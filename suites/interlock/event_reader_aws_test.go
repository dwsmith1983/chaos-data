package interlocksuite

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	chaosaws "github.com/dwsmith1983/chaos-data/adapters/aws"
)

// mockDynamoDBAPI implements chaosaws.DynamoDBAPI for testing AWSEventReader.
type mockDynamoDBAPI struct {
	QueryFn func(ctx context.Context, params *dynamodb.QueryInput, optFns ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error)
}

// compile-time check that mockDynamoDBAPI satisfies chaosaws.DynamoDBAPI.
var _ chaosaws.DynamoDBAPI = (*mockDynamoDBAPI)(nil)

func (m *mockDynamoDBAPI) GetItem(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	return &dynamodb.GetItemOutput{}, nil
}

func (m *mockDynamoDBAPI) PutItem(_ context.Context, _ *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	return &dynamodb.PutItemOutput{}, nil
}

func (m *mockDynamoDBAPI) DeleteItem(_ context.Context, _ *dynamodb.DeleteItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
	return &dynamodb.DeleteItemOutput{}, nil
}

func (m *mockDynamoDBAPI) Query(ctx context.Context, params *dynamodb.QueryInput, optFns ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
	if m.QueryFn != nil {
		return m.QueryFn(ctx, params, optFns...)
	}
	return &dynamodb.QueryOutput{}, nil
}

func (m *mockDynamoDBAPI) Scan(_ context.Context, _ *dynamodb.ScanInput, _ ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error) {
	return &dynamodb.ScanOutput{}, nil
}

func (m *mockDynamoDBAPI) BatchWriteItem(_ context.Context, _ *dynamodb.BatchWriteItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error) {
	return &dynamodb.BatchWriteItemOutput{}, nil
}

func TestAWSEventReader_ReadEvents(t *testing.T) {
	t.Parallel()

	ts := time.Date(2026, 3, 28, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name      string
		pipeline  string
		eventType string
		queryOut  *dynamodb.QueryOutput
		queryErr  error
		wantLen   int
		wantType  string
		wantErr   bool
	}{
		{
			name:     "returns matching events",
			pipeline: "my-pipeline",
			queryOut: &dynamodb.QueryOutput{
				Items: []map[string]dynamodbtypes.AttributeValue{
					{
						"pipelineID": &dynamodbtypes.AttributeValueMemberS{Value: "my-pipeline"},
						"eventType":  &dynamodbtypes.AttributeValueMemberS{Value: "JOB_TRIGGERED"},
						"timestamp":  &dynamodbtypes.AttributeValueMemberS{Value: ts.Format(time.RFC3339Nano)},
					},
					{
						"pipelineID": &dynamodbtypes.AttributeValueMemberS{Value: "my-pipeline"},
						"eventType":  &dynamodbtypes.AttributeValueMemberS{Value: "VALIDATION_EXHAUSTED"},
						"timestamp":  &dynamodbtypes.AttributeValueMemberS{Value: ts.Format(time.RFC3339Nano)},
					},
				},
			},
			wantLen:  2,
			wantType: "JOB_TRIGGERED",
		},
		{
			name:      "filters by eventType when provided",
			pipeline:  "my-pipeline",
			eventType: "JOB_TRIGGERED",
			queryOut: &dynamodb.QueryOutput{
				Items: []map[string]dynamodbtypes.AttributeValue{
					{
						"pipelineID": &dynamodbtypes.AttributeValueMemberS{Value: "my-pipeline"},
						"eventType":  &dynamodbtypes.AttributeValueMemberS{Value: "JOB_TRIGGERED"},
						"timestamp":  &dynamodbtypes.AttributeValueMemberS{Value: ts.Format(time.RFC3339Nano)},
					},
				},
			},
			wantLen:  1,
			wantType: "JOB_TRIGGERED",
		},
		{
			name:     "returns empty when no results",
			pipeline: "empty-pipeline",
			queryOut: &dynamodb.QueryOutput{
				Items: []map[string]dynamodbtypes.AttributeValue{},
			},
			wantLen: 0,
		},
		{
			name:     "returns error on query failure",
			pipeline: "fail-pipeline",
			queryErr: errors.New("dynamodb unavailable"),
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var capturedInput *dynamodb.QueryInput
			mock := &mockDynamoDBAPI{
				QueryFn: func(_ context.Context, params *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
					capturedInput = params
					if tt.queryErr != nil {
						return nil, tt.queryErr
					}
					return tt.queryOut, nil
				},
			}

			reader := NewAWSEventReader(mock, "test-events-table")
			records, err := reader.ReadEvents(context.Background(), tt.pipeline, tt.eventType)

			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if len(records) != tt.wantLen {
				t.Fatalf("got %d records, want %d", len(records), tt.wantLen)
			}

			// Verify the query used the correct PK.
			if capturedInput == nil {
				t.Fatal("query was not called")
			}
			wantPK := chaosaws.InterlockEventPK(tt.pipeline)
			pkVal, ok := capturedInput.ExpressionAttributeValues[":pk"]
			if !ok {
				t.Fatal("missing :pk in ExpressionAttributeValues")
			}
			pkStr, ok := pkVal.(*dynamodbtypes.AttributeValueMemberS)
			if !ok || pkStr.Value != wantPK {
				t.Fatalf("PK = %q, want %q", pkStr.Value, wantPK)
			}

			// Verify filter expression is set when eventType is provided.
			if tt.eventType != "" {
				if capturedInput.FilterExpression == nil {
					t.Fatal("expected FilterExpression to be set")
				}
				etVal, ok := capturedInput.ExpressionAttributeValues[":et"]
				if !ok {
					t.Fatal("missing :et in ExpressionAttributeValues")
				}
				etStr, ok := etVal.(*dynamodbtypes.AttributeValueMemberS)
				if !ok || etStr.Value != tt.eventType {
					t.Fatalf("eventType filter = %q, want %q", etStr.Value, tt.eventType)
				}
			} else {
				if capturedInput.FilterExpression != nil {
					t.Fatal("expected FilterExpression to be nil when eventType is empty")
				}
			}

			// Verify first record content when we have results.
			if tt.wantLen > 0 && tt.wantType != "" {
				if records[0].EventType != tt.wantType {
					t.Fatalf("records[0].EventType = %q, want %q", records[0].EventType, tt.wantType)
				}
				if records[0].PipelineID != tt.pipeline {
					t.Fatalf("records[0].PipelineID = %q, want %q", records[0].PipelineID, tt.pipeline)
				}
				if !records[0].Timestamp.Equal(ts) {
					t.Fatalf("records[0].Timestamp = %v, want %v", records[0].Timestamp, ts)
				}
			}
		})
	}
}

func TestAWSEventReader_ReadEvents_WithDetail(t *testing.T) {
	t.Parallel()

	ts := time.Date(2026, 3, 28, 12, 0, 0, 0, time.UTC)
	mock := &mockDynamoDBAPI{
		QueryFn: func(_ context.Context, _ *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			return &dynamodb.QueryOutput{
				Items: []map[string]dynamodbtypes.AttributeValue{
					{
						"pipelineID": &dynamodbtypes.AttributeValueMemberS{Value: "p1"},
						"eventType":  &dynamodbtypes.AttributeValueMemberS{Value: "JOB_TRIGGERED"},
						"timestamp":  &dynamodbtypes.AttributeValueMemberS{Value: ts.Format(time.RFC3339Nano)},
						"detail": &dynamodbtypes.AttributeValueMemberM{
							Value: map[string]dynamodbtypes.AttributeValue{
								"schedule": &dynamodbtypes.AttributeValueMemberS{Value: "daily"},
								"reason":   &dynamodbtypes.AttributeValueMemberS{Value: "all_valid"},
							},
						},
					},
				},
			}, nil
		},
	}

	reader := NewAWSEventReader(mock, "test-table")
	records, err := reader.ReadEvents(context.Background(), "p1", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(records) != 1 {
		t.Fatalf("got %d records, want 1", len(records))
	}

	rec := records[0]
	if rec.Detail == nil {
		t.Fatal("expected detail map, got nil")
	}
	if rec.Detail["schedule"] != "daily" {
		t.Fatalf("detail[schedule] = %v, want %q", rec.Detail["schedule"], "daily")
	}
	if rec.Detail["reason"] != "all_valid" {
		t.Fatalf("detail[reason] = %v, want %q", rec.Detail["reason"], "all_valid")
	}
}

func TestAWSEventReader_Reset(t *testing.T) {
	t.Parallel()

	mock := &mockDynamoDBAPI{}
	reader := NewAWSEventReader(mock, "test-table")

	// Reset should not panic or return an error — it is a no-op.
	reader.Reset()
}
