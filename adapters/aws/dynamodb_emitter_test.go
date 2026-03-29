package aws_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	chaosaws "github.com/dwsmith1983/chaos-data/adapters/aws"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

func TestDynamoDBEmitter_Emit_DelegatesToWriteEvent(t *testing.T) {
	t.Parallel()

	var captured map[string]dynamodbtypes.AttributeValue

	mock := &mockDynamoDBAPI{
		PutItemFn: func(_ context.Context, input *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
			captured = input.Item
			return &dynamodb.PutItemOutput{}, nil
		},
	}

	emitter, err := chaosaws.NewDynamoDBEmitter(mock, "test-events-table")
	if err != nil {
		t.Fatalf("NewDynamoDBEmitter() error = %v", err)
	}

	event := types.ChaosEvent{
		ID:           "test-event-1",
		ExperimentID: "exp-001",
		Scenario:     "late-data-bronze",
		Category:     "data-arrival",
		Severity:     types.SeverityModerate,
		Target:       "raw/cdr/2026-03-29/file001.parquet",
		Mutation:     "delay",
		Params:       map[string]string{"duration": "30m"},
		Timestamp:    time.Date(2026, 3, 29, 14, 30, 0, 0, time.UTC),
		Mode:         "deterministic",
	}

	if err := emitter.Emit(context.Background(), event); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}

	if captured == nil {
		t.Fatal("expected PutItem to be called")
	}

	// Verify the PK follows CHAOS# convention.
	pk, ok := captured["PK"].(*dynamodbtypes.AttributeValueMemberS)
	if !ok {
		t.Fatal("PK is not a string attribute")
	}
	if pk.Value != "CHAOS#exp-001" {
		t.Errorf("PK = %q, want %q", pk.Value, "CHAOS#exp-001")
	}

	// Verify scenario field.
	sc, ok := captured["scenario"].(*dynamodbtypes.AttributeValueMemberS)
	if !ok {
		t.Fatal("scenario is not a string attribute")
	}
	if sc.Value != "late-data-bronze" {
		t.Errorf("scenario = %q, want %q", sc.Value, "late-data-bronze")
	}
}

func TestDynamoDBEmitter_Emit_PropagatesError(t *testing.T) {
	t.Parallel()

	errDynamo := errors.New("simulated DynamoDB failure")

	mock := &mockDynamoDBAPI{
		PutItemFn: func(_ context.Context, _ *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
			return nil, errDynamo
		},
	}

	emitter, constructErr := chaosaws.NewDynamoDBEmitter(mock, "test-events-table")
	if constructErr != nil {
		t.Fatalf("NewDynamoDBEmitter() error = %v", constructErr)
	}

	event := types.ChaosEvent{
		ID:           "test-event-2",
		ExperimentID: "exp-002",
		Scenario:     "post-run-drift",
		Category:     "data-quality",
		Severity:     types.SeverityModerate,
		Mutation:     "post-run-drift",
		Timestamp:    time.Now(),
		Mode:         "deterministic",
	}

	err := emitter.Emit(context.Background(), event)
	if err == nil {
		t.Fatal("expected error from Emit when DynamoDB fails")
	}
	if !errors.Is(err, errDynamo) {
		t.Fatalf("Emit() error = %v, want wrapping %v", err, errDynamo)
	}
}
