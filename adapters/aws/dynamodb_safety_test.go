package aws_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	chaosaws "github.com/dwsmith1983/chaos-data/adapters/aws"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

const testSafetyTable = "chaos-control"

func newSafety(api *mockDynamoDBAPI) *chaosaws.DynamoDBSafety {
	return chaosaws.NewDynamoDBSafety(api, testSafetyTable)
}

// ---------------------------------------------------------------------------
// IsEnabled
// ---------------------------------------------------------------------------

func TestDynamoDBSafety_IsEnabled_True(t *testing.T) {
	t.Parallel()

	api := &mockDynamoDBAPI{
		GetItemFn: func(_ context.Context, in *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{
				Item: map[string]dtypes.AttributeValue{
					"PK":    &dtypes.AttributeValueMemberS{Value: "CONTROL#chaos-enabled"},
					"SK":    &dtypes.AttributeValueMemberS{Value: "CONTROL#chaos-enabled"},
					"value": &dtypes.AttributeValueMemberS{Value: "true"},
				},
			}, nil
		},
	}

	got, err := newSafety(api).IsEnabled(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !got {
		t.Error("expected IsEnabled to return true")
	}
}

func TestDynamoDBSafety_IsEnabled_False(t *testing.T) {
	t.Parallel()

	api := &mockDynamoDBAPI{
		GetItemFn: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{
				Item: map[string]dtypes.AttributeValue{
					"PK":    &dtypes.AttributeValueMemberS{Value: "CONTROL#chaos-enabled"},
					"SK":    &dtypes.AttributeValueMemberS{Value: "CONTROL#chaos-enabled"},
					"value": &dtypes.AttributeValueMemberS{Value: "false"},
				},
			}, nil
		},
	}

	got, err := newSafety(api).IsEnabled(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got {
		t.Error("expected IsEnabled to return false")
	}
}

func TestDynamoDBSafety_IsEnabled_Missing(t *testing.T) {
	t.Parallel()

	api := &mockDynamoDBAPI{
		GetItemFn: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{Item: nil}, nil
		},
	}

	got, err := newSafety(api).IsEnabled(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got {
		t.Error("expected IsEnabled to return false (fail-safe) when record is missing")
	}
}

func TestDynamoDBSafety_IsEnabled_Error(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("dynamo timeout")
	api := &mockDynamoDBAPI{
		GetItemFn: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return nil, wantErr
		},
	}

	got, err := newSafety(api).IsEnabled(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, wantErr) {
		t.Errorf("expected error wrapping %v, got %v", wantErr, err)
	}
	if got {
		t.Error("expected IsEnabled to return false (fail-safe) on error")
	}
}

// ---------------------------------------------------------------------------
// MaxSeverity
// ---------------------------------------------------------------------------

func TestDynamoDBSafety_MaxSeverity_Happy(t *testing.T) {
	t.Parallel()

	api := &mockDynamoDBAPI{
		GetItemFn: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{
				Item: map[string]dtypes.AttributeValue{
					"PK":    &dtypes.AttributeValueMemberS{Value: "CONTROL#max-severity"},
					"SK":    &dtypes.AttributeValueMemberS{Value: "CONTROL#max-severity"},
					"value": &dtypes.AttributeValueMemberS{Value: "moderate"},
				},
			}, nil
		},
	}

	got, err := newSafety(api).MaxSeverity(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != types.SeverityModerate {
		t.Errorf("got %v, want SeverityModerate", got)
	}
}

func TestDynamoDBSafety_MaxSeverity_Missing(t *testing.T) {
	t.Parallel()

	api := &mockDynamoDBAPI{
		GetItemFn: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{Item: nil}, nil
		},
	}

	got, err := newSafety(api).MaxSeverity(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != types.SeverityLow {
		t.Errorf("got %v, want SeverityLow (fail-safe)", got)
	}
}

func TestDynamoDBSafety_MaxSeverity_InvalidValue(t *testing.T) {
	t.Parallel()

	api := &mockDynamoDBAPI{
		GetItemFn: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{
				Item: map[string]dtypes.AttributeValue{
					"PK":    &dtypes.AttributeValueMemberS{Value: "CONTROL#max-severity"},
					"SK":    &dtypes.AttributeValueMemberS{Value: "CONTROL#max-severity"},
					"value": &dtypes.AttributeValueMemberS{Value: "mega-critical"},
				},
			}, nil
		},
	}

	got, err := newSafety(api).MaxSeverity(context.Background())
	if err != nil {
		t.Fatalf("expected nil error for invalid severity (fail-safe), got %v", err)
	}
	if got != types.SeverityLow {
		t.Errorf("got %v, want SeverityLow (fail-safe) on invalid value", got)
	}
}

func TestDynamoDBSafety_MaxSeverity_Error(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("dynamo unavailable")
	api := &mockDynamoDBAPI{
		GetItemFn: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return nil, wantErr
		},
	}

	got, err := newSafety(api).MaxSeverity(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, wantErr) {
		t.Errorf("expected error wrapping %v, got %v", wantErr, err)
	}
	if got != types.SeverityLow {
		t.Errorf("got %v, want SeverityLow (fail-safe) on error", got)
	}
}

// ---------------------------------------------------------------------------
// CheckBlastRadius
// ---------------------------------------------------------------------------

func TestDynamoDBSafety_CheckBlastRadius_WithinLimits(t *testing.T) {
	t.Parallel()

	api := &mockDynamoDBAPI{
		GetItemFn: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{
				Item: map[string]dtypes.AttributeValue{
					"PK":               &dtypes.AttributeValueMemberS{Value: "CONTROL#blast-radius"},
					"SK":               &dtypes.AttributeValueMemberS{Value: "CONTROL#blast-radius"},
					"max_affected_pct": &dtypes.AttributeValueMemberN{Value: "50"},
					"max_pipelines":    &dtypes.AttributeValueMemberN{Value: "10"},
				},
			}, nil
		},
	}

	stats := types.ExperimentStats{
		AffectedPct:       25.0,
		AffectedPipelines: 5,
	}

	if err := newSafety(api).CheckBlastRadius(context.Background(), stats); err != nil {
		t.Fatalf("expected no error when within limits, got %v", err)
	}
}

func TestDynamoDBSafety_CheckBlastRadius_Exceeded(t *testing.T) {
	t.Parallel()

	api := &mockDynamoDBAPI{
		GetItemFn: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{
				Item: map[string]dtypes.AttributeValue{
					"PK":               &dtypes.AttributeValueMemberS{Value: "CONTROL#blast-radius"},
					"SK":               &dtypes.AttributeValueMemberS{Value: "CONTROL#blast-radius"},
					"max_affected_pct": &dtypes.AttributeValueMemberN{Value: "20"},
					"max_pipelines":    &dtypes.AttributeValueMemberN{Value: "3"},
				},
			}, nil
		},
	}

	stats := types.ExperimentStats{
		AffectedPct:       30.0,
		AffectedPipelines: 5,
	}

	err := newSafety(api).CheckBlastRadius(context.Background(), stats)
	if err == nil {
		t.Fatal("expected error when blast radius is exceeded")
	}
}

func TestDynamoDBSafety_CheckBlastRadius_NoConfig(t *testing.T) {
	t.Parallel()

	api := &mockDynamoDBAPI{
		GetItemFn: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{Item: nil}, nil
		},
	}

	stats := types.ExperimentStats{
		AffectedPct:       99.0,
		AffectedPipelines: 100,
	}

	if err := newSafety(api).CheckBlastRadius(context.Background(), stats); err != nil {
		t.Fatalf("expected nil when no config record exists, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// CheckSLAWindow
// ---------------------------------------------------------------------------

func TestDynamoDBSafety_CheckSLAWindow_Safe(t *testing.T) {
	t.Parallel()

	// Deadline is 2 hours from now — outside the 30-minute window.
	deadline := time.Now().Add(2 * time.Hour)

	api := &mockDynamoDBAPI{
		GetItemFn: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{
				Item: map[string]dtypes.AttributeValue{
					"PK":       &dtypes.AttributeValueMemberS{Value: "CONTROL#sla-window"},
					"SK":       &dtypes.AttributeValueMemberS{Value: "etl-daily"},
					"deadline": &dtypes.AttributeValueMemberS{Value: deadline.Format(time.RFC3339)},
				},
			}, nil
		},
	}

	safe, err := newSafety(api).CheckSLAWindow(context.Background(), "etl-daily")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !safe {
		t.Error("expected true (safe) when outside SLA window")
	}
}

func TestDynamoDBSafety_CheckSLAWindow_InWindow(t *testing.T) {
	t.Parallel()

	// Deadline is 15 minutes from now — within the 30-minute window.
	deadline := time.Now().Add(15 * time.Minute)

	api := &mockDynamoDBAPI{
		GetItemFn: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{
				Item: map[string]dtypes.AttributeValue{
					"PK":       &dtypes.AttributeValueMemberS{Value: "CONTROL#sla-window"},
					"SK":       &dtypes.AttributeValueMemberS{Value: "etl-daily"},
					"deadline": &dtypes.AttributeValueMemberS{Value: deadline.Format(time.RFC3339)},
				},
			}, nil
		},
	}

	safe, err := newSafety(api).CheckSLAWindow(context.Background(), "etl-daily")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if safe {
		t.Error("expected false (unsafe) when within 30 minutes of SLA deadline")
	}
}

func TestDynamoDBSafety_CheckSLAWindow_NoConfig(t *testing.T) {
	t.Parallel()

	api := &mockDynamoDBAPI{
		GetItemFn: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{Item: nil}, nil
		},
	}

	safe, err := newSafety(api).CheckSLAWindow(context.Background(), "etl-daily")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !safe {
		t.Error("expected true (safe) when no SLA config exists")
	}
}
