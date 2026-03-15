package aws_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	chaosaws "github.com/dwsmith1983/chaos-data/adapters/aws"
	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

const testSafetyTable = "chaos-control"

func newSafety(api *mockDynamoDBAPI) *chaosaws.DynamoDBSafety {
	return chaosaws.NewDynamoDBSafety(api, testSafetyTable, 5*time.Minute)
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

func TestDynamoDBSafety_CheckBlastRadius_MaxHeldBytesExceeded(t *testing.T) {
	t.Parallel()

	api := &mockDynamoDBAPI{
		GetItemFn: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{
				Item: map[string]dtypes.AttributeValue{
					"PK":              &dtypes.AttributeValueMemberS{Value: "CONTROL#blast-radius"},
					"SK":              &dtypes.AttributeValueMemberS{Value: "CONTROL#blast-radius"},
					"max_held_bytes": &dtypes.AttributeValueMemberN{Value: "1000"},
				},
			}, nil
		},
	}

	stats := types.ExperimentStats{
		HeldBytes: 1001,
	}

	err := newSafety(api).CheckBlastRadius(context.Background(), stats)
	if err == nil {
		t.Fatal("expected error when HeldBytes exceeds max_held_bytes")
	}
}

func TestDynamoDBSafety_CheckBlastRadius_MaxMutationsExceeded(t *testing.T) {
	t.Parallel()

	api := &mockDynamoDBAPI{
		GetItemFn: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{
				Item: map[string]dtypes.AttributeValue{
					"PK":            &dtypes.AttributeValueMemberS{Value: "CONTROL#blast-radius"},
					"SK":            &dtypes.AttributeValueMemberS{Value: "CONTROL#blast-radius"},
					"max_mutations": &dtypes.AttributeValueMemberN{Value: "5"},
				},
			}, nil
		},
	}

	stats := types.ExperimentStats{
		MutationsApplied: 6,
	}

	err := newSafety(api).CheckBlastRadius(context.Background(), stats)
	if err == nil {
		t.Fatal("expected error when MutationsApplied exceeds max_mutations")
	}
}

func TestDynamoDBSafety_CheckBlastRadius_HeldBytesWithinLimit(t *testing.T) {
	t.Parallel()

	api := &mockDynamoDBAPI{
		GetItemFn: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{
				Item: map[string]dtypes.AttributeValue{
					"PK":              &dtypes.AttributeValueMemberS{Value: "CONTROL#blast-radius"},
					"SK":              &dtypes.AttributeValueMemberS{Value: "CONTROL#blast-radius"},
					"max_held_bytes": &dtypes.AttributeValueMemberN{Value: "5000"},
					"max_mutations":  &dtypes.AttributeValueMemberN{Value: "10"},
				},
			}, nil
		},
	}

	stats := types.ExperimentStats{
		HeldBytes:        4000,
		MutationsApplied: 9,
	}

	if err := newSafety(api).CheckBlastRadius(context.Background(), stats); err != nil {
		t.Fatalf("expected no error when within limits, got %v", err)
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

// ---------------------------------------------------------------------------
// CheckCooldown
// ---------------------------------------------------------------------------

func TestDynamoDBSafety_CheckCooldown_NoPrior(t *testing.T) {
	t.Parallel()

	api := &mockDynamoDBAPI{
		GetItemFn: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{Item: nil}, nil
		},
	}

	err := newSafety(api).CheckCooldown(context.Background(), "sc-delay")
	if err != nil {
		t.Fatalf("expected nil when no prior injection, got %v", err)
	}
}

func TestDynamoDBSafety_CheckCooldown_Active(t *testing.T) {
	t.Parallel()

	// Injection recorded 1 minute ago — within the 5-minute cooldown.
	recentTS := time.Now().Add(-1 * time.Minute).Format(time.RFC3339Nano)

	api := &mockDynamoDBAPI{
		GetItemFn: func(_ context.Context, in *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{
				Item: map[string]dtypes.AttributeValue{
					"PK":            &dtypes.AttributeValueMemberS{Value: chaosaws.CooldownPK()},
					"SK":            &dtypes.AttributeValueMemberS{Value: chaosaws.CooldownSK("sc-delay")},
					"last_injected": &dtypes.AttributeValueMemberS{Value: recentTS},
				},
			}, nil
		},
	}

	err := newSafety(api).CheckCooldown(context.Background(), "sc-delay")
	if !errors.Is(err, adapter.ErrCooldownActive) {
		t.Fatalf("expected ErrCooldownActive, got %v", err)
	}
}

func TestDynamoDBSafety_CheckCooldown_Expired(t *testing.T) {
	t.Parallel()

	// Injection recorded 10 minutes ago — outside the 5-minute cooldown.
	oldTS := time.Now().Add(-10 * time.Minute).Format(time.RFC3339Nano)

	api := &mockDynamoDBAPI{
		GetItemFn: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{
				Item: map[string]dtypes.AttributeValue{
					"PK":            &dtypes.AttributeValueMemberS{Value: chaosaws.CooldownPK()},
					"SK":            &dtypes.AttributeValueMemberS{Value: chaosaws.CooldownSK("sc-delay")},
					"last_injected": &dtypes.AttributeValueMemberS{Value: oldTS},
				},
			}, nil
		},
	}

	err := newSafety(api).CheckCooldown(context.Background(), "sc-delay")
	if err != nil {
		t.Fatalf("expected nil when cooldown expired, got %v", err)
	}
}

func TestDynamoDBSafety_CheckCooldown_GetItemError(t *testing.T) {
	t.Parallel()

	api := &mockDynamoDBAPI{
		GetItemFn: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return nil, errors.New("dynamo timeout")
		},
	}

	// Fail-safe: errors in CheckCooldown allow injection (return nil).
	err := newSafety(api).CheckCooldown(context.Background(), "sc-delay")
	if err != nil {
		t.Fatalf("expected nil (fail-safe) on GetItem error, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// RecordInjection
// ---------------------------------------------------------------------------

func TestDynamoDBSafety_RecordInjection(t *testing.T) {
	t.Parallel()

	var capturedInput *dynamodb.PutItemInput
	api := &mockDynamoDBAPI{
		PutItemFn: func(_ context.Context, in *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
			capturedInput = in
			return &dynamodb.PutItemOutput{}, nil
		},
	}

	err := newSafety(api).RecordInjection(context.Background(), "sc-delay")
	if err != nil {
		t.Fatalf("RecordInjection() error = %v", err)
	}

	if capturedInput == nil {
		t.Fatal("PutItem was not called")
	}

	// Verify PK.
	pk, ok := capturedInput.Item["PK"].(*dtypes.AttributeValueMemberS)
	if !ok {
		t.Fatal("PK is not a string attribute")
	}
	if pk.Value != chaosaws.CooldownPK() {
		t.Errorf("PK = %q, want %q", pk.Value, chaosaws.CooldownPK())
	}

	// Verify SK.
	sk, ok := capturedInput.Item["SK"].(*dtypes.AttributeValueMemberS)
	if !ok {
		t.Fatal("SK is not a string attribute")
	}
	if sk.Value != chaosaws.CooldownSK("sc-delay") {
		t.Errorf("SK = %q, want %q", sk.Value, chaosaws.CooldownSK("sc-delay"))
	}

	// Verify last_injected is present and parseable.
	ts, ok := capturedInput.Item["last_injected"].(*dtypes.AttributeValueMemberS)
	if !ok {
		t.Fatal("last_injected is not a string attribute")
	}
	if _, err := time.Parse(time.RFC3339Nano, ts.Value); err != nil {
		t.Errorf("last_injected is not valid RFC3339Nano: %v", err)
	}
}
