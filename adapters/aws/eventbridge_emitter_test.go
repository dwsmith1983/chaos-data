package aws_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	ebtypes "github.com/aws/aws-sdk-go-v2/service/eventbridge/types"

	chaosaws "github.com/dwsmith1983/chaos-data/adapters/aws"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

func newTestChaosEvent() types.ChaosEvent {
	return types.ChaosEvent{
		ID:           "evt-001",
		ExperimentID: "exp-001",
		Scenario:     "corrupt-parquet",
		Category:     "data-corruption",
		Severity:     types.SeverityModerate,
		Target:       "s3://staging/data.parquet",
		Mutation:     "bit-flip",
		Params:       map[string]string{"column": "revenue", "rate": "0.05"},
		Timestamp:    time.Date(2026, 3, 14, 12, 0, 0, 0, time.UTC),
		Mode:         "deterministic",
	}
}

func TestEventBridgeEmitter_Emit_Happy(t *testing.T) {
	t.Parallel()

	var captured *eventbridge.PutEventsInput
	mock := &mockEventBridgeAPI{
		PutEventsFn: func(ctx context.Context, params *eventbridge.PutEventsInput, optFns ...func(*eventbridge.Options)) (*eventbridge.PutEventsOutput, error) {
			captured = params
			return &eventbridge.PutEventsOutput{
				FailedEntryCount: 0,
				Entries:          []ebtypes.PutEventsResultEntry{{}},
			}, nil
		},
	}

	emitter := chaosaws.NewEventBridgeEmitter(mock, "my-bus")
	err := emitter.Emit(context.Background(), newTestChaosEvent())
	if err != nil {
		t.Fatalf("Emit() = %v, want nil", err)
	}

	if captured == nil {
		t.Fatal("PutEvents was not called")
	}
	if len(captured.Entries) != 1 {
		t.Fatalf("PutEvents entries = %d, want 1", len(captured.Entries))
	}

	entry := captured.Entries[0]
	if got := *entry.Source; got != "chaos-data" {
		t.Errorf("Source = %q, want %q", got, "chaos-data")
	}
	if got := *entry.DetailType; got != "chaos-data.fault-injected" {
		t.Errorf("DetailType = %q, want %q", got, "chaos-data.fault-injected")
	}
	if got := *entry.EventBusName; got != "my-bus" {
		t.Errorf("EventBusName = %q, want %q", got, "my-bus")
	}
	if entry.Detail == nil || *entry.Detail == "" {
		t.Error("Detail is empty, want JSON-encoded ChaosEvent")
	}
}

func TestEventBridgeEmitter_Emit_VerifyDetailContent(t *testing.T) {
	t.Parallel()

	var capturedDetail string
	mock := &mockEventBridgeAPI{
		PutEventsFn: func(ctx context.Context, params *eventbridge.PutEventsInput, optFns ...func(*eventbridge.Options)) (*eventbridge.PutEventsOutput, error) {
			capturedDetail = *params.Entries[0].Detail
			return &eventbridge.PutEventsOutput{
				FailedEntryCount: 0,
				Entries:          []ebtypes.PutEventsResultEntry{{}},
			}, nil
		},
	}

	emitter := chaosaws.NewEventBridgeEmitter(mock, "my-bus")
	evt := newTestChaosEvent()
	if err := emitter.Emit(context.Background(), evt); err != nil {
		t.Fatalf("Emit() = %v, want nil", err)
	}

	var decoded types.ChaosEvent
	if err := json.Unmarshal([]byte(capturedDetail), &decoded); err != nil {
		t.Fatalf("unmarshal Detail: %v", err)
	}

	if decoded.ID != evt.ID {
		t.Errorf("ID = %q, want %q", decoded.ID, evt.ID)
	}
	if decoded.ExperimentID != evt.ExperimentID {
		t.Errorf("ExperimentID = %q, want %q", decoded.ExperimentID, evt.ExperimentID)
	}
	if decoded.Scenario != evt.Scenario {
		t.Errorf("Scenario = %q, want %q", decoded.Scenario, evt.Scenario)
	}
	if decoded.Category != evt.Category {
		t.Errorf("Category = %q, want %q", decoded.Category, evt.Category)
	}
	if decoded.Severity != evt.Severity {
		t.Errorf("Severity = %v, want %v", decoded.Severity, evt.Severity)
	}
	if decoded.Target != evt.Target {
		t.Errorf("Target = %q, want %q", decoded.Target, evt.Target)
	}
	if decoded.Mutation != evt.Mutation {
		t.Errorf("Mutation = %q, want %q", decoded.Mutation, evt.Mutation)
	}
	if decoded.Mode != evt.Mode {
		t.Errorf("Mode = %q, want %q", decoded.Mode, evt.Mode)
	}
	if !decoded.Timestamp.Equal(evt.Timestamp) {
		t.Errorf("Timestamp = %v, want %v", decoded.Timestamp, evt.Timestamp)
	}
	if decoded.Params["column"] != "revenue" {
		t.Errorf("Params[column] = %q, want %q", decoded.Params["column"], "revenue")
	}
	if decoded.Params["rate"] != "0.05" {
		t.Errorf("Params[rate] = %q, want %q", decoded.Params["rate"], "0.05")
	}
}

func TestEventBridgeEmitter_Emit_Error(t *testing.T) {
	t.Parallel()

	apiErr := errors.New("eventbridge: throttled")
	mock := &mockEventBridgeAPI{
		PutEventsFn: func(ctx context.Context, params *eventbridge.PutEventsInput, optFns ...func(*eventbridge.Options)) (*eventbridge.PutEventsOutput, error) {
			return nil, apiErr
		},
	}

	emitter := chaosaws.NewEventBridgeEmitter(mock, "my-bus")
	err := emitter.Emit(context.Background(), newTestChaosEvent())
	if err == nil {
		t.Fatal("Emit() = nil, want error")
	}
	if !errors.Is(err, apiErr) {
		t.Errorf("Emit() error = %v, want wrapped %v", err, apiErr)
	}
}

func TestEventBridgeEmitter_Emit_PartialFailure(t *testing.T) {
	t.Parallel()

	errCode := "InternalError"
	errMsg := "something went wrong"
	mock := &mockEventBridgeAPI{
		PutEventsFn: func(ctx context.Context, params *eventbridge.PutEventsInput, optFns ...func(*eventbridge.Options)) (*eventbridge.PutEventsOutput, error) {
			return &eventbridge.PutEventsOutput{
				FailedEntryCount: 1,
				Entries: []ebtypes.PutEventsResultEntry{
					{
						ErrorCode:    &errCode,
						ErrorMessage: &errMsg,
					},
				},
			}, nil
		},
	}

	emitter := chaosaws.NewEventBridgeEmitter(mock, "my-bus")
	err := emitter.Emit(context.Background(), newTestChaosEvent())
	if err == nil {
		t.Fatal("Emit() = nil, want error for partial failure")
	}

	// Verify error contains useful details.
	errStr := err.Error()
	if !strings.Contains(errStr, "InternalError") {
		t.Errorf("error %q does not contain error code %q", errStr, "InternalError")
	}
	if !strings.Contains(errStr, "something went wrong") {
		t.Errorf("error %q does not contain error message %q", errStr, "something went wrong")
	}
}

func TestEventBridgeEmitter_Emit_DefaultBus(t *testing.T) {
	t.Parallel()

	var capturedBus string
	mock := &mockEventBridgeAPI{
		PutEventsFn: func(ctx context.Context, params *eventbridge.PutEventsInput, optFns ...func(*eventbridge.Options)) (*eventbridge.PutEventsOutput, error) {
			capturedBus = *params.Entries[0].EventBusName
			return &eventbridge.PutEventsOutput{
				FailedEntryCount: 0,
				Entries:          []ebtypes.PutEventsResultEntry{{}},
			}, nil
		},
	}

	emitter := chaosaws.NewEventBridgeEmitter(mock, "")
	if err := emitter.Emit(context.Background(), newTestChaosEvent()); err != nil {
		t.Fatalf("Emit() = %v, want nil", err)
	}

	if capturedBus != "default" {
		t.Errorf("EventBusName = %q, want %q", capturedBus, "default")
	}
}
