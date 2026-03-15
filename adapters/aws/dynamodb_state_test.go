package aws_test

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	chaosaws "github.com/dwsmith1983/chaos-data/adapters/aws"
	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

const testTable = "chaos-state"

func TestDynamoDBState_ReadSensor_Happy(t *testing.T) {
	t.Parallel()

	ts := time.Date(2026, 3, 14, 12, 0, 0, 0, time.UTC)

	mock := &mockDynamoDBAPI{
		GetItemFn: func(_ context.Context, params *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			if got := *params.TableName; got != testTable {
				t.Errorf("TableName = %q, want %q", got, testTable)
			}
			wantPK := chaosaws.SensorPK("etl-pipeline")
			wantSK := chaosaws.SensorSK("landing/orders")
			if got := params.Key["PK"].(*dynamodbtypes.AttributeValueMemberS).Value; got != wantPK {
				t.Errorf("PK = %q, want %q", got, wantPK)
			}
			if got := params.Key["SK"].(*dynamodbtypes.AttributeValueMemberS).Value; got != wantSK {
				t.Errorf("SK = %q, want %q", got, wantSK)
			}
			return &dynamodb.GetItemOutput{
				Item: map[string]dynamodbtypes.AttributeValue{
					"PK":           &dynamodbtypes.AttributeValueMemberS{Value: wantPK},
					"SK":           &dynamodbtypes.AttributeValueMemberS{Value: wantSK},
					"pipeline":     &dynamodbtypes.AttributeValueMemberS{Value: "etl-pipeline"},
					"key":          &dynamodbtypes.AttributeValueMemberS{Value: "landing/orders"},
					"status":       &dynamodbtypes.AttributeValueMemberS{Value: "ready"},
					"last_updated": &dynamodbtypes.AttributeValueMemberS{Value: ts.Format(time.RFC3339Nano)},
					"metadata": &dynamodbtypes.AttributeValueMemberM{Value: map[string]dynamodbtypes.AttributeValue{
						"source": &dynamodbtypes.AttributeValueMemberS{Value: "s3"},
					}},
				},
			}, nil
		},
	}

	store := chaosaws.NewDynamoDBState(mock, testTable)
	got, err := store.ReadSensor(context.Background(), "etl-pipeline", "landing/orders")
	if err != nil {
		t.Fatalf("ReadSensor() error = %v", err)
	}

	if got.Pipeline != "etl-pipeline" {
		t.Errorf("Pipeline = %q, want %q", got.Pipeline, "etl-pipeline")
	}
	if got.Key != "landing/orders" {
		t.Errorf("Key = %q, want %q", got.Key, "landing/orders")
	}
	if got.Status != types.SensorStatusReady {
		t.Errorf("Status = %q, want %q", got.Status, types.SensorStatusReady)
	}
	if !got.LastUpdated.Equal(ts) {
		t.Errorf("LastUpdated = %v, want %v", got.LastUpdated, ts)
	}
	if got.Metadata["source"] != "s3" {
		t.Errorf("Metadata[source] = %q, want %q", got.Metadata["source"], "s3")
	}
}

func TestDynamoDBState_ReadSensor_NotFound(t *testing.T) {
	t.Parallel()

	mock := &mockDynamoDBAPI{
		GetItemFn: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{
				Item: nil,
			}, nil
		},
	}

	store := chaosaws.NewDynamoDBState(mock, testTable)
	got, err := store.ReadSensor(context.Background(), "missing-pipeline", "missing-key")
	if err != nil {
		t.Fatalf("ReadSensor() error = %v, want nil for missing item", err)
	}

	zero := adapter.SensorData{}
	if !reflect.DeepEqual(got, zero) {
		t.Errorf("ReadSensor() = %+v, want zero SensorData", got)
	}
}

func TestDynamoDBState_WriteSensor_Happy(t *testing.T) {
	t.Parallel()

	ts := time.Date(2026, 3, 14, 12, 0, 0, 0, time.UTC)
	data := adapter.SensorData{
		Pipeline:    "etl-pipeline",
		Key:         "landing/orders",
		Status:      types.SensorStatusReady,
		LastUpdated: ts,
		Metadata:    map[string]string{"source": "s3"},
	}

	var captured *dynamodb.PutItemInput
	mock := &mockDynamoDBAPI{
		PutItemFn: func(_ context.Context, params *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
			captured = params
			return &dynamodb.PutItemOutput{}, nil
		},
	}

	store := chaosaws.NewDynamoDBState(mock, testTable)
	err := store.WriteSensor(context.Background(), "etl-pipeline", "landing/orders", data)
	if err != nil {
		t.Fatalf("WriteSensor() error = %v", err)
	}
	if captured == nil {
		t.Fatal("PutItem was not called")
	}
	if got := *captured.TableName; got != testTable {
		t.Errorf("TableName = %q, want %q", got, testTable)
	}

	wantPK := chaosaws.SensorPK("etl-pipeline")
	wantSK := chaosaws.SensorSK("landing/orders")
	if got := captured.Item["PK"].(*dynamodbtypes.AttributeValueMemberS).Value; got != wantPK {
		t.Errorf("PK = %q, want %q", got, wantPK)
	}
	if got := captured.Item["SK"].(*dynamodbtypes.AttributeValueMemberS).Value; got != wantSK {
		t.Errorf("SK = %q, want %q", got, wantSK)
	}
	if got := captured.Item["pipeline"].(*dynamodbtypes.AttributeValueMemberS).Value; got != "etl-pipeline" {
		t.Errorf("pipeline = %q, want %q", got, "etl-pipeline")
	}
	if got := captured.Item["status"].(*dynamodbtypes.AttributeValueMemberS).Value; got != "ready" {
		t.Errorf("status = %q, want %q", got, "ready")
	}
}

func TestDynamoDBState_ReadTriggerStatus_Happy(t *testing.T) {
	t.Parallel()

	mock := &mockDynamoDBAPI{
		GetItemFn: func(_ context.Context, params *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			wantPK := chaosaws.TriggerPK("etl-daily")
			wantSK := chaosaws.TriggerSK("daily", "2026-03-14")
			if got := params.Key["PK"].(*dynamodbtypes.AttributeValueMemberS).Value; got != wantPK {
				t.Errorf("PK = %q, want %q", got, wantPK)
			}
			if got := params.Key["SK"].(*dynamodbtypes.AttributeValueMemberS).Value; got != wantSK {
				t.Errorf("SK = %q, want %q", got, wantSK)
			}
			return &dynamodb.GetItemOutput{
				Item: map[string]dynamodbtypes.AttributeValue{
					"status": &dynamodbtypes.AttributeValueMemberS{Value: "fired"},
				},
			}, nil
		},
	}

	store := chaosaws.NewDynamoDBState(mock, testTable)
	key := adapter.TriggerKey{Pipeline: "etl-daily", Schedule: "daily", Date: "2026-03-14"}
	got, err := store.ReadTriggerStatus(context.Background(), key)
	if err != nil {
		t.Fatalf("ReadTriggerStatus() error = %v", err)
	}
	if got != "fired" {
		t.Errorf("ReadTriggerStatus() = %q, want %q", got, "fired")
	}
}

func TestDynamoDBState_ReadTriggerStatus_NotFound(t *testing.T) {
	t.Parallel()

	mock := &mockDynamoDBAPI{
		GetItemFn: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{Item: nil}, nil
		},
	}

	store := chaosaws.NewDynamoDBState(mock, testTable)
	key := adapter.TriggerKey{Pipeline: "missing", Schedule: "daily", Date: "2026-01-01"}
	got, err := store.ReadTriggerStatus(context.Background(), key)
	if err != nil {
		t.Fatalf("ReadTriggerStatus() error = %v, want nil for missing item", err)
	}
	if got != "" {
		t.Errorf("ReadTriggerStatus() = %q, want empty string for missing item", got)
	}
}

func TestDynamoDBState_WriteTriggerStatus_Happy(t *testing.T) {
	t.Parallel()

	var captured *dynamodb.PutItemInput
	mock := &mockDynamoDBAPI{
		PutItemFn: func(_ context.Context, params *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
			captured = params
			return &dynamodb.PutItemOutput{}, nil
		},
	}

	store := chaosaws.NewDynamoDBState(mock, testTable)
	key := adapter.TriggerKey{Pipeline: "etl-daily", Schedule: "daily", Date: "2026-03-14"}
	err := store.WriteTriggerStatus(context.Background(), key, "fired")
	if err != nil {
		t.Fatalf("WriteTriggerStatus() error = %v", err)
	}
	if captured == nil {
		t.Fatal("PutItem was not called")
	}

	wantPK := chaosaws.TriggerPK("etl-daily")
	wantSK := chaosaws.TriggerSK("daily", "2026-03-14")
	if got := captured.Item["PK"].(*dynamodbtypes.AttributeValueMemberS).Value; got != wantPK {
		t.Errorf("PK = %q, want %q", got, wantPK)
	}
	if got := captured.Item["SK"].(*dynamodbtypes.AttributeValueMemberS).Value; got != wantSK {
		t.Errorf("SK = %q, want %q", got, wantSK)
	}
	if got := captured.Item["status"].(*dynamodbtypes.AttributeValueMemberS).Value; got != "fired" {
		t.Errorf("status = %q, want %q", got, "fired")
	}
}

func TestDynamoDBState_WriteEvent_Happy(t *testing.T) {
	t.Parallel()

	ts := time.Date(2026, 3, 14, 15, 30, 0, 0, time.UTC)
	event := types.ChaosEvent{
		ID:           "evt-001",
		ExperimentID: "exp-001",
		Scenario:     "delay",
		Category:     "latency",
		Severity:     types.SeverityModerate,
		Target:       "s3://bucket/key",
		Mutation:     "add-delay",
		Params:       map[string]string{"ms": "500"},
		Timestamp:    ts,
		Mode:         "deterministic",
	}

	var captured *dynamodb.PutItemInput
	mock := &mockDynamoDBAPI{
		PutItemFn: func(_ context.Context, params *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
			captured = params
			return &dynamodb.PutItemOutput{}, nil
		},
	}

	store := chaosaws.NewDynamoDBState(mock, testTable)
	err := store.WriteEvent(context.Background(), event)
	if err != nil {
		t.Fatalf("WriteEvent() error = %v", err)
	}
	if captured == nil {
		t.Fatal("PutItem was not called")
	}

	wantPK := chaosaws.ChaosPK("exp-001")
	wantSK := chaosaws.ChaosSK(ts, "evt-001")
	if got := captured.Item["PK"].(*dynamodbtypes.AttributeValueMemberS).Value; got != wantPK {
		t.Errorf("PK = %q, want %q", got, wantPK)
	}
	if got := captured.Item["SK"].(*dynamodbtypes.AttributeValueMemberS).Value; got != wantSK {
		t.Errorf("SK = %q, want %q", got, wantSK)
	}
	if got := captured.Item["id"].(*dynamodbtypes.AttributeValueMemberS).Value; got != "evt-001" {
		t.Errorf("id = %q, want %q", got, "evt-001")
	}
	if got := captured.Item["experiment_id"].(*dynamodbtypes.AttributeValueMemberS).Value; got != "exp-001" {
		t.Errorf("experiment_id = %q, want %q", got, "exp-001")
	}
	if got := captured.Item["scenario"].(*dynamodbtypes.AttributeValueMemberS).Value; got != "delay" {
		t.Errorf("scenario = %q, want %q", got, "delay")
	}
	if got := captured.Item["category"].(*dynamodbtypes.AttributeValueMemberS).Value; got != "latency" {
		t.Errorf("category = %q, want %q", got, "latency")
	}
	if got := captured.Item["severity"].(*dynamodbtypes.AttributeValueMemberN).Value; got != strconv.Itoa(int(types.SeverityModerate)) {
		t.Errorf("severity = %q, want %q", got, strconv.Itoa(int(types.SeverityModerate)))
	}
	if got := captured.Item["target"].(*dynamodbtypes.AttributeValueMemberS).Value; got != "s3://bucket/key" {
		t.Errorf("target = %q, want %q", got, "s3://bucket/key")
	}
	if got := captured.Item["mutation"].(*dynamodbtypes.AttributeValueMemberS).Value; got != "add-delay" {
		t.Errorf("mutation = %q, want %q", got, "add-delay")
	}
	if got := captured.Item["mode"].(*dynamodbtypes.AttributeValueMemberS).Value; got != "deterministic" {
		t.Errorf("mode = %q, want %q", got, "deterministic")
	}
	if got := captured.Item["timestamp"].(*dynamodbtypes.AttributeValueMemberS).Value; got != ts.Format(time.RFC3339Nano) {
		t.Errorf("timestamp = %q, want %q", got, ts.Format(time.RFC3339Nano))
	}

	// Verify params map
	paramsAttr := captured.Item["params"].(*dynamodbtypes.AttributeValueMemberM)
	if got := paramsAttr.Value["ms"].(*dynamodbtypes.AttributeValueMemberS).Value; got != "500" {
		t.Errorf("params[ms] = %q, want %q", got, "500")
	}
}

// ---------------------------------------------------------------------------
// DeleteSensor
// ---------------------------------------------------------------------------

func TestDynamoDBState_DeleteSensor_Happy(t *testing.T) {
	t.Parallel()

	var captured *dynamodb.DeleteItemInput
	mock := &mockDynamoDBAPI{
		DeleteItemFn: func(_ context.Context, params *dynamodb.DeleteItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
			captured = params
			return &dynamodb.DeleteItemOutput{}, nil
		},
	}

	store := chaosaws.NewDynamoDBState(mock, testTable)
	err := store.DeleteSensor(context.Background(), "etl-pipeline", "landing/orders")
	if err != nil {
		t.Fatalf("DeleteSensor() error = %v", err)
	}
	if captured == nil {
		t.Fatal("DeleteItem was not called")
	}
	if got := *captured.TableName; got != testTable {
		t.Errorf("TableName = %q, want %q", got, testTable)
	}

	wantPK := chaosaws.SensorPK("etl-pipeline")
	wantSK := chaosaws.SensorSK("landing/orders")
	if got := captured.Key["PK"].(*dynamodbtypes.AttributeValueMemberS).Value; got != wantPK {
		t.Errorf("PK = %q, want %q", got, wantPK)
	}
	if got := captured.Key["SK"].(*dynamodbtypes.AttributeValueMemberS).Value; got != wantSK {
		t.Errorf("SK = %q, want %q", got, wantSK)
	}
}

func TestDynamoDBState_DeleteSensor_Error(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("dynamo delete failed")
	mock := &mockDynamoDBAPI{
		DeleteItemFn: func(_ context.Context, _ *dynamodb.DeleteItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
			return nil, wantErr
		},
	}

	store := chaosaws.NewDynamoDBState(mock, testTable)
	err := store.DeleteSensor(context.Background(), "etl-pipeline", "landing/orders")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, wantErr) {
		t.Errorf("expected error wrapping %v, got %v", wantErr, err)
	}
}

// ---------------------------------------------------------------------------
// ReadChaosEvents
// ---------------------------------------------------------------------------

func TestDynamoDBState_ReadChaosEvents_Happy(t *testing.T) {
	t.Parallel()

	ts1 := time.Date(2026, 3, 14, 15, 30, 0, 0, time.UTC)
	ts2 := time.Date(2026, 3, 14, 15, 31, 0, 0, time.UTC)

	mock := &mockDynamoDBAPI{
		QueryFn: func(_ context.Context, params *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			if got := *params.TableName; got != testTable {
				t.Errorf("TableName = %q, want %q", got, testTable)
			}
			wantPK := chaosaws.ChaosPK("exp-001")
			gotPK := params.ExpressionAttributeValues[":pk"].(*dynamodbtypes.AttributeValueMemberS).Value
			if gotPK != wantPK {
				t.Errorf("PK = %q, want %q", gotPK, wantPK)
			}
			return &dynamodb.QueryOutput{
				Items: []map[string]dynamodbtypes.AttributeValue{
					{
						"id":            &dynamodbtypes.AttributeValueMemberS{Value: "evt-001"},
						"experiment_id": &dynamodbtypes.AttributeValueMemberS{Value: "exp-001"},
						"scenario":      &dynamodbtypes.AttributeValueMemberS{Value: "delay"},
						"category":      &dynamodbtypes.AttributeValueMemberS{Value: "latency"},
						"severity":      &dynamodbtypes.AttributeValueMemberN{Value: strconv.Itoa(int(types.SeverityModerate))},
						"target":        &dynamodbtypes.AttributeValueMemberS{Value: "s3://bucket/key1"},
						"mutation":      &dynamodbtypes.AttributeValueMemberS{Value: "add-delay"},
						"timestamp":     &dynamodbtypes.AttributeValueMemberS{Value: ts1.Format(time.RFC3339Nano)},
						"mode":          &dynamodbtypes.AttributeValueMemberS{Value: "deterministic"},
						"params": &dynamodbtypes.AttributeValueMemberM{Value: map[string]dynamodbtypes.AttributeValue{
							"ms": &dynamodbtypes.AttributeValueMemberS{Value: "500"},
						}},
					},
					{
						"id":            &dynamodbtypes.AttributeValueMemberS{Value: "evt-002"},
						"experiment_id": &dynamodbtypes.AttributeValueMemberS{Value: "exp-001"},
						"scenario":      &dynamodbtypes.AttributeValueMemberS{Value: "corrupt"},
						"category":      &dynamodbtypes.AttributeValueMemberS{Value: "integrity"},
						"severity":      &dynamodbtypes.AttributeValueMemberN{Value: strconv.Itoa(int(types.SeveritySevere))},
						"target":        &dynamodbtypes.AttributeValueMemberS{Value: "s3://bucket/key2"},
						"mutation":      &dynamodbtypes.AttributeValueMemberS{Value: "flip-bytes"},
						"timestamp":     &dynamodbtypes.AttributeValueMemberS{Value: ts2.Format(time.RFC3339Nano)},
						"mode":          &dynamodbtypes.AttributeValueMemberS{Value: "probabilistic"},
					},
				},
			}, nil
		},
	}

	store := chaosaws.NewDynamoDBState(mock, testTable)
	events, err := store.ReadChaosEvents(context.Background(), "exp-001")
	if err != nil {
		t.Fatalf("ReadChaosEvents() error = %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("ReadChaosEvents() returned %d events, want 2", len(events))
	}

	// Verify first event
	if events[0].ID != "evt-001" {
		t.Errorf("events[0].ID = %q, want %q", events[0].ID, "evt-001")
	}
	if events[0].ExperimentID != "exp-001" {
		t.Errorf("events[0].ExperimentID = %q, want %q", events[0].ExperimentID, "exp-001")
	}
	if events[0].Scenario != "delay" {
		t.Errorf("events[0].Scenario = %q, want %q", events[0].Scenario, "delay")
	}
	if events[0].Category != "latency" {
		t.Errorf("events[0].Category = %q, want %q", events[0].Category, "latency")
	}
	if events[0].Severity != types.SeverityModerate {
		t.Errorf("events[0].Severity = %d, want %d", events[0].Severity, types.SeverityModerate)
	}
	if events[0].Target != "s3://bucket/key1" {
		t.Errorf("events[0].Target = %q, want %q", events[0].Target, "s3://bucket/key1")
	}
	if events[0].Mutation != "add-delay" {
		t.Errorf("events[0].Mutation = %q, want %q", events[0].Mutation, "add-delay")
	}
	if !events[0].Timestamp.Equal(ts1) {
		t.Errorf("events[0].Timestamp = %v, want %v", events[0].Timestamp, ts1)
	}
	if events[0].Mode != "deterministic" {
		t.Errorf("events[0].Mode = %q, want %q", events[0].Mode, "deterministic")
	}
	if events[0].Params["ms"] != "500" {
		t.Errorf("events[0].Params[ms] = %q, want %q", events[0].Params["ms"], "500")
	}

	// Verify second event
	if events[1].ID != "evt-002" {
		t.Errorf("events[1].ID = %q, want %q", events[1].ID, "evt-002")
	}
	if events[1].Severity != types.SeveritySevere {
		t.Errorf("events[1].Severity = %d, want %d", events[1].Severity, types.SeveritySevere)
	}
	if events[1].Params != nil {
		t.Errorf("events[1].Params = %v, want nil (no params attribute)", events[1].Params)
	}
}

func TestDynamoDBState_ReadChaosEvents_Empty(t *testing.T) {
	t.Parallel()

	mock := &mockDynamoDBAPI{
		QueryFn: func(_ context.Context, _ *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			return &dynamodb.QueryOutput{
				Items: []map[string]dynamodbtypes.AttributeValue{},
			}, nil
		},
	}

	store := chaosaws.NewDynamoDBState(mock, testTable)
	events, err := store.ReadChaosEvents(context.Background(), "exp-missing")
	if err != nil {
		t.Fatalf("ReadChaosEvents() error = %v", err)
	}
	if len(events) != 0 {
		t.Errorf("ReadChaosEvents() returned %d events, want 0", len(events))
	}
}

func TestDynamoDBState_ReadChaosEvents_Error(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("dynamo query failed")
	mock := &mockDynamoDBAPI{
		QueryFn: func(_ context.Context, _ *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			return nil, wantErr
		},
	}

	store := chaosaws.NewDynamoDBState(mock, testTable)
	events, err := store.ReadChaosEvents(context.Background(), "exp-001")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, wantErr) {
		t.Errorf("expected error wrapping %v, got %v", wantErr, err)
	}
	if events != nil {
		t.Errorf("expected nil events on error, got %v", events)
	}
}
