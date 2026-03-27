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

// ---------------------------------------------------------------------------
// WritePipelineConfig / ReadPipelineConfig
// ---------------------------------------------------------------------------

func TestDynamoDBState_WritePipelineConfig_ReadPipelineConfig(t *testing.T) {
	t.Parallel()

	configData := []byte(`{"schedule":"daily","targets":["s3"]}`)

	// Track PutItem calls
	var capturedPut *dynamodb.PutItemInput
	mock := &mockDynamoDBAPI{
		PutItemFn: func(_ context.Context, params *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
			capturedPut = params
			return &dynamodb.PutItemOutput{}, nil
		},
		GetItemFn: func(_ context.Context, params *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			wantPK := chaosaws.ConfigPK("etl-pipeline")
			wantSK := chaosaws.ConfigSK()
			if got := params.Key["PK"].(*dynamodbtypes.AttributeValueMemberS).Value; got != wantPK {
				t.Errorf("GetItem PK = %q, want %q", got, wantPK)
			}
			if got := params.Key["SK"].(*dynamodbtypes.AttributeValueMemberS).Value; got != wantSK {
				t.Errorf("GetItem SK = %q, want %q", got, wantSK)
			}
			return &dynamodb.GetItemOutput{
				Item: map[string]dynamodbtypes.AttributeValue{
					"PK":     &dynamodbtypes.AttributeValueMemberS{Value: wantPK},
					"SK":     &dynamodbtypes.AttributeValueMemberS{Value: wantSK},
					"config": &dynamodbtypes.AttributeValueMemberS{Value: string(configData)},
				},
			}, nil
		},
	}

	store := chaosaws.NewDynamoDBState(mock, testTable)

	// Write
	err := store.WritePipelineConfig(context.Background(), "etl-pipeline", configData)
	if err != nil {
		t.Fatalf("WritePipelineConfig() error = %v", err)
	}
	if capturedPut == nil {
		t.Fatal("PutItem was not called")
	}
	if got := *capturedPut.TableName; got != testTable {
		t.Errorf("TableName = %q, want %q", got, testTable)
	}

	wantPK := chaosaws.ConfigPK("etl-pipeline")
	wantSK := chaosaws.ConfigSK()
	if got := capturedPut.Item["PK"].(*dynamodbtypes.AttributeValueMemberS).Value; got != wantPK {
		t.Errorf("PK = %q, want %q", got, wantPK)
	}
	if got := capturedPut.Item["SK"].(*dynamodbtypes.AttributeValueMemberS).Value; got != wantSK {
		t.Errorf("SK = %q, want %q", got, wantSK)
	}
	if got := capturedPut.Item["config"].(*dynamodbtypes.AttributeValueMemberS).Value; got != string(configData) {
		t.Errorf("config = %q, want %q", got, string(configData))
	}

	// Read
	got, err := store.ReadPipelineConfig(context.Background(), "etl-pipeline")
	if err != nil {
		t.Fatalf("ReadPipelineConfig() error = %v", err)
	}
	if string(got) != string(configData) {
		t.Errorf("ReadPipelineConfig() = %q, want %q", string(got), string(configData))
	}
}

func TestDynamoDBState_ReadPipelineConfig_NotFound(t *testing.T) {
	t.Parallel()

	mock := &mockDynamoDBAPI{
		GetItemFn: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{Item: nil}, nil
		},
	}

	store := chaosaws.NewDynamoDBState(mock, testTable)
	got, err := store.ReadPipelineConfig(context.Background(), "missing-pipeline")
	if err != nil {
		t.Fatalf("ReadPipelineConfig() error = %v, want nil for missing item", err)
	}
	if got != nil {
		t.Errorf("ReadPipelineConfig() = %v, want nil for missing item", got)
	}
}

// ---------------------------------------------------------------------------
// DeleteByPrefix
// ---------------------------------------------------------------------------

func TestDynamoDBState_DeleteByPrefix(t *testing.T) {
	t.Parallel()

	// Scan returns 3 items, 2 matching prefix "SENSOR#etl", 1 not matching
	// (but scan is server-side filtered, so mock returns only matches)
	matchPK1 := "SENSOR#etl-pipe-1"
	matchSK1 := "KEY#landing/orders"
	matchPK2 := "SENSOR#etl-pipe-2"
	matchSK2 := "KEY#landing/returns"

	var capturedScan *dynamodb.ScanInput
	var capturedBatch *dynamodb.BatchWriteItemInput

	mock := &mockDynamoDBAPI{
		ScanFn: func(_ context.Context, params *dynamodb.ScanInput, _ ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error) {
			capturedScan = params
			return &dynamodb.ScanOutput{
				Items: []map[string]dynamodbtypes.AttributeValue{
					{
						"PK": &dynamodbtypes.AttributeValueMemberS{Value: matchPK1},
						"SK": &dynamodbtypes.AttributeValueMemberS{Value: matchSK1},
					},
					{
						"PK": &dynamodbtypes.AttributeValueMemberS{Value: matchPK2},
						"SK": &dynamodbtypes.AttributeValueMemberS{Value: matchSK2},
					},
				},
			}, nil
		},
		BatchWriteItemFn: func(_ context.Context, params *dynamodb.BatchWriteItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error) {
			capturedBatch = params
			return &dynamodb.BatchWriteItemOutput{}, nil
		},
	}

	store := chaosaws.NewDynamoDBState(mock, testTable)
	err := store.DeleteByPrefix(context.Background(), "SENSOR#etl")
	if err != nil {
		t.Fatalf("DeleteByPrefix() error = %v", err)
	}

	// Verify scan was called with filter expression
	if capturedScan == nil {
		t.Fatal("Scan was not called")
	}
	if got := *capturedScan.TableName; got != testTable {
		t.Errorf("Scan TableName = %q, want %q", got, testTable)
	}

	// Verify batch write was called with 2 delete requests
	if capturedBatch == nil {
		t.Fatal("BatchWriteItem was not called")
	}
	requests := capturedBatch.RequestItems[testTable]
	if len(requests) != 2 {
		t.Fatalf("BatchWriteItem requests = %d, want 2", len(requests))
	}

	// Verify the keys in delete requests
	del1 := requests[0].DeleteRequest
	if del1 == nil {
		t.Fatal("expected DeleteRequest, got nil")
	}
	gotPK1 := del1.Key["PK"].(*dynamodbtypes.AttributeValueMemberS).Value
	gotSK1 := del1.Key["SK"].(*dynamodbtypes.AttributeValueMemberS).Value
	if gotPK1 != matchPK1 || gotSK1 != matchSK1 {
		t.Errorf("delete[0] PK=%q SK=%q, want PK=%q SK=%q", gotPK1, gotSK1, matchPK1, matchSK1)
	}

	del2 := requests[1].DeleteRequest
	if del2 == nil {
		t.Fatal("expected DeleteRequest, got nil")
	}
	gotPK2 := del2.Key["PK"].(*dynamodbtypes.AttributeValueMemberS).Value
	gotSK2 := del2.Key["SK"].(*dynamodbtypes.AttributeValueMemberS).Value
	if gotPK2 != matchPK2 || gotSK2 != matchSK2 {
		t.Errorf("delete[1] PK=%q SK=%q, want PK=%q SK=%q", gotPK2, gotSK2, matchPK2, matchSK2)
	}
}

func TestDynamoDBState_DeleteByPrefix_NoMatches(t *testing.T) {
	t.Parallel()

	batchCalled := false
	mock := &mockDynamoDBAPI{
		ScanFn: func(_ context.Context, _ *dynamodb.ScanInput, _ ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error) {
			return &dynamodb.ScanOutput{Items: []map[string]dynamodbtypes.AttributeValue{}}, nil
		},
		BatchWriteItemFn: func(_ context.Context, _ *dynamodb.BatchWriteItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error) {
			batchCalled = true
			return &dynamodb.BatchWriteItemOutput{}, nil
		},
	}

	store := chaosaws.NewDynamoDBState(mock, testTable)
	err := store.DeleteByPrefix(context.Background(), "SENSOR#nonexistent")
	if err != nil {
		t.Fatalf("DeleteByPrefix() error = %v", err)
	}
	if batchCalled {
		t.Error("BatchWriteItem should not be called when scan returns no items")
	}
}

// ---------------------------------------------------------------------------
// WriteRerun / CountReruns
// ---------------------------------------------------------------------------

func TestDynamoDBState_WriteRerun_CountReruns(t *testing.T) {
	t.Parallel()

	var capturedPuts []*dynamodb.PutItemInput
	mock := &mockDynamoDBAPI{
		PutItemFn: func(_ context.Context, params *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
			capturedPuts = append(capturedPuts, params)
			return &dynamodb.PutItemOutput{}, nil
		},
		QueryFn: func(_ context.Context, params *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			// Verify the query parameters
			wantPK := chaosaws.RerunPK("etl-pipeline")
			gotPK := params.ExpressionAttributeValues[":pk"].(*dynamodbtypes.AttributeValueMemberS).Value
			if gotPK != wantPK {
				t.Errorf("Query PK = %q, want %q", gotPK, wantPK)
			}
			// Return 2 items to simulate 2 reruns written
			return &dynamodb.QueryOutput{
				Count: 2,
				Items: []map[string]dynamodbtypes.AttributeValue{
					{"PK": &dynamodbtypes.AttributeValueMemberS{Value: wantPK}},
					{"PK": &dynamodbtypes.AttributeValueMemberS{Value: wantPK}},
				},
			}, nil
		},
	}

	store := chaosaws.NewDynamoDBState(mock, testTable)

	// Write 2 reruns
	err := store.WriteRerun(context.Background(), "etl-pipeline", "daily", "2026-03-14", "data-quality-issue")
	if err != nil {
		t.Fatalf("WriteRerun() error = %v", err)
	}
	err = store.WriteRerun(context.Background(), "etl-pipeline", "daily", "2026-03-14", "upstream-delay")
	if err != nil {
		t.Fatalf("WriteRerun() error = %v", err)
	}

	if len(capturedPuts) != 2 {
		t.Fatalf("expected 2 PutItem calls, got %d", len(capturedPuts))
	}

	// Verify PK/SK on first write
	wantPK := chaosaws.RerunPK("etl-pipeline")
	if got := capturedPuts[0].Item["PK"].(*dynamodbtypes.AttributeValueMemberS).Value; got != wantPK {
		t.Errorf("WriteRerun PK = %q, want %q", got, wantPK)
	}
	if got := capturedPuts[0].Item["reason"].(*dynamodbtypes.AttributeValueMemberS).Value; got != "data-quality-issue" {
		t.Errorf("WriteRerun reason = %q, want %q", got, "data-quality-issue")
	}

	// Count reruns
	count, err := store.CountReruns(context.Background(), "etl-pipeline", "daily", "2026-03-14")
	if err != nil {
		t.Fatalf("CountReruns() error = %v", err)
	}
	if count != 2 {
		t.Errorf("CountReruns() = %d, want 2", count)
	}
}

func TestDynamoDBState_CountReruns_Zero(t *testing.T) {
	t.Parallel()

	mock := &mockDynamoDBAPI{
		QueryFn: func(_ context.Context, _ *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			return &dynamodb.QueryOutput{
				Count: 0,
				Items: []map[string]dynamodbtypes.AttributeValue{},
			}, nil
		},
	}

	store := chaosaws.NewDynamoDBState(mock, testTable)
	count, err := store.CountReruns(context.Background(), "etl-pipeline", "daily", "2026-01-01")
	if err != nil {
		t.Fatalf("CountReruns() error = %v", err)
	}
	if count != 0 {
		t.Errorf("CountReruns() = %d, want 0", count)
	}
}

// ---------------------------------------------------------------------------
// ReadJobEvents
// ---------------------------------------------------------------------------

func TestDynamoDBState_ReadJobEvents(t *testing.T) {
	t.Parallel()

	ts1 := time.Date(2026, 3, 14, 15, 30, 0, 0, time.UTC)
	ts2 := time.Date(2026, 3, 14, 15, 35, 0, 0, time.UTC)

	mock := &mockDynamoDBAPI{
		QueryFn: func(_ context.Context, params *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			// Verify query uses correct PK and SK prefix
			wantPK := chaosaws.JobEventPK("etl-pipeline")
			gotPK := params.ExpressionAttributeValues[":pk"].(*dynamodbtypes.AttributeValueMemberS).Value
			if gotPK != wantPK {
				t.Errorf("Query PK = %q, want %q", gotPK, wantPK)
			}

			// Verify ScanIndexForward is false (DESC order)
			if params.ScanIndexForward == nil || *params.ScanIndexForward != false {
				t.Error("expected ScanIndexForward=false for DESC order")
			}

			// Return events in DESC order (newest first)
			return &dynamodb.QueryOutput{
				Items: []map[string]dynamodbtypes.AttributeValue{
					{
						"pipeline":  &dynamodbtypes.AttributeValueMemberS{Value: "etl-pipeline"},
						"schedule":  &dynamodbtypes.AttributeValueMemberS{Value: "daily"},
						"date":      &dynamodbtypes.AttributeValueMemberS{Value: "2026-03-14"},
						"event":     &dynamodbtypes.AttributeValueMemberS{Value: "completed"},
						"run_id":    &dynamodbtypes.AttributeValueMemberS{Value: "run-002"},
						"timestamp": &dynamodbtypes.AttributeValueMemberS{Value: ts2.Format(time.RFC3339Nano)},
					},
					{
						"pipeline":  &dynamodbtypes.AttributeValueMemberS{Value: "etl-pipeline"},
						"schedule":  &dynamodbtypes.AttributeValueMemberS{Value: "daily"},
						"date":      &dynamodbtypes.AttributeValueMemberS{Value: "2026-03-14"},
						"event":     &dynamodbtypes.AttributeValueMemberS{Value: "started"},
						"run_id":    &dynamodbtypes.AttributeValueMemberS{Value: "run-001"},
						"timestamp": &dynamodbtypes.AttributeValueMemberS{Value: ts1.Format(time.RFC3339Nano)},
					},
				},
			}, nil
		},
	}

	store := chaosaws.NewDynamoDBState(mock, testTable)
	events, err := store.ReadJobEvents(context.Background(), "etl-pipeline", "daily", "2026-03-14")
	if err != nil {
		t.Fatalf("ReadJobEvents() error = %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("ReadJobEvents() returned %d events, want 2", len(events))
	}

	// Verify DESC order (newest first)
	if events[0].Event != "completed" {
		t.Errorf("events[0].Event = %q, want %q", events[0].Event, "completed")
	}
	if events[0].RunID != "run-002" {
		t.Errorf("events[0].RunID = %q, want %q", events[0].RunID, "run-002")
	}
	if !events[0].Timestamp.Equal(ts2) {
		t.Errorf("events[0].Timestamp = %v, want %v", events[0].Timestamp, ts2)
	}

	if events[1].Event != "started" {
		t.Errorf("events[1].Event = %q, want %q", events[1].Event, "started")
	}
	if events[1].RunID != "run-001" {
		t.Errorf("events[1].RunID = %q, want %q", events[1].RunID, "run-001")
	}
	if events[1].Pipeline != "etl-pipeline" {
		t.Errorf("events[1].Pipeline = %q, want %q", events[1].Pipeline, "etl-pipeline")
	}
	if events[1].Schedule != "daily" {
		t.Errorf("events[1].Schedule = %q, want %q", events[1].Schedule, "daily")
	}
	if events[1].Date != "2026-03-14" {
		t.Errorf("events[1].Date = %q, want %q", events[1].Date, "2026-03-14")
	}
}

func TestDynamoDBState_ReadJobEvents_Empty(t *testing.T) {
	t.Parallel()

	mock := &mockDynamoDBAPI{
		QueryFn: func(_ context.Context, _ *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			return &dynamodb.QueryOutput{
				Items: []map[string]dynamodbtypes.AttributeValue{},
			}, nil
		},
	}

	store := chaosaws.NewDynamoDBState(mock, testTable)
	events, err := store.ReadJobEvents(context.Background(), "etl-pipeline", "daily", "2026-01-01")
	if err != nil {
		t.Fatalf("ReadJobEvents() error = %v", err)
	}
	if len(events) != 0 {
		t.Errorf("ReadJobEvents() returned %d events, want 0", len(events))
	}
}
