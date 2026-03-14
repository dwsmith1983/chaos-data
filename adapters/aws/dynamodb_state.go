package aws

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// Compile-time assertion: DynamoDBState implements adapter.StateStore.
var _ adapter.StateStore = (*DynamoDBState)(nil)

// DynamoDBState implements adapter.StateStore backed by a single DynamoDB table.
type DynamoDBState struct {
	api       DynamoDBAPI
	tableName string
}

// NewDynamoDBState returns a new DynamoDBState that reads/writes the given table.
func NewDynamoDBState(api DynamoDBAPI, tableName string) *DynamoDBState {
	return &DynamoDBState{api: api, tableName: tableName}
}

// ReadSensor retrieves sensor data for the given pipeline and key.
// If the item does not exist, a zero-value SensorData is returned (no error).
func (s *DynamoDBState) ReadSensor(ctx context.Context, pipeline, key string) (adapter.SensorData, error) {
	out, err := s.api.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: &s.tableName,
		Key: map[string]dynamodbtypes.AttributeValue{
			"PK": &dynamodbtypes.AttributeValueMemberS{Value: SensorPK(pipeline)},
			"SK": &dynamodbtypes.AttributeValueMemberS{Value: SensorSK(key)},
		},
	})
	if err != nil {
		return adapter.SensorData{}, fmt.Errorf("dynamodb read sensor: %w", err)
	}

	if len(out.Item) == 0 {
		return adapter.SensorData{}, nil
	}

	data, err := unmarshalSensorData(out.Item)
	if err != nil {
		return adapter.SensorData{}, fmt.Errorf("dynamodb read sensor: %w", err)
	}
	return data, nil
}

// WriteSensor stores sensor data for the given pipeline and key.
func (s *DynamoDBState) WriteSensor(ctx context.Context, pipeline, key string, data adapter.SensorData) error {
	item := map[string]dynamodbtypes.AttributeValue{
		"PK":           &dynamodbtypes.AttributeValueMemberS{Value: SensorPK(pipeline)},
		"SK":           &dynamodbtypes.AttributeValueMemberS{Value: SensorSK(key)},
		"pipeline":     &dynamodbtypes.AttributeValueMemberS{Value: data.Pipeline},
		"key":          &dynamodbtypes.AttributeValueMemberS{Value: data.Key},
		"status":       &dynamodbtypes.AttributeValueMemberS{Value: string(data.Status)},
		"last_updated": &dynamodbtypes.AttributeValueMemberS{Value: data.LastUpdated.Format(time.RFC3339Nano)},
	}

	if len(data.Metadata) > 0 {
		m := make(map[string]dynamodbtypes.AttributeValue, len(data.Metadata))
		for k, v := range data.Metadata {
			m[k] = &dynamodbtypes.AttributeValueMemberS{Value: v}
		}
		item["metadata"] = &dynamodbtypes.AttributeValueMemberM{Value: m}
	}

	_, err := s.api.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &s.tableName,
		Item:      item,
	})
	if err != nil {
		return fmt.Errorf("dynamodb write sensor: %w", err)
	}
	return nil
}

// ReadTriggerStatus retrieves the trigger status for the given key.
// If the item does not exist, an empty string is returned (no error).
func (s *DynamoDBState) ReadTriggerStatus(ctx context.Context, key adapter.TriggerKey) (string, error) {
	out, err := s.api.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: &s.tableName,
		Key: map[string]dynamodbtypes.AttributeValue{
			"PK": &dynamodbtypes.AttributeValueMemberS{Value: TriggerPK(key.Pipeline)},
			"SK": &dynamodbtypes.AttributeValueMemberS{Value: TriggerSK(key.Schedule, key.Date)},
		},
	})
	if err != nil {
		return "", fmt.Errorf("dynamodb read trigger status: %w", err)
	}

	if len(out.Item) == 0 {
		return "", nil
	}

	attr, ok := out.Item["status"]
	if !ok {
		return "", nil
	}
	sv, ok := attr.(*dynamodbtypes.AttributeValueMemberS)
	if !ok {
		return "", nil
	}
	return sv.Value, nil
}

// WriteTriggerStatus stores the trigger status for the given key.
func (s *DynamoDBState) WriteTriggerStatus(ctx context.Context, key adapter.TriggerKey, status string) error {
	_, err := s.api.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &s.tableName,
		Item: map[string]dynamodbtypes.AttributeValue{
			"PK":     &dynamodbtypes.AttributeValueMemberS{Value: TriggerPK(key.Pipeline)},
			"SK":     &dynamodbtypes.AttributeValueMemberS{Value: TriggerSK(key.Schedule, key.Date)},
			"status": &dynamodbtypes.AttributeValueMemberS{Value: status},
		},
	})
	if err != nil {
		return fmt.Errorf("dynamodb write trigger status: %w", err)
	}
	return nil
}

// WriteEvent stores a chaos event in the DynamoDB table.
func (s *DynamoDBState) WriteEvent(ctx context.Context, event types.ChaosEvent) error {
	item := map[string]dynamodbtypes.AttributeValue{
		"PK":            &dynamodbtypes.AttributeValueMemberS{Value: ChaosPK(event.ExperimentID)},
		"SK":            &dynamodbtypes.AttributeValueMemberS{Value: ChaosSK(event.Timestamp, event.ID)},
		"id":            &dynamodbtypes.AttributeValueMemberS{Value: event.ID},
		"experiment_id": &dynamodbtypes.AttributeValueMemberS{Value: event.ExperimentID},
		"scenario":      &dynamodbtypes.AttributeValueMemberS{Value: event.Scenario},
		"category":      &dynamodbtypes.AttributeValueMemberS{Value: event.Category},
		"severity":      &dynamodbtypes.AttributeValueMemberN{Value: strconv.Itoa(int(event.Severity))},
		"target":        &dynamodbtypes.AttributeValueMemberS{Value: event.Target},
		"mutation":      &dynamodbtypes.AttributeValueMemberS{Value: event.Mutation},
		"timestamp":     &dynamodbtypes.AttributeValueMemberS{Value: event.Timestamp.Format(time.RFC3339Nano)},
		"mode":          &dynamodbtypes.AttributeValueMemberS{Value: event.Mode},
	}

	if len(event.Params) > 0 {
		m := make(map[string]dynamodbtypes.AttributeValue, len(event.Params))
		for k, v := range event.Params {
			m[k] = &dynamodbtypes.AttributeValueMemberS{Value: v}
		}
		item["params"] = &dynamodbtypes.AttributeValueMemberM{Value: m}
	}

	_, err := s.api.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &s.tableName,
		Item:      item,
	})
	if err != nil {
		return fmt.Errorf("dynamodb write event: %w", err)
	}
	return nil
}

// unmarshalSensorData converts a DynamoDB item map to an adapter.SensorData.
func unmarshalSensorData(item map[string]dynamodbtypes.AttributeValue) (adapter.SensorData, error) {
	var data adapter.SensorData

	if v, ok := item["pipeline"]; ok {
		if sv, ok := v.(*dynamodbtypes.AttributeValueMemberS); ok {
			data.Pipeline = sv.Value
		}
	}
	if v, ok := item["key"]; ok {
		if sv, ok := v.(*dynamodbtypes.AttributeValueMemberS); ok {
			data.Key = sv.Value
		}
	}
	if v, ok := item["status"]; ok {
		if sv, ok := v.(*dynamodbtypes.AttributeValueMemberS); ok {
			data.Status = types.SensorStatus(sv.Value)
		}
	}
	if v, ok := item["last_updated"]; ok {
		if sv, ok := v.(*dynamodbtypes.AttributeValueMemberS); ok {
			t, err := time.Parse(time.RFC3339Nano, sv.Value)
			if err != nil {
				return adapter.SensorData{}, fmt.Errorf("parse last_updated: %w", err)
			}
			data.LastUpdated = t
		}
	}
	if v, ok := item["metadata"]; ok {
		if mv, ok := v.(*dynamodbtypes.AttributeValueMemberM); ok {
			data.Metadata = make(map[string]string, len(mv.Value))
			for k, av := range mv.Value {
				if sv, ok := av.(*dynamodbtypes.AttributeValueMemberS); ok {
					data.Metadata[k] = sv.Value
				}
			}
		}
	}

	return data, nil
}
