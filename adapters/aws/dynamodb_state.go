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

// DeleteSensor removes the sensor record for the given pipeline and key.
func (s *DynamoDBState) DeleteSensor(ctx context.Context, pipeline, key string) error {
	_, err := s.api.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: &s.tableName,
		Key: map[string]dynamodbtypes.AttributeValue{
			"PK": &dynamodbtypes.AttributeValueMemberS{Value: SensorPK(pipeline)},
			"SK": &dynamodbtypes.AttributeValueMemberS{Value: SensorSK(key)},
		},
	})
	if err != nil {
		return fmt.Errorf("dynamodb delete sensor: %w", err)
	}
	return nil
}

// ReadChaosEvents returns all chaos events for the given experiment ID.
func (s *DynamoDBState) ReadChaosEvents(ctx context.Context, experimentID string) ([]types.ChaosEvent, error) {
	pk := ChaosPK(experimentID)
	expr := "PK = :pk"
	out, err := s.api.Query(ctx, &dynamodb.QueryInput{
		TableName:              &s.tableName,
		KeyConditionExpression: &expr,
		ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
			":pk": &dynamodbtypes.AttributeValueMemberS{Value: pk},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("dynamodb read chaos events: %w", err)
	}

	events := make([]types.ChaosEvent, 0, len(out.Items))
	for _, item := range out.Items {
		event, err := unmarshalChaosEvent(item)
		if err != nil {
			return nil, fmt.Errorf("dynamodb read chaos events: %w", err)
		}
		events = append(events, event)
	}
	return events, nil
}

// WritePipelineConfig stores a pipeline configuration blob.
func (s *DynamoDBState) WritePipelineConfig(_ context.Context, _ string, _ []byte) error {
	return fmt.Errorf("WritePipelineConfig: not yet implemented")
}

// ReadPipelineConfig retrieves a pipeline configuration blob.
func (s *DynamoDBState) ReadPipelineConfig(_ context.Context, _ string) ([]byte, error) {
	return nil, fmt.Errorf("ReadPipelineConfig: not yet implemented")
}

// DeleteByPrefix removes all state entries matching the given prefix.
func (s *DynamoDBState) DeleteByPrefix(_ context.Context, _ string) error {
	return fmt.Errorf("DeleteByPrefix: not yet implemented")
}

// CountReruns returns the number of reruns for a pipeline/schedule/date.
func (s *DynamoDBState) CountReruns(_ context.Context, _, _, _ string) (int, error) {
	return 0, fmt.Errorf("CountReruns: not yet implemented")
}

// WriteRerun records a rerun event.
func (s *DynamoDBState) WriteRerun(_ context.Context, _, _, _, _ string) error {
	return fmt.Errorf("WriteRerun: not yet implemented")
}

// ReadJobEvents returns job events for a pipeline/schedule/date.
func (s *DynamoDBState) ReadJobEvents(_ context.Context, _, _, _ string) ([]adapter.JobEvent, error) {
	return nil, fmt.Errorf("ReadJobEvents: not yet implemented")
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

// unmarshalChaosEvent converts a DynamoDB item map to a types.ChaosEvent.
func unmarshalChaosEvent(item map[string]dynamodbtypes.AttributeValue) (types.ChaosEvent, error) {
	var event types.ChaosEvent

	if v, ok := item["id"]; ok {
		if sv, ok := v.(*dynamodbtypes.AttributeValueMemberS); ok {
			event.ID = sv.Value
		}
	}
	if v, ok := item["experiment_id"]; ok {
		if sv, ok := v.(*dynamodbtypes.AttributeValueMemberS); ok {
			event.ExperimentID = sv.Value
		}
	}
	if v, ok := item["scenario"]; ok {
		if sv, ok := v.(*dynamodbtypes.AttributeValueMemberS); ok {
			event.Scenario = sv.Value
		}
	}
	if v, ok := item["category"]; ok {
		if sv, ok := v.(*dynamodbtypes.AttributeValueMemberS); ok {
			event.Category = sv.Value
		}
	}
	if v, ok := item["severity"]; ok {
		if nv, ok := v.(*dynamodbtypes.AttributeValueMemberN); ok {
			n, err := strconv.Atoi(nv.Value)
			if err != nil {
				return types.ChaosEvent{}, fmt.Errorf("parse severity: %w", err)
			}
			event.Severity = types.Severity(n)
		}
	}
	if v, ok := item["target"]; ok {
		if sv, ok := v.(*dynamodbtypes.AttributeValueMemberS); ok {
			event.Target = sv.Value
		}
	}
	if v, ok := item["mutation"]; ok {
		if sv, ok := v.(*dynamodbtypes.AttributeValueMemberS); ok {
			event.Mutation = sv.Value
		}
	}
	if v, ok := item["timestamp"]; ok {
		if sv, ok := v.(*dynamodbtypes.AttributeValueMemberS); ok {
			t, err := time.Parse(time.RFC3339Nano, sv.Value)
			if err != nil {
				return types.ChaosEvent{}, fmt.Errorf("parse timestamp: %w", err)
			}
			event.Timestamp = t
		}
	}
	if v, ok := item["mode"]; ok {
		if sv, ok := v.(*dynamodbtypes.AttributeValueMemberS); ok {
			event.Mode = sv.Value
		}
	}
	if v, ok := item["params"]; ok {
		if mv, ok := v.(*dynamodbtypes.AttributeValueMemberM); ok {
			event.Params = make(map[string]string, len(mv.Value))
			for k, av := range mv.Value {
				if sv, ok := av.(*dynamodbtypes.AttributeValueMemberS); ok {
					event.Params[k] = sv.Value
				}
			}
		}
	}

	return event, nil
}
