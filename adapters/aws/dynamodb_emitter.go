package aws

import (
	"context"
	"errors"
	"fmt"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// Compile-time interface assertion.
var _ adapter.EventEmitter = (*DynamoDBEmitter)(nil)

// DynamoDBEmitter implements adapter.EventEmitter by delegating to
// DynamoDBState.WriteEvent. This bridges the gap between the StateStore
// interface (WriteEvent) and the engine's EventEmitter interface (Emit).
type DynamoDBEmitter struct {
	state *DynamoDBState
}

// NewDynamoDBEmitter creates a DynamoDBEmitter backed by a DynamoDB table.
// The tableName is the events table where CHAOS# records are stored.
func NewDynamoDBEmitter(api DynamoDBAPI, tableName string) (*DynamoDBEmitter, error) {
	if tableName == "" {
		return nil, errors.New("dynamodb emitter: tableName must not be empty")
	}
	return &DynamoDBEmitter{state: NewDynamoDBState(api, tableName)}, nil
}

// Emit writes a chaos event to the DynamoDB events table.
func (e *DynamoDBEmitter) Emit(ctx context.Context, event types.ChaosEvent) error {
	if err := e.state.WriteEvent(ctx, event); err != nil {
		return fmt.Errorf("dynamodb emitter: %w", err)
	}
	return nil
}
