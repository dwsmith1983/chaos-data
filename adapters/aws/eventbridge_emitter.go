package aws

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	ebtypes "github.com/aws/aws-sdk-go-v2/service/eventbridge/types"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// Compile-time interface assertion.
var _ adapter.EventEmitter = (*EventBridgeEmitter)(nil)

// EventBridgeEmitter implements adapter.EventEmitter by publishing chaos
// events to an AWS EventBridge bus.
type EventBridgeEmitter struct {
	api     EventBridgeAPI
	busName string
	source  string
}

// NewEventBridgeEmitter creates an EventBridgeEmitter that publishes events
// to the given bus. If busName is empty, it defaults to "default".
func NewEventBridgeEmitter(api EventBridgeAPI, busName string) *EventBridgeEmitter {
	if busName == "" {
		busName = "default"
	}
	return &EventBridgeEmitter{
		api:     api,
		busName: busName,
		source:  "chaos-data",
	}
}

// Emit marshals the ChaosEvent to JSON and publishes it to EventBridge.
// It returns an error if the API call fails or if EventBridge reports a
// partial failure (FailedEntryCount > 0).
func (e *EventBridgeEmitter) Emit(ctx context.Context, event types.ChaosEvent) error {
	detail, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("eventbridge emitter: marshal event: %w", err)
	}

	detailStr := string(detail)
	detailType := "chaos-data.fault-injected"

	input := &eventbridge.PutEventsInput{
		Entries: []ebtypes.PutEventsRequestEntry{
			{
				Source:       &e.source,
				DetailType:  &detailType,
				EventBusName: &e.busName,
				Detail:      &detailStr,
			},
		},
	}

	output, err := e.api.PutEvents(ctx, input)
	if err != nil {
		return fmt.Errorf("eventbridge emitter: put events: %w", err)
	}

	if output.FailedEntryCount > 0 {
		if len(output.Entries) > 0 {
			entry := output.Entries[0]
			var code, msg string
			if entry.ErrorCode != nil {
				code = *entry.ErrorCode
			}
			if entry.ErrorMessage != nil {
				msg = *entry.ErrorMessage
			}
			return fmt.Errorf("eventbridge emitter: entry failed: %s: %s", code, msg)
		}
		return fmt.Errorf("eventbridge emitter: %d entries failed", output.FailedEntryCount)
	}

	return nil
}
