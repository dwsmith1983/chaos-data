package interlock

import (
	"context"
	"fmt"
	"time"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// Asserter provides polling-based assertions for verifying chaos mutation
// effects against state stores and event readers.
type Asserter struct {
	store        adapter.StateStore
	reader       adapter.EventReader
	pollInterval time.Duration
}

// NewAsserter creates a new Asserter with the given dependencies.
func NewAsserter(store adapter.StateStore, reader adapter.EventReader, pollInterval time.Duration) *Asserter {
	return &Asserter{
		store:        store,
		reader:       reader,
		pollInterval: pollInterval,
	}
}

// AssertSensorStatus polls the state store until the sensor identified by
// pipeline and sensorKey has the expectedStatus, or until the timeout or
// context cancellation occurs.
func (a *Asserter) AssertSensorStatus(ctx context.Context, pipeline, sensorKey string, expectedStatus types.SensorStatus, timeout time.Duration) error {
	deadline := time.After(timeout)
	ticker := time.NewTicker(a.pollInterval)
	defer ticker.Stop()

	var lastStatus types.SensorStatus
	var lastErr error
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline:
			if lastErr != nil {
				return fmt.Errorf("assert sensor status timed out: pipeline=%q key=%q want=%q got=%q (last error: %v)",
					pipeline, sensorKey, expectedStatus, lastStatus, lastErr)
			}
			return fmt.Errorf("assert sensor status timed out: pipeline=%q key=%q want=%q got=%q", pipeline, sensorKey, expectedStatus, lastStatus)
		case <-ticker.C:
			sensor, err := a.store.ReadSensor(ctx, pipeline, sensorKey)
			if err != nil {
				lastErr = err
				continue
			}
			lastStatus = sensor.Status
			if sensor.Status == expectedStatus {
				return nil
			}
		}
	}
}

// AssertTriggerStatus polls the state store until the trigger identified by
// key has the expectedStatus, or until the timeout or context cancellation
// occurs.
func (a *Asserter) AssertTriggerStatus(ctx context.Context, key adapter.TriggerKey, expectedStatus string, timeout time.Duration) error {
	deadline := time.After(timeout)
	ticker := time.NewTicker(a.pollInterval)
	defer ticker.Stop()

	var lastStatus string
	var lastErr error
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline:
			if lastErr != nil {
				return fmt.Errorf("assert trigger status timed out: pipeline=%q schedule=%q date=%q want=%q got=%q (last error: %v)",
					key.Pipeline, key.Schedule, key.Date, expectedStatus, lastStatus, lastErr)
			}
			return fmt.Errorf("assert trigger status timed out: pipeline=%q schedule=%q date=%q want=%q got=%q", key.Pipeline, key.Schedule, key.Date, expectedStatus, lastStatus)
		case <-ticker.C:
			status, err := a.store.ReadTriggerStatus(ctx, key)
			if err != nil {
				lastErr = err
				continue
			}
			lastStatus = status
			if status == expectedStatus {
				return nil
			}
		}
	}
}

// AssertEventEmitted polls the event reader's Manifest until an event
// matching the given scenario and mutation is found, or until the timeout or
// context cancellation occurs.
func (a *Asserter) AssertEventEmitted(ctx context.Context, scenario, mutation string, timeout time.Duration) error {
	deadline := time.After(timeout)
	ticker := time.NewTicker(a.pollInterval)
	defer ticker.Stop()

	var lastErr error
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline:
			if lastErr != nil {
				return fmt.Errorf("assert event emitted timed out: scenario=%q mutation=%q (last error: %v)",
					scenario, mutation, lastErr)
			}
			return fmt.Errorf("assert event emitted timed out: scenario=%q mutation=%q", scenario, mutation)
		case <-ticker.C:
			events, err := a.reader.Manifest(ctx)
			if err != nil {
				lastErr = err
				continue
			}
			for _, e := range events {
				if e.Scenario == scenario && e.Mutation == mutation {
					return nil
				}
			}
		}
	}
}
