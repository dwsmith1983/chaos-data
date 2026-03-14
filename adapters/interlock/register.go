package interlock

import (
	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/mutation"
)

// RegisterAll registers all Interlock sensor and trigger mutations with the
// given registry. It returns the first registration error encountered.
func RegisterAll(reg *mutation.Registry, store adapter.StateStore, cfg Config) error {
	if err := RegisterSensors(reg, store, cfg); err != nil {
		return err
	}
	return RegisterTriggers(reg, store, cfg)
}

// RegisterSensors registers the three Interlock sensor mutations:
// InterlockStaleSensor, InterlockPhantomSensor, and InterlockSplitSensor.
func RegisterSensors(reg *mutation.Registry, store adapter.StateStore, cfg Config) error {
	sensors := []mutation.Mutation{
		NewInterlockStaleSensor(store, cfg),
		NewInterlockPhantomSensor(store, cfg),
		NewInterlockSplitSensor(store, cfg),
	}
	for _, m := range sensors {
		if err := reg.Register(m); err != nil {
			return err
		}
	}
	return nil
}

// RegisterTriggers registers the four Interlock trigger mutations:
// InterlockPhantomTrigger, InterlockJobKill, InterlockTriggerTimeout,
// and InterlockFalseSuccess.
func RegisterTriggers(reg *mutation.Registry, store adapter.StateStore, cfg Config) error {
	triggers := []mutation.Mutation{
		NewInterlockPhantomTrigger(store, cfg),
		NewInterlockJobKill(store, cfg),
		NewInterlockTriggerTimeout(store, cfg),
		NewInterlockFalseSuccess(store, cfg),
	}
	for _, m := range triggers {
		if err := reg.Register(m); err != nil {
			return err
		}
	}
	return nil
}
