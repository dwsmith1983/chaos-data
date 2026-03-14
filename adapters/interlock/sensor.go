package interlock

import (
	"context"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/mutation"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// Compile-time assertions that sensor wrappers implement mutation.Mutation.
var (
	_ mutation.Mutation = (*InterlockStaleSensor)(nil)
	_ mutation.Mutation = (*InterlockPhantomSensor)(nil)
	_ mutation.Mutation = (*InterlockSplitSensor)(nil)
)

// InterlockStaleSensor wraps a StaleSensorMutation with Interlock-specific
// pipeline prefix enrichment.
type InterlockStaleSensor struct {
	inner *mutation.StaleSensorMutation
	cfg   Config
}

// NewInterlockStaleSensor creates a new InterlockStaleSensor.
func NewInterlockStaleSensor(store adapter.StateStore, cfg Config) *InterlockStaleSensor {
	return &InterlockStaleSensor{
		inner: mutation.NewStaleSensorMutation(store),
		cfg:   cfg,
	}
}

// Type returns "interlock-stale-sensor".
func (s *InterlockStaleSensor) Type() string { return "interlock-stale-sensor" }

// Apply enriches params with PipelinePrefix and delegates to the underlying
// StaleSensorMutation.
func (s *InterlockStaleSensor) Apply(ctx context.Context, obj types.DataObject, transport adapter.DataTransport, params map[string]string) (types.MutationRecord, error) {
	enriched := enrichSensorParams(params, s.cfg)
	rec, err := s.inner.Apply(ctx, obj, transport, enriched)
	if err == nil {
		rec.Mutation = s.Type()
	}
	return rec, err
}

// InterlockPhantomSensor wraps a PhantomSensorMutation with Interlock-specific
// pipeline prefix enrichment.
type InterlockPhantomSensor struct {
	inner *mutation.PhantomSensorMutation
	cfg   Config
}

// NewInterlockPhantomSensor creates a new InterlockPhantomSensor.
func NewInterlockPhantomSensor(store adapter.StateStore, cfg Config) *InterlockPhantomSensor {
	return &InterlockPhantomSensor{
		inner: mutation.NewPhantomSensorMutation(store),
		cfg:   cfg,
	}
}

// Type returns "interlock-phantom-sensor".
func (s *InterlockPhantomSensor) Type() string { return "interlock-phantom-sensor" }

// Apply enriches params with PipelinePrefix and delegates to the underlying
// PhantomSensorMutation.
func (s *InterlockPhantomSensor) Apply(ctx context.Context, obj types.DataObject, transport adapter.DataTransport, params map[string]string) (types.MutationRecord, error) {
	enriched := enrichSensorParams(params, s.cfg)
	rec, err := s.inner.Apply(ctx, obj, transport, enriched)
	if err == nil {
		rec.Mutation = s.Type()
	}
	return rec, err
}

// InterlockSplitSensor wraps a SplitSensorMutation with Interlock-specific
// pipeline prefix enrichment.
type InterlockSplitSensor struct {
	inner *mutation.SplitSensorMutation
	cfg   Config
}

// NewInterlockSplitSensor creates a new InterlockSplitSensor.
func NewInterlockSplitSensor(store adapter.StateStore, cfg Config) *InterlockSplitSensor {
	return &InterlockSplitSensor{
		inner: mutation.NewSplitSensorMutation(store),
		cfg:   cfg,
	}
}

// Type returns "interlock-split-sensor".
func (s *InterlockSplitSensor) Type() string { return "interlock-split-sensor" }

// Apply enriches params with PipelinePrefix and delegates to the underlying
// SplitSensorMutation.
func (s *InterlockSplitSensor) Apply(ctx context.Context, obj types.DataObject, transport adapter.DataTransport, params map[string]string) (types.MutationRecord, error) {
	enriched := enrichSensorParams(params, s.cfg)
	rec, err := s.inner.Apply(ctx, obj, transport, enriched)
	if err == nil {
		rec.Mutation = s.Type()
	}
	return rec, err
}

// enrichSensorParams returns a copy of params with the pipeline value
// prefixed by Config.PipelinePrefix (if non-empty).
func enrichSensorParams(params map[string]string, cfg Config) map[string]string {
	enriched := make(map[string]string, len(params))
	for k, v := range params {
		enriched[k] = v
	}
	if cfg.PipelinePrefix != "" {
		if pipeline, ok := enriched["pipeline"]; ok {
			enriched["pipeline"] = cfg.PipelinePrefix + pipeline
		}
	}
	return enriched
}
