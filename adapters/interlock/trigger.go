package interlock

import (
	"context"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/mutation"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// Compile-time assertions that trigger wrappers implement mutation.Mutation.
var (
	_ mutation.Mutation = (*InterlockPhantomTrigger)(nil)
	_ mutation.Mutation = (*InterlockJobKill)(nil)
	_ mutation.Mutation = (*InterlockTriggerTimeout)(nil)
	_ mutation.Mutation = (*InterlockFalseSuccess)(nil)
)

// InterlockPhantomTrigger wraps a PhantomTriggerMutation with Interlock-specific
// pipeline prefix and default schedule enrichment.
type InterlockPhantomTrigger struct {
	inner *mutation.PhantomTriggerMutation
	cfg   Config
}

// NewInterlockPhantomTrigger creates a new InterlockPhantomTrigger.
func NewInterlockPhantomTrigger(store adapter.StateStore, cfg Config) *InterlockPhantomTrigger {
	return &InterlockPhantomTrigger{
		inner: mutation.NewPhantomTriggerMutation(store),
		cfg:   cfg,
	}
}

// Type returns "interlock-phantom-trigger".
func (t *InterlockPhantomTrigger) Type() string { return "interlock-phantom-trigger" }

// Apply enriches params with PipelinePrefix and DefaultSchedule, then
// delegates to the underlying PhantomTriggerMutation.
func (t *InterlockPhantomTrigger) Apply(ctx context.Context, obj types.DataObject, transport adapter.DataTransport, params map[string]string) (types.MutationRecord, error) {
	enriched := enrichTriggerParams(params, t.cfg)
	rec, err := t.inner.Apply(ctx, obj, transport, enriched)
	if err == nil {
		rec.Mutation = t.Type()
	}
	return rec, err
}

// InterlockJobKill wraps a JobKillMutation with Interlock-specific
// pipeline prefix and default schedule enrichment.
type InterlockJobKill struct {
	inner *mutation.JobKillMutation
	cfg   Config
}

// NewInterlockJobKill creates a new InterlockJobKill.
func NewInterlockJobKill(store adapter.StateStore, cfg Config) *InterlockJobKill {
	return &InterlockJobKill{
		inner: mutation.NewJobKillMutation(store),
		cfg:   cfg,
	}
}

// Type returns "interlock-job-kill".
func (j *InterlockJobKill) Type() string { return "interlock-job-kill" }

// Apply enriches params with PipelinePrefix and DefaultSchedule, then
// delegates to the underlying JobKillMutation.
func (j *InterlockJobKill) Apply(ctx context.Context, obj types.DataObject, transport adapter.DataTransport, params map[string]string) (types.MutationRecord, error) {
	enriched := enrichTriggerParams(params, j.cfg)
	rec, err := j.inner.Apply(ctx, obj, transport, enriched)
	if err == nil {
		rec.Mutation = j.Type()
	}
	return rec, err
}

// InterlockTriggerTimeout wraps a TriggerTimeoutMutation with Interlock-specific
// pipeline prefix and default schedule enrichment.
type InterlockTriggerTimeout struct {
	inner *mutation.TriggerTimeoutMutation
	cfg   Config
}

// NewInterlockTriggerTimeout creates a new InterlockTriggerTimeout.
func NewInterlockTriggerTimeout(store adapter.StateStore, cfg Config) *InterlockTriggerTimeout {
	return &InterlockTriggerTimeout{
		inner: mutation.NewTriggerTimeoutMutation(store),
		cfg:   cfg,
	}
}

// Type returns "interlock-trigger-timeout".
func (tt *InterlockTriggerTimeout) Type() string { return "interlock-trigger-timeout" }

// Apply enriches params with PipelinePrefix and DefaultSchedule, then
// delegates to the underlying TriggerTimeoutMutation.
func (tt *InterlockTriggerTimeout) Apply(ctx context.Context, obj types.DataObject, transport adapter.DataTransport, params map[string]string) (types.MutationRecord, error) {
	enriched := enrichTriggerParams(params, tt.cfg)
	rec, err := tt.inner.Apply(ctx, obj, transport, enriched)
	if err == nil {
		rec.Mutation = tt.Type()
	}
	return rec, err
}

// InterlockFalseSuccess wraps a FalseSuccessMutation with Interlock-specific
// pipeline prefix and default schedule enrichment.
type InterlockFalseSuccess struct {
	inner *mutation.FalseSuccessMutation
	cfg   Config
}

// NewInterlockFalseSuccess creates a new InterlockFalseSuccess.
func NewInterlockFalseSuccess(store adapter.StateStore, cfg Config) *InterlockFalseSuccess {
	return &InterlockFalseSuccess{
		inner: mutation.NewFalseSuccessMutation(store),
		cfg:   cfg,
	}
}

// Type returns "interlock-false-success".
func (f *InterlockFalseSuccess) Type() string { return "interlock-false-success" }

// Apply enriches params with PipelinePrefix and DefaultSchedule, then
// delegates to the underlying FalseSuccessMutation.
func (f *InterlockFalseSuccess) Apply(ctx context.Context, obj types.DataObject, transport adapter.DataTransport, params map[string]string) (types.MutationRecord, error) {
	enriched := enrichTriggerParams(params, f.cfg)
	rec, err := f.inner.Apply(ctx, obj, transport, enriched)
	if err == nil {
		rec.Mutation = f.Type()
	}
	return rec, err
}

// enrichTriggerParams returns a copy of params with the pipeline value
// prefixed by Config.PipelinePrefix (if non-empty) and "schedule" set to
// Config.DefaultSchedule when absent from the original params.
func enrichTriggerParams(params map[string]string, cfg Config) map[string]string {
	enriched := make(map[string]string, len(params)+1)
	for k, v := range params {
		enriched[k] = v
	}
	if cfg.PipelinePrefix != "" {
		if pipeline, ok := enriched["pipeline"]; ok {
			enriched["pipeline"] = cfg.PipelinePrefix + pipeline
		}
	}
	if schedule := enriched["schedule"]; schedule == "" && cfg.DefaultSchedule != "" {
		enriched["schedule"] = cfg.DefaultSchedule
	}
	return enriched
}
