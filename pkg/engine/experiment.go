package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// Experiment represents a time-boxed chaos experiment run.
type Experiment struct {
	// ID is a unique identifier for this experiment.
	ID string

	config    types.ExperimentConfig
	state     types.ExperimentState
	startTime time.Time
	endTime   time.Time
	events    []types.ChaosEvent
	records   []types.MutationRecord
	err       error
	mu        sync.Mutex
	cancel    context.CancelFunc
	done      chan struct{}

	// resolver is an optional DependencyResolver used by BlastRadius().
	// It is set by StartExperiment when the engine has a resolver configured.
	resolver adapter.DependencyResolver
}

// State returns the current experiment state in a thread-safe manner.
func (exp *Experiment) State() types.ExperimentState {
	exp.mu.Lock()
	defer exp.mu.Unlock()
	return exp.state
}

// StartTime returns the experiment's start time.
func (exp *Experiment) StartTime() time.Time {
	exp.mu.Lock()
	defer exp.mu.Unlock()
	return exp.startTime
}

// EndTime returns the experiment's end time.
func (exp *Experiment) EndTime() time.Time {
	exp.mu.Lock()
	defer exp.mu.Unlock()
	return exp.endTime
}

// Events returns a copy of the experiment's collected events.
func (exp *Experiment) Events() []types.ChaosEvent {
	exp.mu.Lock()
	defer exp.mu.Unlock()
	result := make([]types.ChaosEvent, len(exp.events))
	copy(result, exp.events)
	return result
}

// Records returns a copy of the experiment's mutation records.
func (exp *Experiment) Records() []types.MutationRecord {
	exp.mu.Lock()
	defer exp.mu.Unlock()
	result := make([]types.MutationRecord, len(exp.records))
	copy(result, exp.records)
	return result
}

// Stop stops a running experiment immediately by canceling its context.
// The experiment state is set to ExperimentAborted.
func (exp *Experiment) Stop() {
	exp.mu.Lock()
	if exp.state == types.ExperimentRunning {
		exp.state = types.ExperimentAborted
		exp.endTime = time.Now()
	}
	cancel := exp.cancel
	exp.mu.Unlock()

	if cancel != nil {
		cancel()
	}
}

// Wait blocks until the experiment completes (either duration elapsed or stopped).
func (exp *Experiment) Wait() {
	<-exp.done
}

// Err returns any error produced by the engine run. It should be called after Wait.
func (exp *Experiment) Err() error {
	exp.mu.Lock()
	defer exp.mu.Unlock()
	return exp.err
}

// Manifest returns all chaos events from the experiment as a JSONL-formatted byte slice.
// Each event is marshaled as a single JSON line.
func (exp *Experiment) Manifest() ([]byte, error) {
	exp.mu.Lock()
	events := make([]types.ChaosEvent, len(exp.events))
	copy(events, exp.events)
	exp.mu.Unlock()

	var buf bytes.Buffer
	for _, ev := range events {
		data, err := json.Marshal(ev)
		if err != nil {
			return nil, fmt.Errorf("manifest: marshal event %q: %w", ev.ID, err)
		}
		buf.Write(data)
		buf.WriteByte('\n')
	}
	return buf.Bytes(), nil
}

// Stats returns current experiment statistics computed from collected events.
func (exp *Experiment) Stats() types.ExperimentStats {
	exp.mu.Lock()
	events := make([]types.ChaosEvent, len(exp.events))
	copy(events, exp.events)
	id := exp.ID
	start := exp.startTime
	end := exp.endTime
	exp.mu.Unlock()

	targets := make(map[string]struct{})
	pipelines := make(map[string]struct{})

	for _, ev := range events {
		targets[ev.Target] = struct{}{}
		pipelines[ev.Scenario] = struct{}{}
	}

	var affectedPct float64
	if len(targets) > 0 {
		// AffectedPct is the percentage of unique targets that had events,
		// relative to the total number of events. When we lack total object
		// count, we report 100% if there were any events.
		affectedPct = 100.0
	}

	return types.ExperimentStats{
		ExperimentID:      id,
		TotalEvents:       len(events),
		AffectedTargets:   len(targets),
		AffectedPipelines: len(pipelines),
		AffectedPct:       affectedPct,
		StartTime:         start,
		EndTime:           end,
	}
}

// BlastRadius computes the downstream impact of the experiment by resolving
// dependencies for every mutation record where Applied is true. It returns a
// BlastRadiusEntry per applied record. If no DependencyResolver was configured
// on the engine, BlastRadius returns nil. Errors from GetDownstream are silently
// ignored on a per-record basis (fail-open) — a resolver failure does not
// prevent the remaining records from being resolved.
//
// BlastRadius is intended to be called after Wait() returns.
func (exp *Experiment) BlastRadius(ctx context.Context) []types.BlastRadiusEntry {
	exp.mu.Lock()
	resolver := exp.resolver
	records := make([]types.MutationRecord, len(exp.records))
	copy(records, exp.records)
	exp.mu.Unlock()

	if resolver == nil {
		return nil
	}

	var entries []types.BlastRadiusEntry
	for _, r := range records {
		if !r.Applied {
			continue
		}
		downstream, err := resolver.GetDownstream(ctx, r.ObjectKey)
		if err != nil {
			// Fail-open: skip this record on resolver error.
			continue
		}
		entries = append(entries, types.BlastRadiusEntry{
			MutatedObject: r.ObjectKey,
			MutationType:  r.Mutation,
			Downstream:    downstream,
		})
	}
	return entries
}

// generateExperimentID creates a unique experiment identifier using
// timestamp and random components.
func generateExperimentID() string {
	return fmt.Sprintf("exp-%d-%04x",
		time.Now().UnixNano(),
		rand.Intn(0xFFFF), //nolint:gosec // not security-sensitive
	)
}

// StartExperiment creates and starts a new experiment with the given config.
// It runs the engine in a goroutine, respecting the experiment's duration as a time box.
func (e *Engine) StartExperiment(ctx context.Context, config types.ExperimentConfig) (*Experiment, error) {
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("start experiment: %w", err)
	}

	expID := generateExperimentID()

	// Create a context with timeout from the experiment duration.
	var expCtx context.Context
	var cancel context.CancelFunc
	if config.Duration.Duration > 0 {
		expCtx, cancel = context.WithTimeout(ctx, config.Duration.Duration)
	} else {
		expCtx, cancel = context.WithCancel(ctx)
	}

	// Create an event-collecting emitter that wraps the engine's existing emitter.
	exp := &Experiment{
		ID:        expID,
		config:    config,
		state:     types.ExperimentRunning,
		startTime: time.Now(),
		cancel:    cancel,
		done:      make(chan struct{}),
		resolver:  e.resolver,
	}

	// Create an intercepting emitter that collects events for the experiment.
	collector := &eventCollector{
		experiment: exp,
		delegate:   e.emitter,
	}

	// Build a copy of the engine with the collecting emitter.
	eng := New(
		e.config,
		e.transport,
		e.mutations,
		e.scenarios,
		WithEmitter(collector),
	)
	if e.safety != nil {
		eng.safety = e.safety
	}

	// Start the experiment goroutine.
	go func() {
		defer close(exp.done)

		records, runErr := eng.Run(expCtx)

		exp.mu.Lock()
		exp.records = records
		exp.err = runErr
		// Only transition to Completed if not already Aborted.
		if exp.state == types.ExperimentRunning {
			exp.state = types.ExperimentCompleted
			exp.endTime = time.Now()
		}
		exp.mu.Unlock()

		cancel()
	}()

	return exp, nil
}

// eventCollector is an EventEmitter that intercepts events and stores them
// on the experiment, then delegates to an optional underlying emitter.
type eventCollector struct {
	experiment *Experiment
	delegate   interface{ Emit(context.Context, types.ChaosEvent) error }
}

// Emit records the event on the experiment and delegates to the underlying emitter.
func (c *eventCollector) Emit(ctx context.Context, event types.ChaosEvent) error {
	event.ExperimentID = c.experiment.ID

	c.experiment.mu.Lock()
	c.experiment.events = append(c.experiment.events, event)
	c.experiment.mu.Unlock()

	if c.delegate != nil {
		return c.delegate.Emit(ctx, event)
	}
	return nil
}
