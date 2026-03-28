package interlocksuite

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// WatchdogModule monitors pipeline health by checking sensor deadlines,
// schedule adherence, job timeouts, and trigger staleness. It runs after SLA
// and before Validation in the module chain.
//
// Scenarios covered:
//   - sensor-deadline-expired: sensor past its configured deadline
//   - schedule-missed: cron schedule not met within tolerance
//   - irregular-schedule-missed: inclusion calendar date missed within tolerance
//   - sfn-timeout: running job exceeds timeout_seconds
//   - stale-trigger: running/triggered job with stale sensor writes "stale" to trigger store
type WatchdogModule struct{}

// NewWatchdogModule returns a new WatchdogModule.
func NewWatchdogModule() *WatchdogModule { return &WatchdogModule{} }

// Name returns the module identifier.
func (m *WatchdogModule) Name() string { return "watchdog" }

// Evaluate inspects the pipeline config for watchdog-relevant sections
// (sensor_deadline, schedule, job.timeout_seconds, job.stale_threshold_minutes)
// and emits events or writes trigger state when conditions are met.
// Short-circuits when events already exist for the pipeline.
func (m *WatchdogModule) Evaluate(ctx context.Context, p EvalParams) error {
	// Short-circuit: if events already exist for this pipeline, skip.
	existing, err := p.EventWriter.ReadEvents(ctx, p.Pipeline, "")
	if err != nil {
		return fmt.Errorf("watchdog module: read existing events: %w", err)
	}
	if len(existing) > 0 {
		return nil
	}

	// 1. Sensor deadline check.
	if emitted, emitErr := m.evaluateSensorDeadline(ctx, p); emitErr != nil {
		return emitErr
	} else if emitted {
		return nil
	}

	// 2. Cron schedule missed check.
	if emitted, emitErr := m.evaluateCronSchedule(ctx, p); emitErr != nil {
		return emitErr
	} else if emitted {
		return nil
	}

	// 3. Irregular schedule missed check.
	if emitted, emitErr := m.evaluateIrregularSchedule(ctx, p); emitErr != nil {
		return emitErr
	} else if emitted {
		return nil
	}

	// 4. Job timeout check.
	if emitted, emitErr := m.evaluateJobTimeout(ctx, p); emitErr != nil {
		return emitErr
	} else if emitted {
		return nil
	}

	// 5. Stale trigger check (writes to trigger store, does not emit events).
	if err := m.evaluateStaleTrigger(ctx, p); err != nil {
		return err
	}

	return nil
}

// evaluateSensorDeadline checks whether any sensor is past the configured
// sensor_deadline. Returns (true, nil) when SENSOR_DEADLINE_EXPIRED was
// emitted.
func (m *WatchdogModule) evaluateSensorDeadline(ctx context.Context, p EvalParams) (bool, error) {
	deadlineStr, ok := p.Config["sensor_deadline"].(string)
	if !ok || deadlineStr == "" {
		return false, nil
	}

	deadline, err := parseTimeOfDay(p.Clock.Now(), deadlineStr)
	if err != nil {
		return false, fmt.Errorf("watchdog module: parse sensor_deadline %q: %w", deadlineStr, err)
	}

	now := p.Clock.Now()
	if !now.After(deadline) {
		return false, nil // not past deadline yet
	}

	// Check sensors: if any is PENDING or stale (LastUpdated before deadline).
	for _, key := range p.SensorKeys {
		sd, err := p.Store.ReadSensor(ctx, p.Pipeline, key)
		if err != nil || sd.Key == "" {
			continue
		}
		if sd.Status == types.SensorStatusPending || sd.LastUpdated.Before(deadline) {
			p.EventWriter.Emit(InterlockEventRecord{
				PipelineID: p.Pipeline,
				EventType:  "SENSOR_DEADLINE_EXPIRED",
				Timestamp:  now,
			})
			return true, nil
		}
	}

	return false, nil
}

// evaluateCronSchedule checks whether a cron-scheduled pipeline has missed its
// expected schedule within the configured tolerance. Returns (true, nil) when
// SCHEDULE_MISSED was emitted.
func (m *WatchdogModule) evaluateCronSchedule(ctx context.Context, p EvalParams) (bool, error) {
	schedRaw, ok := p.Config["schedule"]
	if !ok {
		return false, nil
	}
	schedMap, ok := schedRaw.(map[string]any)
	if !ok {
		return false, nil
	}

	// Only applies when cron field is present.
	cronStr := configString(schedMap, "cron")
	if cronStr == "" {
		return false, nil
	}

	toleranceMin, err := configFloat64(schedMap, "tolerance_minutes")
	if err != nil || toleranceMin <= 0 {
		return false, nil // no tolerance configured
	}

	// Read last_schedule_time from sensor metadata.
	lastScheduleTime, found := m.readSensorMetadataTime(ctx, p, "last_schedule_time", "schedule_time")
	if !found {
		return false, nil
	}

	elapsed := p.Clock.Now().Sub(lastScheduleTime).Minutes()
	if elapsed > toleranceMin {
		p.EventWriter.Emit(InterlockEventRecord{
			PipelineID: p.Pipeline,
			EventType:  "SCHEDULE_MISSED",
			Timestamp:  p.Clock.Now(),
		})
		return true, nil
	}

	return false, nil
}

// evaluateIrregularSchedule checks whether an inclusion_calendar pipeline has
// missed an expected scheduled date within tolerance. Returns (true, nil) when
// IRREGULAR_SCHEDULE_MISSED was emitted.
func (m *WatchdogModule) evaluateIrregularSchedule(ctx context.Context, p EvalParams) (bool, error) {
	schedRaw, ok := p.Config["schedule"]
	if !ok {
		return false, nil
	}
	schedMap, ok := schedRaw.(map[string]any)
	if !ok {
		return false, nil
	}

	schedType := configString(schedMap, "type")
	if schedType != "inclusion_calendar" {
		return false, nil
	}

	toleranceMin, err := configFloat64(schedMap, "tolerance_minutes")
	if err != nil || toleranceMin <= 0 {
		return false, nil
	}

	dates := configStringSlice(schedMap, "dates")
	if len(dates) == 0 {
		return false, nil
	}

	now := p.Clock.Now()
	today := now.Format("2006-01-02")

	// Find the most recent date in dates that is <= today.
	var nearestDate time.Time
	found := false
	for _, ds := range dates {
		d, parseErr := time.Parse("2006-01-02", ds)
		if parseErr != nil {
			continue
		}
		if ds <= today && (!found || d.After(nearestDate)) {
			nearestDate = d
			found = true
		}
	}
	if !found {
		return false, nil
	}

	// Check if the expected date + tolerance has passed without execution.
	deadlineForDate := nearestDate.Add(time.Duration(toleranceMin) * time.Minute)
	if !now.After(deadlineForDate) {
		return false, nil // still within tolerance
	}

	// Check sensor metadata for evidence of execution.
	execTime, hasExecution := m.readSensorMetadataTime(ctx, p, "last_schedule_time", "schedule_time")
	if hasExecution && execTime.After(nearestDate) {
		return false, nil // executed after the expected date
	}

	p.EventWriter.Emit(InterlockEventRecord{
		PipelineID: p.Pipeline,
		EventType:  "IRREGULAR_SCHEDULE_MISSED",
		Timestamp:  now,
	})
	return true, nil
}

// evaluateJobTimeout checks whether a running job has exceeded its configured
// timeout. Returns (true, nil) when SFN_TIMEOUT was emitted.
func (m *WatchdogModule) evaluateJobTimeout(ctx context.Context, p EvalParams) (bool, error) {
	jobRaw, ok := p.Config["job"]
	if !ok {
		return false, nil
	}
	jobMap, ok := jobRaw.(map[string]any)
	if !ok {
		return false, nil
	}

	timeoutSec, err := configFloat64(jobMap, "timeout_seconds")
	if err != nil || timeoutSec <= 0 {
		return false, nil
	}

	// Read trigger status — only check if running.
	triggerStatus, err := p.Store.ReadTriggerStatus(ctx, adapter.TriggerKey{
		Pipeline: p.Pipeline,
		Schedule: "default",
		Date:     "default",
	})
	if err != nil || triggerStatus != "running" {
		return false, nil
	}

	// Read start time from sensor metadata.
	startTime, found := m.readSensorMetadataTime(ctx, p, "trigger_time", "schedule_time")
	if !found {
		return false, nil
	}

	elapsed := p.Clock.Now().Sub(startTime).Seconds()
	if elapsed > timeoutSec {
		p.EventWriter.Emit(InterlockEventRecord{
			PipelineID: p.Pipeline,
			EventType:  "SFN_TIMEOUT",
			Timestamp:  p.Clock.Now(),
		})
		return true, nil
	}

	return false, nil
}

// evaluateStaleTrigger checks whether a running/triggered job has sensors that
// have been stale beyond the configured threshold. When detected, writes
// "stale" to the trigger store instead of emitting an event. The
// TriggerStateAsserter reads this status for CondIsStale assertions.
func (m *WatchdogModule) evaluateStaleTrigger(ctx context.Context, p EvalParams) error {
	jobRaw, ok := p.Config["job"]
	if !ok {
		return nil
	}
	jobMap, ok := jobRaw.(map[string]any)
	if !ok {
		return nil
	}

	thresholdMin, err := configFloat64(jobMap, "stale_threshold_minutes")
	if err != nil || thresholdMin <= 0 {
		return nil
	}

	// Read trigger status — only act if running or triggered.
	triggerStatus, err := p.Store.ReadTriggerStatus(ctx, adapter.TriggerKey{
		Pipeline: p.Pipeline,
		Schedule: "default",
		Date:     "default",
	})
	if err != nil {
		return nil
	}
	if triggerStatus != "running" && triggerStatus != "triggered" {
		return nil
	}

	// Check sensor staleness.
	now := p.Clock.Now()
	threshold := time.Duration(thresholdMin * float64(time.Minute))
	for _, key := range p.SensorKeys {
		sd, readErr := p.Store.ReadSensor(ctx, p.Pipeline, key)
		if readErr != nil || sd.Key == "" {
			continue
		}
		if now.Sub(sd.LastUpdated) > threshold {
			// Write "stale" to trigger store.
			writeErr := p.Store.WriteTriggerStatus(ctx, adapter.TriggerKey{
				Pipeline: p.Pipeline,
				Schedule: "default",
				Date:     "default",
			}, "stale")
			if writeErr != nil {
				return fmt.Errorf("watchdog module: write stale trigger status: %w", writeErr)
			}
			return nil
		}
	}

	return nil
}

// readSensorMetadataTime reads the first matching metadata key from any sensor
// in p.SensorKeys and parses it as RFC3339Nano. Returns the parsed time and
// true if found.
func (m *WatchdogModule) readSensorMetadataTime(ctx context.Context, p EvalParams, keys ...string) (time.Time, bool) {
	for _, sensorKey := range p.SensorKeys {
		sd, err := p.Store.ReadSensor(ctx, p.Pipeline, sensorKey)
		if err != nil || sd.Key == "" {
			continue
		}
		for _, metaKey := range keys {
			if val, ok := sd.Metadata[metaKey]; ok && val != "" {
				t, parseErr := time.Parse(time.RFC3339Nano, val)
				if parseErr == nil {
					return t, true
				}
			}
		}
	}
	return time.Time{}, false
}

// parseTimeOfDay combines the date portion of now with an HH:MM time-of-day
// string. Returns the resulting time in the same location as now.
func parseTimeOfDay(now time.Time, tod string) (time.Time, error) {
	parts := strings.SplitN(tod, ":", 2)
	if len(parts) != 2 {
		return time.Time{}, fmt.Errorf("invalid time-of-day format %q, expected HH:MM", tod)
	}
	hour, err := strconv.Atoi(parts[0])
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid hour in %q: %w", tod, err)
	}
	minute, err := strconv.Atoi(parts[1])
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid minute in %q: %w", tod, err)
	}
	y, mo, d := now.Date()
	return time.Date(y, mo, d, hour, minute, 0, 0, now.Location()), nil
}

// configFloat64 extracts a float64 from a map key. Handles float64, int,
// and string representations.
func configFloat64(m map[string]any, key string) (float64, error) {
	v, ok := m[key]
	if !ok {
		return 0, fmt.Errorf("key %q not found", key)
	}
	switch tv := v.(type) {
	case float64:
		return tv, nil
	case int:
		return float64(tv), nil
	case string:
		return strconv.ParseFloat(tv, 64)
	default:
		return 0, fmt.Errorf("key %q has unsupported type %T", key, v)
	}
}
