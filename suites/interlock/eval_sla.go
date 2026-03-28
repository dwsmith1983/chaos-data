package interlocksuite

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
)

// SLAModule evaluates absolute-deadline and relative-duration SLA conditions,
// emitting SLA_WARNING, SLA_BREACH, SLA_MET, RELATIVE_SLA_WARNING, or
// RELATIVE_SLA_BREACH events as appropriate.
//
// Chain position: after Safety, before Watchdog.
type SLAModule struct{}

// NewSLAModule returns a new SLAModule.
func NewSLAModule() *SLAModule { return &SLAModule{} }

// Name returns the module identifier.
func (m *SLAModule) Name() string { return "sla" }

// Evaluate inspects the pipeline config for an "sla" section and emits
// deadline/duration events when conditions are met. Silently returns nil when
// the config has no sla section or when existing events indicate the pipeline
// has already been evaluated.
func (m *SLAModule) Evaluate(ctx context.Context, p EvalParams) error {
	// Short-circuit: return nil if events already exist for this pipeline.
	existing, err := p.EventWriter.ReadEvents(ctx, p.Pipeline, "")
	if err != nil {
		return fmt.Errorf("sla module: read existing events: %w", err)
	}
	if len(existing) > 0 {
		return nil
	}

	// Extract SLA config section.
	slaRaw, ok := p.Config["sla"]
	if !ok {
		return nil // no sla section -- skip
	}
	slaCfg, ok := slaRaw.(map[string]any)
	if !ok {
		return nil // malformed sla section -- skip
	}

	// Read trigger status.
	triggerStatus, err := p.Store.ReadTriggerStatus(ctx, adapter.TriggerKey{
		Pipeline: p.Pipeline,
		Schedule: "default",
		Date:     "default",
	})
	if err != nil {
		return fmt.Errorf("sla module: read trigger status: %w", err)
	}

	now := p.Clock.Now()

	// --- Absolute deadline ---
	if deadlineStr := getConfigString(slaCfg, "deadline"); deadlineStr != "" {
		if err := m.evaluateAbsoluteDeadline(ctx, p, slaCfg, triggerStatus, now, deadlineStr); err != nil {
			return err
		}
	}

	// --- Relative duration ---
	if _, hasMax := slaCfg["max_duration_minutes"]; hasMax {
		if err := m.evaluateRelativeDuration(ctx, p, slaCfg, now); err != nil {
			return err
		}
	}

	return nil
}

// evaluateAbsoluteDeadline handles SLA_MET, SLA_BREACH, and SLA_WARNING for
// absolute deadline configs.
func (m *SLAModule) evaluateAbsoluteDeadline(
	_ context.Context,
	p EvalParams,
	slaCfg map[string]any,
	triggerStatus string,
	now time.Time,
	deadlineStr string,
) error {
	deadline, err := parseDeadline(now, deadlineStr)
	if err != nil {
		return fmt.Errorf("sla module: parse deadline: %w", err)
	}

	warningMinutes := getConfigFloat(slaCfg, "warning_minutes", 30)
	breachFired := getConfigBool(slaCfg, "breach_fired")
	warningStart := deadline.Add(-time.Duration(warningMinutes) * time.Minute)

	// Terminal trigger: emit SLA_MET if before deadline, otherwise suppress.
	if isTerminalStatus(triggerStatus) {
		if now.Before(deadline) {
			p.EventWriter.Emit(InterlockEventRecord{
				PipelineID: p.Pipeline,
				EventType:  "SLA_MET",
				Timestamp:  now,
			})
		}
		// Past deadline + terminal -> suppress all alerts.
		return nil
	}

	// Past deadline -> SLA_BREACH (unless breach_fired suppresses).
	if !now.Before(deadline) {
		if !breachFired {
			p.EventWriter.Emit(InterlockEventRecord{
				PipelineID: p.Pipeline,
				EventType:  "SLA_BREACH",
				Timestamp:  now,
			})
		}
		return nil
	}

	// Within warning window -> SLA_WARNING (unless breach_fired suppresses).
	if !now.Before(warningStart) && now.Before(deadline) {
		if !breachFired {
			p.EventWriter.Emit(InterlockEventRecord{
				PipelineID: p.Pipeline,
				EventType:  "SLA_WARNING",
				Timestamp:  now,
			})
		}
	}

	return nil
}

// evaluateRelativeDuration handles RELATIVE_SLA_WARNING and RELATIVE_SLA_BREACH
// based on job elapsed time versus max_duration_minutes.
func (m *SLAModule) evaluateRelativeDuration(
	ctx context.Context,
	p EvalParams,
	slaCfg map[string]any,
	now time.Time,
) error {
	maxDuration := getConfigFloat(slaCfg, "max_duration_minutes", 0)
	if maxDuration <= 0 {
		return nil
	}
	warningPct := getConfigFloat(slaCfg, "warning_pct", 80)

	// Determine trigger start time from first sensor's metadata.
	triggerStart, err := m.resolveTriggerStartTime(ctx, p)
	if err != nil {
		return err
	}
	if triggerStart.IsZero() {
		return nil // no start time available -- skip
	}

	elapsed := now.Sub(triggerStart)
	maxDur := time.Duration(maxDuration) * time.Minute
	warningDur := time.Duration(float64(maxDur) * warningPct / 100)

	if elapsed > maxDur {
		p.EventWriter.Emit(InterlockEventRecord{
			PipelineID: p.Pipeline,
			EventType:  "RELATIVE_SLA_BREACH",
			Timestamp:  now,
		})
		return nil
	}

	if elapsed > warningDur {
		p.EventWriter.Emit(InterlockEventRecord{
			PipelineID: p.Pipeline,
			EventType:  "RELATIVE_SLA_WARNING",
			Timestamp:  now,
		})
	}

	return nil
}

// resolveTriggerStartTime reads the first sensor from p.SensorKeys and
// inspects its metadata for schedule_time or trigger_time. Falls back to the
// sensor's LastUpdated timestamp.
func (m *SLAModule) resolveTriggerStartTime(ctx context.Context, p EvalParams) (time.Time, error) {
	if len(p.SensorKeys) == 0 {
		return time.Time{}, nil
	}

	sensor, err := p.Store.ReadSensor(ctx, p.Pipeline, p.SensorKeys[0])
	if err != nil {
		return time.Time{}, fmt.Errorf("sla module: read sensor %q: %w", p.SensorKeys[0], err)
	}
	if sensor.Key == "" {
		return time.Time{}, nil
	}

	// Try schedule_time, then trigger_time from metadata.
	for _, field := range []string{"schedule_time", "trigger_time"} {
		if ts, ok := sensor.Metadata[field]; ok && ts != "" {
			parsed, err := time.Parse(time.RFC3339, ts)
			if err != nil {
				continue
			}
			return parsed, nil
		}
	}

	// Fall back to LastUpdated.
	return sensor.LastUpdated, nil
}

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

// parseDeadline combines today's date (from now) with a "HH:MM" deadline
// string to produce a full timestamp in the same location as now.
func parseDeadline(now time.Time, deadline string) (time.Time, error) {
	parts := strings.SplitN(deadline, ":", 2)
	if len(parts) != 2 {
		return time.Time{}, fmt.Errorf("invalid deadline format %q: expected HH:MM", deadline)
	}

	var hour, min int
	if _, err := fmt.Sscanf(parts[0], "%d", &hour); err != nil {
		return time.Time{}, fmt.Errorf("invalid deadline hour %q: %w", parts[0], err)
	}
	if _, err := fmt.Sscanf(parts[1], "%d", &min); err != nil {
		return time.Time{}, fmt.Errorf("invalid deadline minute %q: %w", parts[1], err)
	}

	y, mo, d := now.Date()
	return time.Date(y, mo, d, hour, min, 0, 0, now.Location()), nil
}

// isTerminalStatus returns true for trigger states that indicate the job has
// finished execution (successfully or otherwise).
func isTerminalStatus(status string) bool {
	switch strings.ToLower(status) {
	case "completed", "failed", "succeeded", "killed", "timeout":
		return true
	default:
		return false
	}
}

// getConfigFloat reads a numeric value from a map[string]any, returning
// defaultVal when the key is absent or not convertible.
func getConfigFloat(m map[string]any, key string, defaultVal float64) float64 {
	v, ok := m[key]
	if !ok {
		return defaultVal
	}
	switch n := v.(type) {
	case float64:
		return n
	case float32:
		return float64(n)
	case int:
		return float64(n)
	case int64:
		return float64(n)
	default:
		return defaultVal
	}
}

// getConfigBool reads a bool value from a map[string]any, returning false when
// the key is absent or not a bool.
func getConfigBool(m map[string]any, key string) bool {
	v, ok := m[key]
	if !ok {
		return false
	}
	b, ok := v.(bool)
	if !ok {
		return false
	}
	return b
}

// getConfigString reads a string value from a map[string]any, returning ""
// when the key is absent or not a string.
func getConfigString(m map[string]any, key string) string {
	v, ok := m[key]
	if !ok {
		return ""
	}
	s, ok := v.(string)
	if !ok {
		return ""
	}
	return s
}
