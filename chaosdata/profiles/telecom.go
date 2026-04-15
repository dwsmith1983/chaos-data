package profiles

import (
	"fmt"
	"time"

	"github.com/dwsmith1983/chaos-data/chaosdata"
)

// TelecomGenerator generates chaos data for telecom scenarios.
type TelecomGenerator struct{}

// Name returns the name of the generator.
func (TelecomGenerator) Name() string {
	return "telecom"
}

// Category returns the category of the generator.
func (TelecomGenerator) Category() string {
	return "profiles"
}

func init() {
	chaosdata.Register(TelecomGenerator{})
}

var telecomColumns = []string{
	"call_id", "caller_id", "callee_id", "caller_imsi", "callee_imsi", 
	"cell_tower_id", "call_type", "start_time", "end_time", "duration_sec", 
	"bytes_transferred", "status",
}

// Generate creates telecom CDR records.
func (g TelecomGenerator) Generate(opts chaosdata.GenerateOpts) (chaosdata.Payload, error) {
	if opts.Count > MaxRecordCount {
		return chaosdata.Payload{}, fmt.Errorf("count %d exceeds max %d", opts.Count, MaxRecordCount)
	}

	cfg, err := ParseConfig(opts.Tags)
	if err != nil {
		return chaosdata.Payload{}, err
	}

	rng := NewSeededRand(cfg.Seed)

	count := opts.Count
	if count <= 0 {
		count = 1
	}

	var allRecords []map[string]interface{}
	baseTime := time.Now().UTC()

	// 1. Telecom stream base generation
	for i := 0; i < count; i++ {
		startTime := baseTime.Add(time.Duration(i) * time.Minute)
		duration := rng.Intn(300) + 10 // 10 to 310 seconds
		endTime := startTime.Add(time.Duration(duration) * time.Second)

		record := map[string]interface{}{
			"call_id":           fmt.Sprintf("call_%d", rng.Intn(1000000)),
			"caller_id":         fmt.Sprintf("+1555%07d", rng.Intn(10000000)),
			"callee_id":         fmt.Sprintf("+1555%07d", rng.Intn(10000000)),
			"caller_imsi":       fmt.Sprintf("3102600000%05d", rng.Intn(100000)),
			"callee_imsi":       fmt.Sprintf("3102600000%05d", rng.Intn(100000)),
			"cell_tower_id":     fmt.Sprintf("tower_%d", rng.Intn(500)),
			"call_type":         rng.Pick([]string{"voice", "sms", "data"}),
			"start_time":        startTime.Format(time.RFC3339),
			"end_time":          endTime.Format(time.RFC3339),
			"duration_sec":      duration,
			"bytes_transferred": rng.Intn(1000000),
			"status":            "completed",
		}

		// Apply dropped calls
		if rng.ShouldInject(cfg.ErrorRate) {
			record["status"] = "dropped"
			record["end_time"] = "" // No end time
		}

		// Apply roaming handoffs
		if rng.ShouldInject(cfg.ErrorRate) {
			record["cell_tower_id"] = fmt.Sprintf("tower_%d_roaming", rng.Intn(50))
		}

		// Apply out-of-order packet delivery (semantic overlap conflict)
		if rng.ShouldInject(cfg.ErrorRate) {
			// Swap start and end time
			record["start_time"] = endTime.Format(time.RFC3339)
			record["end_time"] = startTime.Format(time.RFC3339)
		}

		// Structural Chaos
		if rng.ShouldInject(cfg.SchemaDeviation) {
			delete(record, "callee_imsi")
			record["unknown_telemetry"] = "true"
		}

		// Semantic Chaos
		if rng.ShouldInject(cfg.CorruptionRate) {
			record["duration_sec"] = -10 // Impossible duration
		}

		allRecords = append(allRecords, record)
	}

	// Volume Chaos (BurstSize)
	if cfg.BurstSize > 1 {
		var burstRecords []map[string]interface{}
		for _, r := range allRecords {
			for b := 0; b < cfg.BurstSize; b++ {
				cp := make(map[string]interface{}, len(r))
				for k, v := range r {
					cp[k] = v
				}
				burstRecords = append(burstRecords, cp)
			}
		}
		allRecords = burstRecords
	}

	// Temporal Chaos (OutOfOrderRate)
	if cfg.OutOfOrderRate > 0 {
		var shuffled []map[string]interface{}
		for _, idx := range rng.Shuffle(len(allRecords)) {
			rec := allRecords[idx]
			if tStr, ok := rec["start_time"].(string); ok && cfg.TimestampJitter > 0 {
				if t, err := time.Parse(time.RFC3339, tStr); err == nil {
					rec["start_time"] = rng.Jitter(t, cfg.TimestampJitter).Format(time.RFC3339)
				}
			}
			shuffled = append(shuffled, rec)
		}
		allRecords = shuffled
	}

	// Formatting
	data, err := Format(cfg.OutputFormat, allRecords, telecomColumns)
	if err != nil {
		return chaosdata.Payload{}, err
	}

	mimeType := "application/json"
	if cfg.OutputFormat == "csv" {
		mimeType = "text/csv"
	}

	return chaosdata.Payload{
		Data: data,
		Type: mimeType,
		Attributes: map[string]string{
			"generator": g.Name(),
			"category":  g.Category(),
			"records":   fmt.Sprintf("%d", len(allRecords)),
		},
	}, nil
}
