package profiles

import (
	"fmt"
	"time"

	"github.com/dwsmith1983/chaos-data/chaosdata"
)

// IoTGenerator generates chaos data for IoT scenarios.
type IoTGenerator struct{}

// Name returns the name of the generator.
func (IoTGenerator) Name() string {
	return "iot"
}

// Category returns the category of the generator.
func (IoTGenerator) Category() string {
	return "profiles"
}

func init() {
	chaosdata.Register(IoTGenerator{})
}

var iotColumns = []string{
	"device_id", "timestamp", "temperature", "humidity", "battery_level", 
	"firmware_version", "gps_lat", "gps_lon", "status",
}

// Generate creates IoT telemetry records.
func (g IoTGenerator) Generate(opts chaosdata.GenerateOpts) (chaosdata.Payload, error) {
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
	deviceFailureSet := make(map[string]bool)

	// 1. IoT stream base generation
	for i := 0; i < count; i++ {
		deviceID := fmt.Sprintf("device_%d", rng.Intn(100))
		
		// If device failed previously, skip generating data for it (device failure mid-stream)
		if deviceFailureSet[deviceID] {
			// Skip actual reading, simulate missing data
			continue
		}

		record := map[string]interface{}{
			"device_id":        deviceID,
			"timestamp":        baseTime.Add(time.Duration(i) * time.Second).Format(time.RFC3339),
			"temperature":      22.5 + rng.Float64()*5.0,
			"humidity":         45.0 + rng.Float64()*10.0,
			"battery_level":    100.0 - float64(i)*0.01, // Gradual drain
			"firmware_version": "v1.2.3",
			"gps_lat":          37.7749 + (rng.Float64()-0.5)*0.01,
			"gps_lon":          -122.4194 + (rng.Float64()-0.5)*0.01,
			"status":           "active",
		}

		// Apply battery drain simulation (accelerated drain)
		if rng.ShouldInject(cfg.ErrorRate) {
			record["battery_level"] = rng.Float64() * 10.0 // Sudden drop to < 10%
		}

		// Apply device failure mid-stream
		if rng.ShouldInject(cfg.ErrorRate) {
			deviceFailureSet[deviceID] = true
			record["status"] = "offline"
		}

		// Apply GPS jitter
		if rng.ShouldInject(cfg.ErrorRate) {
			record["gps_lat"] = 0.0 // Jump to equator/prime meridian
			record["gps_lon"] = 0.0
		}

		// Apply firmware corruption
		if rng.ShouldInject(cfg.CorruptionRate) {
			record["firmware_version"] = "\x00\xFFcorrupted\x00"
		}

		// Structural Chaos
		if rng.ShouldInject(cfg.SchemaDeviation) {
			delete(record, "humidity")
			record["unknown_sensor"] = rng.Float64()
		}

		// Duplicate Device IDs
		if rng.ShouldInject(cfg.DuplicateRate) {
			allRecords = append(allRecords, record) // Append a duplicate
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
			if tStr, ok := rec["timestamp"].(string); ok && cfg.TimestampJitter > 0 {
				if t, err := time.Parse(time.RFC3339, tStr); err == nil {
					rec["timestamp"] = rng.Jitter(t, cfg.TimestampJitter).Format(time.RFC3339)
				}
			}
			shuffled = append(shuffled, rec)
		}
		allRecords = shuffled
	}

	// Formatting
	data, err := Format(cfg.OutputFormat, allRecords, iotColumns)
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
