package profiles

import (
	"testing"
	"time"
)

func TestDefaultChaosConfig_ReturnsValidConfig(t *testing.T) {
	cfg := DefaultChaosConfig().Validate()
	if cfg.ErrorRate != 0.1 {
		t.Errorf("expected ErrorRate 0.1, got %v", cfg.ErrorRate)
	}
	if cfg.Seed != 0 {
		t.Errorf("expected Seed 0, got %v", cfg.Seed)
	}
	if cfg.OutputFormat != "json" {
		t.Errorf("expected OutputFormat json, got %v", cfg.OutputFormat)
	}
}

func TestValidate_ClampsRates(t *testing.T) {
	cfg := ChaosConfig{
		SchemaDeviation: -0.5,
		DuplicateRate:   1.5,
		OutOfOrderRate:  2.0,
		ErrorRate:       -1.0,
		CorruptionRate:  1.0,
	}.Validate()

	if cfg.SchemaDeviation != 0.0 {
		t.Errorf("expected SchemaDeviation 0.0, got %v", cfg.SchemaDeviation)
	}
	if cfg.DuplicateRate != 1.0 {
		t.Errorf("expected DuplicateRate 1.0, got %v", cfg.DuplicateRate)
	}
	if cfg.OutOfOrderRate != 1.0 {
		t.Errorf("expected OutOfOrderRate 1.0, got %v", cfg.OutOfOrderRate)
	}
	if cfg.ErrorRate != 0.0 {
		t.Errorf("expected ErrorRate 0.0, got %v", cfg.ErrorRate)
	}
	if cfg.CorruptionRate != 1.0 {
		t.Errorf("expected CorruptionRate 1.0, got %v", cfg.CorruptionRate)
	}
}

func TestValidate_ClampsDurations(t *testing.T) {
	cfg := ChaosConfig{
		TimestampJitter: -1 * time.Second,
		ReplayDelay:     -5 * time.Minute,
		BurstInterval:   -10 * time.Millisecond,
	}.Validate()

	if cfg.TimestampJitter != 0 {
		t.Errorf("expected TimestampJitter 0, got %v", cfg.TimestampJitter)
	}
	if cfg.ReplayDelay != 0 {
		t.Errorf("expected ReplayDelay 0, got %v", cfg.ReplayDelay)
	}
	if cfg.BurstInterval != 0 {
		t.Errorf("expected BurstInterval 0, got %v", cfg.BurstInterval)
	}
}

func TestValidate_DefaultsOutputFormat(t *testing.T) {
	cfg := ChaosConfig{}.Validate()
	if cfg.OutputFormat != "json" {
		t.Errorf("expected OutputFormat json, got %v", cfg.OutputFormat)
	}
}

func TestParseConfig_ValidJSON(t *testing.T) {
	tags := map[string]string{
		"chaos_config": `{"error_rate": 0.5, "output_format": "csv", "seed": 42}`,
	}
	cfg, err := ParseConfig(tags)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.ErrorRate != 0.5 {
		t.Errorf("expected ErrorRate 0.5, got %v", cfg.ErrorRate)
	}
	if cfg.OutputFormat != "csv" {
		t.Errorf("expected OutputFormat csv, got %v", cfg.OutputFormat)
	}
	if cfg.Seed != 42 {
		t.Errorf("expected Seed 42, got %v", cfg.Seed)
	}
}

func TestParseConfig_MissingKey(t *testing.T) {
	tags := map[string]string{}
	cfg, err := ParseConfig(tags)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.ErrorRate != 0.1 {
		t.Errorf("expected ErrorRate 0.1, got %v", cfg.ErrorRate)
	}
	if cfg.OutputFormat != "json" {
		t.Errorf("expected OutputFormat json, got %v", cfg.OutputFormat)
	}
}

func TestParseConfig_InvalidJSON(t *testing.T) {
	tags := map[string]string{
		"chaos_config": `{"error_rate": "invalid"}`,
	}
	_, err := ParseConfig(tags)
	if err == nil {
		t.Fatal("expected error parsing invalid json, got nil")
	}
}
