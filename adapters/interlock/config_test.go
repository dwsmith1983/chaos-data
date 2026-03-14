package interlock_test

import (
	"testing"

	"github.com/dwsmith1983/chaos-data/adapters/interlock"
)

func TestConfig_Validate_MissingSensorTableName(t *testing.T) {
	t.Parallel()

	cfg := interlock.Config{
		TriggerTableName: "trigger-table",
	}
	cfg.Defaults()

	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() = nil, want error for missing SensorTableName")
	}
}

func TestConfig_Validate_MissingTriggerTableName(t *testing.T) {
	t.Parallel()

	cfg := interlock.Config{
		SensorTableName: "sensor-table",
	}
	cfg.Defaults()

	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() = nil, want error for missing TriggerTableName")
	}
}

func TestConfig_Validate_Happy(t *testing.T) {
	t.Parallel()

	cfg := interlock.Config{
		SensorTableName:  "sensor-table",
		TriggerTableName: "trigger-table",
	}
	cfg.Defaults()

	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() = %v, want nil", err)
	}
}

func TestConfig_Defaults(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		input     interlock.Config
		wantField string
		wantValue string
		wantInt   int
	}{
		{
			name:      "event bus defaults to interlock",
			input:     interlock.Config{},
			wantField: "EventBusName",
			wantValue: "interlock",
		},
		{
			name:      "event bus not overwritten when set",
			input:     interlock.Config{EventBusName: "custom-bus"},
			wantField: "EventBusName",
			wantValue: "custom-bus",
		},
		{
			name:      "pipeline prefix defaults to empty",
			input:     interlock.Config{},
			wantField: "PipelinePrefix",
			wantValue: "",
		},
		{
			name:      "pipeline prefix not overwritten when set",
			input:     interlock.Config{PipelinePrefix: "prod/"},
			wantField: "PipelinePrefix",
			wantValue: "prod/",
		},
		{
			name:      "default schedule defaults to empty",
			input:     interlock.Config{},
			wantField: "DefaultSchedule",
			wantValue: "",
		},
		{
			name:      "default schedule not overwritten when set",
			input:     interlock.Config{DefaultSchedule: "daily"},
			wantField: "DefaultSchedule",
			wantValue: "daily",
		},
		{
			name:      "SLA window defaults to 30",
			input:     interlock.Config{},
			wantField: "SLAWindowMinutes",
			wantInt:   30,
		},
		{
			name:      "SLA window not overwritten when set",
			input:     interlock.Config{SLAWindowMinutes: 60},
			wantField: "SLAWindowMinutes",
			wantInt:   60,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := tt.input
			cfg.Defaults()

			switch tt.wantField {
			case "EventBusName":
				if cfg.EventBusName != tt.wantValue {
					t.Errorf("EventBusName = %q, want %q", cfg.EventBusName, tt.wantValue)
				}
			case "PipelinePrefix":
				if cfg.PipelinePrefix != tt.wantValue {
					t.Errorf("PipelinePrefix = %q, want %q", cfg.PipelinePrefix, tt.wantValue)
				}
			case "DefaultSchedule":
				if cfg.DefaultSchedule != tt.wantValue {
					t.Errorf("DefaultSchedule = %q, want %q", cfg.DefaultSchedule, tt.wantValue)
				}
			case "SLAWindowMinutes":
				if cfg.SLAWindowMinutes != tt.wantInt {
					t.Errorf("SLAWindowMinutes = %d, want %d", cfg.SLAWindowMinutes, tt.wantInt)
				}
			default:
				t.Fatalf("unknown field %q", tt.wantField)
			}
		})
	}
}
