package airflow_test

import (
	"testing"
	"time"

	"github.com/dwsmith1983/chaos-data/adapters/airflow"
)

func TestSensorKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		pipeline string
		key      string
		want     string
	}{
		{
			name:     "simple",
			pipeline: "etl-daily",
			key:      "landing/orders",
			want:     "chaos:sensor:etl-daily:landing/orders",
		},
		{
			name:     "nested path",
			pipeline: "ingest",
			key:      "raw/events/2026/03",
			want:     "chaos:sensor:ingest:raw/events/2026/03",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := airflow.SensorKey(tt.pipeline, tt.key)
			if got != tt.want {
				t.Errorf("SensorKey(%q, %q) = %q, want %q", tt.pipeline, tt.key, got, tt.want)
			}
		})
	}
}

func TestTriggerStatusKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		pipeline string
		schedule string
		date     string
		want     string
	}{
		{
			name:     "daily schedule",
			pipeline: "etl-daily",
			schedule: "daily",
			date:     "2026-03-14",
			want:     "chaos:trigger:etl-daily:daily:2026-03-14",
		},
		{
			name:     "hourly schedule",
			pipeline: "streaming",
			schedule: "hourly",
			date:     "2026-03-29",
			want:     "chaos:trigger:streaming:hourly:2026-03-29",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := airflow.TriggerStatusKey(tt.pipeline, tt.schedule, tt.date)
			if got != tt.want {
				t.Errorf("TriggerStatusKey(%q, %q, %q) = %q, want %q",
					tt.pipeline, tt.schedule, tt.date, got, tt.want)
			}
		})
	}
}

func TestEventKey(t *testing.T) {
	t.Parallel()

	ts := time.Date(2026, 3, 14, 10, 30, 0, 123456789, time.UTC)
	got := airflow.EventKey("exp-001", ts, "evt-abc")
	want := "chaos:event:exp-001:" + ts.Format(time.RFC3339Nano) + ":evt-abc"

	if got != want {
		t.Errorf("EventKey() = %q, want %q", got, want)
	}
}

func TestEventKeyPrefix(t *testing.T) {
	t.Parallel()

	got := airflow.EventKeyPrefix("exp-001")
	want := "chaos:event:exp-001:"

	if got != want {
		t.Errorf("EventKeyPrefix(%q) = %q, want %q", "exp-001", got, want)
	}
}

func TestConfigKey(t *testing.T) {
	t.Parallel()

	got := airflow.ConfigKey("etl-daily")
	want := "chaos:config:etl-daily"

	if got != want {
		t.Errorf("ConfigKey(%q) = %q, want %q", "etl-daily", got, want)
	}
}

func TestRerunKey(t *testing.T) {
	t.Parallel()

	ts := time.Date(2026, 3, 14, 12, 0, 0, 0, time.UTC)
	got := airflow.RerunKey("etl-daily", "daily", "2026-03-14", ts)
	want := "chaos:rerun:etl-daily:daily:2026-03-14:" + ts.Format(time.RFC3339Nano)

	if got != want {
		t.Errorf("RerunKey() = %q, want %q", got, want)
	}
}

func TestRerunKeyPrefix(t *testing.T) {
	t.Parallel()

	got := airflow.RerunKeyPrefix("etl-daily", "daily", "2026-03-14")
	want := "chaos:rerun:etl-daily:daily:2026-03-14:"

	if got != want {
		t.Errorf("RerunKeyPrefix() = %q, want %q", got, want)
	}
}

func TestJobEventKey(t *testing.T) {
	t.Parallel()

	ts := time.Date(2026, 3, 14, 12, 0, 0, 0, time.UTC)
	got := airflow.JobEventKey("etl-daily", "daily", "2026-03-14", ts, "run-123")
	want := "chaos:job:etl-daily:daily:2026-03-14:" + ts.Format(time.RFC3339Nano) + ":run-123"

	if got != want {
		t.Errorf("JobEventKey() = %q, want %q", got, want)
	}
}

func TestJobEventKeyPrefix(t *testing.T) {
	t.Parallel()

	got := airflow.JobEventKeyPrefix("etl-daily", "daily", "2026-03-14")
	want := "chaos:job:etl-daily:daily:2026-03-14:"

	if got != want {
		t.Errorf("JobEventKeyPrefix() = %q, want %q", got, want)
	}
}

func TestHasPrefix(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		key    string
		prefix string
		want   bool
	}{
		{
			name:   "matching prefix",
			key:    "chaos:sensor:etl:landing",
			prefix: "chaos:sensor:etl:",
			want:   true,
		},
		{
			name:   "non-matching prefix",
			key:    "chaos:sensor:etl:landing",
			prefix: "chaos:trigger:",
			want:   false,
		},
		{
			name:   "exact match",
			key:    "chaos:config:etl",
			prefix: "chaos:config:etl",
			want:   true,
		},
		{
			name:   "empty prefix",
			key:    "chaos:sensor:etl:landing",
			prefix: "",
			want:   true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := airflow.HasPrefix(tt.key, tt.prefix)
			if got != tt.want {
				t.Errorf("HasPrefix(%q, %q) = %v, want %v", tt.key, tt.prefix, got, tt.want)
			}
		})
	}
}
