package aws_test

import (
	"testing"
	"time"

	chaosaws "github.com/dwsmith1983/chaos-data/adapters/aws"
)

func TestSensorPK(t *testing.T) {
	t.Parallel()

	got := chaosaws.SensorPK("my-pipeline")
	want := "SENSOR#my-pipeline"

	if got != want {
		t.Errorf("SensorPK(%q) = %q, want %q", "my-pipeline", got, want)
	}
}

func TestSensorSK(t *testing.T) {
	t.Parallel()

	got := chaosaws.SensorSK("landing/orders")
	want := "KEY#landing/orders"

	if got != want {
		t.Errorf("SensorSK(%q) = %q, want %q", "landing/orders", got, want)
	}
}

func TestTriggerPK(t *testing.T) {
	t.Parallel()

	got := chaosaws.TriggerPK("etl-daily")
	want := "TRIGGER#etl-daily"

	if got != want {
		t.Errorf("TriggerPK(%q) = %q, want %q", "etl-daily", got, want)
	}
}

func TestTriggerSK(t *testing.T) {
	t.Parallel()

	got := chaosaws.TriggerSK("daily", "2026-03-14")
	want := "SCHED#daily#DATE#2026-03-14"

	if got != want {
		t.Errorf("TriggerSK(%q, %q) = %q, want %q", "daily", "2026-03-14", got, want)
	}
}

func TestChaosPK(t *testing.T) {
	t.Parallel()

	got := chaosaws.ChaosPK("exp-001")
	want := "CHAOS#exp-001"

	if got != want {
		t.Errorf("ChaosPK(%q) = %q, want %q", "exp-001", got, want)
	}
}

func TestChaosSK(t *testing.T) {
	t.Parallel()

	ts := time.Date(2026, 3, 14, 10, 30, 0, 123456789, time.UTC)
	got := chaosaws.ChaosSK(ts, "evt-abc")
	want := "EVENT#" + ts.Format(time.RFC3339Nano) + "#evt-abc"

	if got != want {
		t.Errorf("ChaosSK(%v, %q) = %q, want %q", ts, "evt-abc", got, want)
	}
}

func TestControlPK(t *testing.T) {
	t.Parallel()

	got := chaosaws.ControlPK("kill-switch")
	want := "CONTROL#kill-switch"

	if got != want {
		t.Errorf("ControlPK(%q) = %q, want %q", "kill-switch", got, want)
	}
}

func TestDepsPK(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		pipeline string
		want     string
	}{
		{name: "simple pipeline", pipeline: "bronze-cdr", want: "DEPS#bronze-cdr"},
		{name: "nested name", pipeline: "ml/training/v2", want: "DEPS#ml/training/v2"},
		{name: "empty string", pipeline: "", want: "DEPS#"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := chaosaws.DepsPK(tt.pipeline); got != tt.want {
				t.Errorf("DepsPK(%q) = %q, want %q", tt.pipeline, got, tt.want)
			}
		})
	}
}

func TestDownstreamSK(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		downstream string
		want       string
	}{
		{name: "simple downstream", downstream: "silver-cdr-hour", want: "DOWNSTREAM#silver-cdr-hour"},
		{name: "nested name", downstream: "reporting/daily", want: "DOWNSTREAM#reporting/daily"},
		{name: "empty string", downstream: "", want: "DOWNSTREAM#"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := chaosaws.DownstreamSK(tt.downstream); got != tt.want {
				t.Errorf("DownstreamSK(%q) = %q, want %q", tt.downstream, got, tt.want)
			}
		})
	}
}

func TestDownstreamSKPrefix(t *testing.T) {
	t.Parallel()

	got := chaosaws.DownstreamSKPrefix()
	want := "DOWNSTREAM#"

	if got != want {
		t.Errorf("DownstreamSKPrefix() = %q, want %q", got, want)
	}
}
