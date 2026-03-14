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
