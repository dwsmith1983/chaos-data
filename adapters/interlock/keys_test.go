package interlock_test

import (
	"testing"

	"github.com/dwsmith1983/chaos-data/adapters/interlock"
)

func TestSensorPK(t *testing.T) {
	t.Parallel()

	got := interlock.SensorPK("my-pipeline")
	want := "SENSOR#my-pipeline"

	if got != want {
		t.Errorf("SensorPK(%q) = %q, want %q", "my-pipeline", got, want)
	}
}

func TestSensorSK(t *testing.T) {
	t.Parallel()

	got := interlock.SensorSK("landing/orders")
	want := "KEY#landing/orders"

	if got != want {
		t.Errorf("SensorSK(%q) = %q, want %q", "landing/orders", got, want)
	}
}

func TestTriggerPK(t *testing.T) {
	t.Parallel()

	got := interlock.TriggerPK("etl-daily")
	want := "TRIGGER#etl-daily"

	if got != want {
		t.Errorf("TriggerPK(%q) = %q, want %q", "etl-daily", got, want)
	}
}

func TestTriggerSK(t *testing.T) {
	t.Parallel()

	got := interlock.TriggerSK("daily", "2026-03-14")
	want := "SCHED#daily#DATE#2026-03-14"

	if got != want {
		t.Errorf("TriggerSK(%q, %q) = %q, want %q", "daily", "2026-03-14", got, want)
	}
}
