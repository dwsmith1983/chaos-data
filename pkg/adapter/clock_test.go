package adapter_test

import (
	"testing"
	"time"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
)

func TestTestClock_TickerAdvance(t *testing.T) {
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	clk := adapter.NewTestClock(start)

	ticker := clk.NewTicker(1 * time.Second)
	defer ticker.Stop()

	// First tick is pre-loaded by NewTicker.
	select {
	case got := <-ticker.C():
		if !got.Equal(start) {
			t.Fatalf("initial tick: want %v, got %v", start, got)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("initial tick not received within timeout")
	}

	// Advance the clock — this should deliver a second tick.
	clk.Advance(5 * time.Second)
	expected := start.Add(5 * time.Second)

	select {
	case got := <-ticker.C():
		if !got.Equal(expected) {
			t.Fatalf("tick after Advance: want %v, got %v", expected, got)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("tick after Advance not received within timeout")
	}

	// Advance again to confirm repeated ticking works.
	clk.Advance(3 * time.Second)
	expected2 := expected.Add(3 * time.Second)

	select {
	case got := <-ticker.C():
		if !got.Equal(expected2) {
			t.Fatalf("second tick after Advance: want %v, got %v", expected2, got)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("second tick after Advance not received within timeout")
	}
}

func TestTestClock_TickerStopUnregisters(t *testing.T) {
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	clk := adapter.NewTestClock(start)

	ticker := clk.NewTicker(1 * time.Second)

	// Drain the initial tick.
	<-ticker.C()

	// Stop the ticker, then advance — the channel should NOT receive.
	ticker.Stop()
	clk.Advance(5 * time.Second)

	select {
	case <-ticker.C():
		t.Fatal("stopped ticker should not receive ticks after Advance")
	case <-time.After(50 * time.Millisecond):
		// Expected: no tick received.
	}
}

func TestTestClock_MultipleTickers(t *testing.T) {
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	clk := adapter.NewTestClock(start)

	t1 := clk.NewTicker(1 * time.Second)
	t2 := clk.NewTicker(1 * time.Second)
	defer t1.Stop()
	defer t2.Stop()

	// Drain initial ticks.
	<-t1.C()
	<-t2.C()

	// Advance — both tickers should fire.
	clk.Advance(1 * time.Second)
	expected := start.Add(1 * time.Second)

	select {
	case got := <-t1.C():
		if !got.Equal(expected) {
			t.Fatalf("ticker1: want %v, got %v", expected, got)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("ticker1 did not receive tick after Advance")
	}

	select {
	case got := <-t2.C():
		if !got.Equal(expected) {
			t.Fatalf("ticker2: want %v, got %v", expected, got)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("ticker2 did not receive tick after Advance")
	}
}
