package adapter

import (
	"sync"
	"time"
)

// Clock abstracts time for testability.
type Clock interface {
	// Now returns the current time.
	Now() time.Time

	// NewTicker returns a channel that delivers the current time at regular intervals.
	NewTicker(d time.Duration) Ticker

	// After waits for the duration to elapse and then sends the current time on the returned channel.
	After(d time.Duration) <-chan time.Time
}

// Ticker abstracts time.Ticker for testability.
type Ticker interface {
	C() <-chan time.Time
	Stop()
}

// SystemTicker wraps a time.Ticker to implement adapter.Ticker.
type SystemTicker struct {
	ticker *time.Ticker
}

func (s *SystemTicker) C() <-chan time.Time {
	return s.ticker.C
}

func (s *SystemTicker) Stop() {
	s.ticker.Stop()
}

// WallClock implements Clock using the real system clock.
type WallClock struct{}

// NewWallClock returns a WallClock instance.
func NewWallClock() *WallClock {
	return &WallClock{}
}

func (c *WallClock) Now() time.Time {
	return time.Now()
}

func (c *WallClock) NewTicker(d time.Duration) Ticker {
	return &SystemTicker{ticker: time.NewTicker(d)}
}

func (c *WallClock) After(d time.Duration) <-chan time.Time {
	return time.After(d)
}

// TestClock implements Clock for testing, allowing time to be artificially advanced.
type TestClock struct {
	mu      sync.Mutex
	now     time.Time
	tickers []*TestTicker
}

func NewTestClock(start time.Time) *TestClock {
	return &TestClock{now: start}
}

func (c *TestClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// Advance instantly moves the clock forward by the given duration and
// sends a tick to every active ticker registered with this clock.
func (c *TestClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
	for _, t := range c.tickers {
		select {
		case t.c <- c.now:
		default:
			// Channel full — drop tick to avoid blocking.
		}
	}
}

func (c *TestClock) NewTicker(d time.Duration) Ticker {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := &TestTicker{
		c:     make(chan time.Time, 1),
		stop:  make(chan struct{}),
		clock: c,
	}
	// Instantly tick once for immediate evaluation in loops.
	t.c <- c.now
	c.tickers = append(c.tickers, t)
	return t
}

func (c *TestClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	ch := make(chan time.Time, 1)
	ch <- c.now.Add(d)
	return ch
}

// removeTicker removes the given ticker from the clock's active list.
func (c *TestClock) removeTicker(t *TestTicker) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, tt := range c.tickers {
		if tt == t {
			c.tickers = append(c.tickers[:i], c.tickers[i+1:]...)
			return
		}
	}
}

// TestTicker is a stub ticker for TestClock.
type TestTicker struct {
	c     chan time.Time
	stop  chan struct{}
	clock *TestClock
}

func (t *TestTicker) C() <-chan time.Time {
	return t.c
}

func (t *TestTicker) Stop() {
	t.clock.removeTicker(t)
	close(t.stop)
}
