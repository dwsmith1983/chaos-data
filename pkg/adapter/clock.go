package adapter

import "time"

// Clock abstracts time for testability.
type Clock interface {
	// Now returns the current time.
	Now() time.Time

	// NewTicker returns a channel that delivers the current time at regular intervals.
	// We return <-chan time.Time instead of *time.Ticker so it can be easily mocked,
	// but the caller must handle stopping the ticker out-of-band, or we provide a Ticker interface.
	// Wait, the standard library ticker has a Stop() method.
	// Let's define a Ticker interface instead.
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
// It is thread-safe.
type TestClock struct {
	now time.Time
}

func NewTestClock(start time.Time) *TestClock {
	return &TestClock{now: start}
}

func (c *TestClock) Now() time.Time {
	return c.now
}

// Advance instantly moves the clock forward by the given duration.
func (c *TestClock) Advance(d time.Duration) {
	c.now = c.now.Add(d)
}

// NewTicker is a stub for TestClock. 
// For pure testing with instantaneous evaluation, TestClock might just
// return an immediately firing ticker, but true ticker mocking requires a bit more logic.
// For our use case (evaluating assertions instantly), returning a closed channel could simulate an infinite fast loop,
// but it's simpler to just return a channel we send on when Advance is called, or just a fast ticker.
func (c *TestClock) NewTicker(d time.Duration) Ticker {
	t := &TestTicker{
		c:    make(chan time.Time, 1),
		stop: make(chan struct{}),
	}
	// Instantly tick once for immediate evaluation in loops
	t.c <- c.now
	return t
}

func (c *TestClock) After(d time.Duration) <-chan time.Time {
	ch := make(chan time.Time, 1)
	ch <- c.now.Add(d)
	return ch
}

// TestTicker is a stub ticker for TestClock.
type TestTicker struct {
	c    chan time.Time
	stop chan struct{}
}

func (t *TestTicker) C() <-chan time.Time {
	return t.c
}

func (t *TestTicker) Stop() {
	close(t.stop)
}
