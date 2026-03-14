package safety

import (
	"fmt"
	"time"
)

// CheckTimeBox returns an error if the experiment has exceeded its allowed duration.
// A zero duration is rejected as invalid.
func CheckTimeBox(startTime time.Time, maxDuration time.Duration) error {
	if maxDuration <= 0 {
		return fmt.Errorf("time box: invalid max duration %v (must be positive)", maxDuration)
	}
	elapsed := time.Since(startTime)
	if elapsed > maxDuration {
		return fmt.Errorf(
			"time box: experiment exceeded max duration %v (elapsed %v)",
			maxDuration, elapsed.Truncate(time.Second),
		)
	}
	return nil
}
