// Package safety provides functions to enforce safety boundaries
// for chaos experiments, including severity checks, kill switches,
// blast radius limits, SLA windows, and time boxing.
package safety

import (
	"fmt"

	"github.com/dwsmith1983/chaos-data/pkg/scenario"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// CheckSeverity returns an error if the scenario's severity exceeds the maximum allowed.
func CheckSeverity(s scenario.Scenario, maxAllowed types.Severity) error {
	if s.Severity.ExceedsThreshold(maxAllowed) {
		return fmt.Errorf(
			"scenario %q severity %s exceeds maximum allowed %s",
			s.Name, s.Severity, maxAllowed,
		)
	}
	return nil
}
