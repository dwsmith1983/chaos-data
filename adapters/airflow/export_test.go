package airflow

import "io"

// SetWarnWriter replaces the warning destination for testing.
// It returns a function that restores the original writer.
func SetWarnWriter(w io.Writer) func() {
	old := warnWriter
	warnWriter = w
	return func() { warnWriter = old }
}
