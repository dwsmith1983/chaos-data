package interlocksuite

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
	"time"
)

// FormatTable writes a terminal-friendly table of the coverage matrix to w.
func FormatTable(matrix CoverageMatrix, w io.Writer) error {
	if _, err := fmt.Fprintln(w, "INTERLOCK CHAOS SUITE — Coverage Matrix"); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}
	if _, err := fmt.Fprintln(w); err != nil {
		return fmt.Errorf("failed to write newline: %w", err)
	}

	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)

	// tabwriter buffers all writes until Flush — only the Flush error matters.
	fmt.Fprintln(tw, "Category\tCapability\tScenarios\tPass\tFail\tDuration\tStatus")
	fmt.Fprintln(tw, "────────\t──────────\t─────────\t────\t────\t────────\t──────")

	for _, r := range matrix.Results {
		scenarios := r.Passed + r.Failed
		pass := fmt.Sprintf("%d", r.Passed)
		fail := fmt.Sprintf("%d", r.Failed)
		dur := formatDuration(r.Duration)
		status := formatStatus(r.Status)

		if r.Status == StatusUntested {
			pass = "-"
			fail = "-"
			dur = "-"
		}

		fmt.Fprintf(tw, "%s\t%s\t%d\t%s\t%s\t%s\t%s\n",
			r.Category, r.Capability.ID, scenarios, pass, fail, dur, status,
		)
	}

	if err := tw.Flush(); err != nil {
		return fmt.Errorf("failed to flush table: %w", err)
	}

	if _, err := fmt.Fprintln(w); err != nil {
		return fmt.Errorf("failed to write newline: %w", err)
	}
	if _, err := fmt.Fprintf(w, "Summary: %d/%d COVERED | %d GAPS | %d UNTESTED\n",
		matrix.Covered, matrix.Total, matrix.Gaps, matrix.Untested,
	); err != nil {
		return fmt.Errorf("failed to write summary: %w", err)
	}

	return nil
}

// FormatJSON writes the coverage matrix as JSON to w.
func FormatJSON(matrix CoverageMatrix, w io.Writer) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	if err := enc.Encode(matrix); err != nil {
		return fmt.Errorf("failed to encode coverage matrix as JSON: %w", err)
	}
	return nil
}

// FormatMarkdown writes the coverage matrix as a Markdown table to w.
func FormatMarkdown(matrix CoverageMatrix, w io.Writer) error {
	if _, err := fmt.Fprintln(w, "# Interlock Coverage Matrix"); err != nil {
		return fmt.Errorf("failed to write markdown title: %w", err)
	}
	if _, err := fmt.Fprintln(w); err != nil {
		return fmt.Errorf("failed to write newline: %w", err)
	}
	if _, err := fmt.Fprintln(w, "| Category | Capability | Scenarios | Pass | Fail | Duration | Status |"); err != nil {
		return fmt.Errorf("failed to write markdown header: %w", err)
	}
	if _, err := fmt.Fprintln(w, "| --- | --- | --- | --- | --- | --- | --- |"); err != nil {
		return fmt.Errorf("failed to write markdown separator: %w", err)
	}

	for _, r := range matrix.Results {
		scenarios := r.Passed + r.Failed
		pass := fmt.Sprintf("%d", r.Passed)
		fail := fmt.Sprintf("%d", r.Failed)
		dur := formatDuration(r.Duration)
		status := string(r.Status)

		if r.Status == StatusUntested {
			pass = "-"
			fail = "-"
			dur = "-"
		}

		if _, err := fmt.Fprintf(w, "| %s | %s | %d | %s | %s | %s | %s |\n",
			escapeMD(r.Category), escapeMD(r.Capability.ID), scenarios, pass, fail, dur, status,
		); err != nil {
			return fmt.Errorf("failed to write markdown row: %w", err)
		}
	}

	if _, err := fmt.Fprintln(w); err != nil {
		return fmt.Errorf("failed to write newline: %w", err)
	}
	if _, err := fmt.Fprintf(w, "**Summary:** %d/%d COVERED | %d GAPS | %d UNTESTED\n",
		matrix.Covered, matrix.Total, matrix.Gaps, matrix.Untested,
	); err != nil {
		return fmt.Errorf("failed to write markdown summary: %w", err)
	}

	return nil
}

// formatDuration renders a duration as seconds with one decimal place.
func formatDuration(d time.Duration) string {
	return fmt.Sprintf("%.1fs", d.Seconds())
}

// escapeMD escapes pipe characters that would break Markdown table formatting.
func escapeMD(s string) string {
	return strings.ReplaceAll(s, "|", "\\|")
}

// formatStatus returns a display string for a capability status.
func formatStatus(s CapabilityStatus) string {
	switch s {
	case StatusGap:
		return "⚠ GAP"
	case StatusUntested:
		return "UNTESTED"
	default:
		return string(s)
	}
}
