package format

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
)

// FormatJSON marshals data to JSON.
func FormatJSON(data interface{}) ([]byte, error) {
	return json.Marshal(data)
}

// FormatCSV marshals a slice of maps to CSV with explicit column ordering.
// Nil values are mapped to empty strings.
func FormatCSV(data []map[string]interface{}, columns []string) ([]byte, error) {
	var buf bytes.Buffer
	w := csv.NewWriter(&buf)

	// Write header
	if err := w.Write(columns); err != nil {
		return nil, fmt.Errorf("failed to write CSV header: %w", err)
	}

	// Write rows
	for _, row := range data {
		record := make([]string, len(columns))
		for i, col := range columns {
			val, ok := row[col]
			if !ok || val == nil {
				record[i] = ""
			} else {
				record[i] = fmt.Sprintf("%v", val)
			}
		}
		if err := w.Write(record); err != nil {
			return nil, fmt.Errorf("failed to write CSV record: %w", err)
		}
	}

	w.Flush()
	if err := w.Error(); err != nil {
		return nil, fmt.Errorf("CSV writer error: %w", err)
	}

	return buf.Bytes(), nil
}

// Format dispatches to the appropriate formatter based on the format string.
func Format(formatType string, data interface{}, columns []string) ([]byte, error) {
	switch formatType {
	case "json":
		return FormatJSON(data)
	case "csv":
		maps, ok := data.([]map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("CSV format requires []map[string]interface{} data")
		}
		return FormatCSV(maps, columns)
	default:
		return nil, fmt.Errorf("unknown format: %s", formatType)
	}
}
