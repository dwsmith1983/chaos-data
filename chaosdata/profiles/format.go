package profiles

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
)

// FormatJSON formats records as JSON.
func FormatJSON(records []map[string]interface{}) ([]byte, error) {
	return json.Marshal(records)
}

// FormatCSV formats records as CSV.
func FormatCSV(records []map[string]interface{}, columns []string) ([]byte, error) {
	var buf bytes.Buffer
	writer := csv.NewWriter(&buf)

	if err := writer.Write(columns); err != nil {
		return nil, err
	}

	for _, record := range records {
		row := make([]string, len(columns))
		for i, col := range columns {
			val, ok := record[col]
			if !ok || val == nil {
				row[i] = ""
			} else {
				row[i] = fmt.Sprintf("%v", val)
			}
		}
		if err := writer.Write(row); err != nil {
			return nil, err
		}
	}

	writer.Flush()
	if err := writer.Error(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// Format dispatches to the correct formatter based on the format string.
func Format(format string, records []map[string]interface{}, columns []string) ([]byte, error) {
	switch format {
	case "json":
		return FormatJSON(records)
	case "csv":
		return FormatCSV(records, columns)
	default:
		return nil, fmt.Errorf("unknown format: %s", format)
	}
}
