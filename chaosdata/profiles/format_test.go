package profiles

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestFormatJSON_RoundTrip(t *testing.T) {
	records := []map[string]interface{}{
		{"id": 1, "name": "alice"},
		{"id": 2, "name": "bob"},
	}

	data, err := FormatJSON(records)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var parsed []map[string]interface{}
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}

	if len(parsed) != 2 {
		t.Fatalf("expected 2 records, got %d", len(parsed))
	}
	if int(parsed[0]["id"].(float64)) != 1 {
		t.Errorf("expected id 1, got %v", parsed[0]["id"])
	}
}

func TestFormatCSV_CorrectHeadersAndRows(t *testing.T) {
	records := []map[string]interface{}{
		{"id": 1, "name": "alice"},
		{"id": 2, "name": "bob"},
	}
	columns := []string{"id", "name"}

	data, err := FormatCSV(records, columns)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	csvStr := string(data)
	lines := strings.Split(strings.TrimSpace(csvStr), "\n")
	if len(lines) != 3 {
		t.Fatalf("expected 3 lines, got %d", len(lines))
	}
	if lines[0] != "id,name" {
		t.Errorf("expected header 'id,name', got %v", lines[0])
	}
	if lines[1] != "1,alice" {
		t.Errorf("expected row '1,alice', got %v", lines[1])
	}
}

func TestFormatCSV_NilFieldsMappedToEmpty(t *testing.T) {
	records := []map[string]interface{}{
		{"id": 1, "name": nil, "missing": "ignored"},
	}
	columns := []string{"id", "name", "other"}

	data, err := FormatCSV(records, columns)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if lines[1] != "1,," {
		t.Errorf("expected row '1,,', got %v", lines[1])
	}
}

func TestFormatCSV_EscapingCommasAndQuotes(t *testing.T) {
	records := []map[string]interface{}{
		{"id": 1, "name": `Alice "Ally" Smith, Esq.`},
	}
	columns := []string{"id", "name"}

	data, err := FormatCSV(records, columns)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	expected := `1,"Alice ""Ally"" Smith, Esq."`
	if lines[1] != expected {
		t.Errorf("expected row %s, got %s", expected, lines[1])
	}
}

func TestFormat_UnknownFormat_ReturnsError(t *testing.T) {
	_, err := Format("xml", nil, nil)
	if err == nil {
		t.Fatal("expected error for unknown format, got nil")
	}
}

func TestFormat_Dispatch(t *testing.T) {
	records := []map[string]interface{}{{"id": 1}}
	
	jsonData, _ := Format("json", records, []string{"id"})
	if !strings.HasPrefix(string(jsonData), "[") {
		t.Errorf("expected json format, got %s", string(jsonData))
	}

	csvData, _ := Format("csv", records, []string{"id"})
	if !strings.HasPrefix(string(csvData), "id\n1") {
		t.Errorf("expected csv format, got %s", string(csvData))
	}
}
