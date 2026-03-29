package mutation_test

import (
	"context"
	"testing"

	"github.com/dwsmith1983/chaos-data/pkg/mutation"
	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

func TestSchemaDriftMutation_Type(t *testing.T) {
	s := &mutation.SchemaDriftMutation{}
	if got := s.Type(); got != "schema-drift" {
		t.Errorf("Type() = %q, want %q", got, "schema-drift")
	}
}

func TestSchemaDriftMutation_AddColumns(t *testing.T) {
	inputData := makeJSONL([]map[string]interface{}{
		{"id": float64(1), "name": "Alice"},
		{"id": float64(2), "name": "Bob"},
	})

	transport := newMockTransport()
	obj := types.DataObject{Key: "data/records.jsonl"}
	transport.ReadData[obj.Key] = inputData

	s := &mutation.SchemaDriftMutation{}
	record, err := s.Apply(context.Background(), obj, transport, map[string]string{
		"add_columns": "email,phone",
	}, adapter.NewWallClock())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !record.Applied {
		t.Error("expected Applied=true")
	}
	if record.Mutation != "schema-drift" {
		t.Errorf("Mutation = %q, want %q", record.Mutation, "schema-drift")
	}

	// Parse written data.
	calls := transport.getCalls()
	var writeData []byte
	for _, c := range calls {
		if c.Method == "Write" {
			writeData = c.Data
			break
		}
	}
	if writeData == nil {
		t.Fatal("no Write call found")
	}

	records, parseErr := parseJSONL(writeData)
	if parseErr != nil {
		t.Fatalf("failed to parse JSONL: %v", parseErr)
	}
	if len(records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(records))
	}

	for i, rec := range records {
		// New columns should exist with null values.
		for _, col := range []string{"email", "phone"} {
			v, ok := rec[col]
			if !ok {
				t.Errorf("record %d: missing added column %q", i, col)
			} else if v != nil {
				t.Errorf("record %d: column %q = %v, want nil", i, col, v)
			}
		}
		// Original columns should still exist.
		if _, ok := rec["id"]; !ok {
			t.Errorf("record %d: missing original column 'id'", i)
		}
		if _, ok := rec["name"]; !ok {
			t.Errorf("record %d: missing original column 'name'", i)
		}
	}
}

func TestSchemaDriftMutation_RemoveColumns(t *testing.T) {
	inputData := makeJSONL([]map[string]interface{}{
		{"id": float64(1), "name": "Alice", "email": "alice@test.com"},
		{"id": float64(2), "name": "Bob", "email": "bob@test.com"},
	})

	transport := newMockTransport()
	obj := types.DataObject{Key: "data/records.jsonl"}
	transport.ReadData[obj.Key] = inputData

	s := &mutation.SchemaDriftMutation{}
	record, err := s.Apply(context.Background(), obj, transport, map[string]string{
		"remove_columns": "email",
	}, adapter.NewWallClock())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !record.Applied {
		t.Error("expected Applied=true")
	}

	// Parse written data.
	calls := transport.getCalls()
	var writeData []byte
	for _, c := range calls {
		if c.Method == "Write" {
			writeData = c.Data
			break
		}
	}

	records, parseErr := parseJSONL(writeData)
	if parseErr != nil {
		t.Fatalf("failed to parse JSONL: %v", parseErr)
	}

	for i, rec := range records {
		if _, ok := rec["email"]; ok {
			t.Errorf("record %d: column 'email' should have been removed", i)
		}
		if _, ok := rec["id"]; !ok {
			t.Errorf("record %d: missing original column 'id'", i)
		}
		if _, ok := rec["name"]; !ok {
			t.Errorf("record %d: missing original column 'name'", i)
		}
	}
}

func TestSchemaDriftMutation_ChangeTypes(t *testing.T) {
	inputData := makeJSONL([]map[string]interface{}{
		{"id": float64(42), "name": "Alice", "active": true},
	})

	transport := newMockTransport()
	obj := types.DataObject{Key: "data/records.jsonl"}
	transport.ReadData[obj.Key] = inputData

	s := &mutation.SchemaDriftMutation{}
	record, err := s.Apply(context.Background(), obj, transport, map[string]string{
		"change_types": "id:string,active:string",
	}, adapter.NewWallClock())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !record.Applied {
		t.Error("expected Applied=true")
	}

	// Parse written data.
	calls := transport.getCalls()
	var writeData []byte
	for _, c := range calls {
		if c.Method == "Write" {
			writeData = c.Data
			break
		}
	}

	records, parseErr := parseJSONL(writeData)
	if parseErr != nil {
		t.Fatalf("failed to parse JSONL: %v", parseErr)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}

	rec := records[0]
	// id was float64(42), should now be "42".
	idVal, ok := rec["id"]
	if !ok {
		t.Fatal("missing 'id' field")
	}
	idStr, isStr := idVal.(string)
	if !isStr {
		t.Fatalf("id should be string, got %T", idVal)
	}
	if idStr != "42" {
		t.Errorf("id = %q, want %q", idStr, "42")
	}

	// active was bool true, should now be "true".
	activeVal, ok := rec["active"]
	if !ok {
		t.Fatal("missing 'active' field")
	}
	activeStr, isStr := activeVal.(string)
	if !isStr {
		t.Fatalf("active should be string, got %T", activeVal)
	}
	if activeStr != "true" {
		t.Errorf("active = %q, want %q", activeStr, "true")
	}
}

func TestSchemaDriftMutation_ReadError(t *testing.T) {
	transport := newMockTransport()
	obj := types.DataObject{Key: "data/missing.jsonl"}

	s := &mutation.SchemaDriftMutation{}
	record, err := s.Apply(context.Background(), obj, transport, map[string]string{
		"add_columns": "extra",
	}, adapter.NewWallClock())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if record.Applied {
		t.Error("expected Applied=false on error")
	}
}

func TestSchemaDriftMutation_NoParams(t *testing.T) {
	inputData := makeJSONL([]map[string]interface{}{
		{"id": float64(1), "name": "Alice"},
	})

	transport := newMockTransport()
	obj := types.DataObject{Key: "data/records.jsonl"}
	transport.ReadData[obj.Key] = inputData

	s := &mutation.SchemaDriftMutation{}
	record, err := s.Apply(context.Background(), obj, transport, map[string]string{}, adapter.NewWallClock())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// With no schema changes specified, it should still succeed (no-op write).
	if !record.Applied {
		t.Error("expected Applied=true")
	}
}
