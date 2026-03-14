package types_test

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/dwsmith1983/chaos-data/pkg/types"
)

func TestObjectFilterMatches(t *testing.T) {
	tests := []struct {
		name   string
		filter types.ObjectFilter
		obj    types.DataObject
		want   bool
	}{
		{
			name:   "empty filter matches any key",
			filter: types.ObjectFilter{},
			obj:    types.DataObject{Key: "anything/at/all.csv"},
			want:   true,
		},
		{
			name:   "prefix only match",
			filter: types.ObjectFilter{Prefix: "data/raw/"},
			obj:    types.DataObject{Key: "data/raw/file.parquet"},
			want:   true,
		},
		{
			name:   "prefix only no match",
			filter: types.ObjectFilter{Prefix: "data/raw/"},
			obj:    types.DataObject{Key: "data/processed/file.parquet"},
			want:   false,
		},
		{
			name:   "glob only match",
			filter: types.ObjectFilter{Match: "*.csv"},
			obj:    types.DataObject{Key: "report.csv"},
			want:   true,
		},
		{
			name:   "glob only no match",
			filter: types.ObjectFilter{Match: "*.csv"},
			obj:    types.DataObject{Key: "report.parquet"},
			want:   false,
		},
		{
			name:   "combined prefix and glob both match",
			filter: types.ObjectFilter{Prefix: "data/", Match: "data/*.csv"},
			obj:    types.DataObject{Key: "data/report.csv"},
			want:   true,
		},
		{
			name:   "combined prefix matches but glob does not",
			filter: types.ObjectFilter{Prefix: "data/", Match: "data/*.csv"},
			obj:    types.DataObject{Key: "data/report.parquet"},
			want:   false,
		},
		{
			name:   "combined glob matches but prefix does not",
			filter: types.ObjectFilter{Prefix: "archive/", Match: "*.csv"},
			obj:    types.DataObject{Key: "data/report.csv"},
			want:   false,
		},
		{
			name:   "prefix is exact key",
			filter: types.ObjectFilter{Prefix: "data/report.csv"},
			obj:    types.DataObject{Key: "data/report.csv"},
			want:   true,
		},
		{
			name:   "glob with question mark wildcard",
			filter: types.ObjectFilter{Match: "file?.txt"},
			obj:    types.DataObject{Key: "file1.txt"},
			want:   true,
		},
		{
			name:   "glob with character class",
			filter: types.ObjectFilter{Match: "file[0-9].txt"},
			obj:    types.DataObject{Key: "file3.txt"},
			want:   true,
		},
		{
			name:   "empty key with empty filter",
			filter: types.ObjectFilter{},
			obj:    types.DataObject{Key: ""},
			want:   true,
		},
		{
			name:   "glob does not match across path separators",
			filter: types.ObjectFilter{Match: "*.csv"},
			obj:    types.DataObject{Key: "data/report.csv"},
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.filter.Matches(tt.obj)
			if got != tt.want {
				t.Errorf("ObjectFilter%+v.Matches(Key=%q) = %v, want %v",
					tt.filter, tt.obj.Key, got, tt.want)
			}
		})
	}
}

func TestObjectFilterMatchesInvalidGlob(t *testing.T) {
	filter := types.ObjectFilter{Match: "[invalid"}
	obj := types.DataObject{Key: "anything.txt"}

	got := filter.Matches(obj)
	if got {
		t.Error("ObjectFilter with invalid glob pattern should not match")
	}
}

func TestTargetValidate(t *testing.T) {
	tests := []struct {
		name    string
		target  types.Target
		wantErr bool
	}{
		{
			name:    "valid layer data",
			target:  types.Target{Layer: "data"},
			wantErr: false,
		},
		{
			name:    "valid layer state",
			target:  types.Target{Layer: "state"},
			wantErr: false,
		},
		{
			name:    "valid layer orchestrator",
			target:  types.Target{Layer: "orchestrator"},
			wantErr: false,
		},
		{
			name:    "invalid layer",
			target:  types.Target{Layer: "network"},
			wantErr: true,
		},
		{
			name:    "empty layer",
			target:  types.Target{Layer: ""},
			wantErr: true,
		},
		{
			name:    "uppercase layer is invalid",
			target:  types.Target{Layer: "Data"},
			wantErr: true,
		},
		{
			name: "valid layer with transport and filter",
			target: types.Target{
				Layer:     "data",
				Transport: "s3://my-bucket",
				Filter:    types.ObjectFilter{Prefix: "raw/"},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.target.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Target%+v.Validate() error = %v, wantErr %v",
					tt.target, err, tt.wantErr)
			}
		})
	}
}

func TestTargetValidateErrorMessage(t *testing.T) {
	target := types.Target{Layer: "bogus"}
	err := target.Validate()
	if err == nil {
		t.Fatal("expected error for invalid layer")
	}

	if !errors.Is(err, types.ErrInvalidLayer) {
		t.Errorf("expected ErrInvalidLayer, got: %v", err)
	}
}

func TestDataObjectJSONRoundTrip(t *testing.T) {
	now := time.Date(2026, 3, 14, 10, 30, 0, 0, time.UTC)

	original := types.DataObject{
		Key:          "data/raw/events.parquet",
		Size:         1048576,
		LastModified: now,
		ContentType:  "application/octet-stream",
		Metadata: map[string]string{
			"source":  "pipeline-a",
			"version": "2",
		},
	}

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("json.Marshal(DataObject) error: %v", err)
	}

	var roundTripped types.DataObject
	if err := json.Unmarshal(data, &roundTripped); err != nil {
		t.Fatalf("json.Unmarshal(DataObject) error: %v", err)
	}

	if roundTripped.Key != original.Key {
		t.Errorf("Key = %q, want %q", roundTripped.Key, original.Key)
	}
	if roundTripped.Size != original.Size {
		t.Errorf("Size = %d, want %d", roundTripped.Size, original.Size)
	}
	if !roundTripped.LastModified.Equal(original.LastModified) {
		t.Errorf("LastModified = %v, want %v", roundTripped.LastModified, original.LastModified)
	}
	if roundTripped.ContentType != original.ContentType {
		t.Errorf("ContentType = %q, want %q", roundTripped.ContentType, original.ContentType)
	}
	if roundTripped.Metadata["source"] != "pipeline-a" {
		t.Errorf("Metadata[source] = %q, want %q", roundTripped.Metadata["source"], "pipeline-a")
	}
	if roundTripped.Metadata["version"] != "2" {
		t.Errorf("Metadata[version] = %q, want %q", roundTripped.Metadata["version"], "2")
	}
}

func TestDataObjectJSONFieldNames(t *testing.T) {
	obj := types.DataObject{
		Key:          "test.csv",
		Size:         100,
		LastModified: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		ContentType:  "text/csv",
		Metadata:     map[string]string{"k": "v"},
	}

	data, err := json.Marshal(obj)
	if err != nil {
		t.Fatalf("json.Marshal() error: %v", err)
	}

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("json.Unmarshal() error: %v", err)
	}

	expectedFields := []string{"key", "size", "last_modified", "content_type", "metadata"}
	for _, field := range expectedFields {
		if _, ok := raw[field]; !ok {
			t.Errorf("expected JSON field %q not found in output: %s", field, string(data))
		}
	}
}

func TestHeldObjectEmbedsDataObject(t *testing.T) {
	now := time.Date(2026, 3, 14, 10, 30, 0, 0, time.UTC)
	holdUntil := now.Add(5 * time.Minute)

	held := types.HeldObject{
		DataObject: types.DataObject{
			Key:          "data/important.parquet",
			Size:         2048,
			LastModified: now,
			ContentType:  "application/octet-stream",
		},
		HeldUntil: holdUntil,
		Reason:    "chaos-hold: latency injection",
	}

	// Verify embedded fields are accessible directly.
	if held.Key != "data/important.parquet" {
		t.Errorf("held.Key = %q, want %q", held.Key, "data/important.parquet")
	}
	if held.Size != 2048 {
		t.Errorf("held.Size = %d, want %d", held.Size, 2048)
	}
	if !held.HeldUntil.Equal(holdUntil) {
		t.Errorf("held.HeldUntil = %v, want %v", held.HeldUntil, holdUntil)
	}
	if held.Reason != "chaos-hold: latency injection" {
		t.Errorf("held.Reason = %q, want %q", held.Reason, "chaos-hold: latency injection")
	}
}

func TestHeldObjectJSONRoundTrip(t *testing.T) {
	now := time.Date(2026, 3, 14, 10, 30, 0, 0, time.UTC)
	holdUntil := now.Add(5 * time.Minute)

	original := types.HeldObject{
		DataObject: types.DataObject{
			Key:          "data/held.parquet",
			Size:         512,
			LastModified: now,
			ContentType:  "application/octet-stream",
		},
		HeldUntil: holdUntil,
		Reason:    "chaos-hold: test",
	}

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("json.Marshal(HeldObject) error: %v", err)
	}

	var roundTripped types.HeldObject
	if err := json.Unmarshal(data, &roundTripped); err != nil {
		t.Fatalf("json.Unmarshal(HeldObject) error: %v", err)
	}

	if roundTripped.Key != original.Key {
		t.Errorf("Key = %q, want %q", roundTripped.Key, original.Key)
	}
	if roundTripped.Size != original.Size {
		t.Errorf("Size = %d, want %d", roundTripped.Size, original.Size)
	}
	if !roundTripped.HeldUntil.Equal(original.HeldUntil) {
		t.Errorf("HeldUntil = %v, want %v", roundTripped.HeldUntil, original.HeldUntil)
	}
	if roundTripped.Reason != original.Reason {
		t.Errorf("Reason = %q, want %q", roundTripped.Reason, original.Reason)
	}
}

func TestHeldObjectJSONFieldNames(t *testing.T) {
	held := types.HeldObject{
		DataObject: types.DataObject{
			Key:          "test.csv",
			Size:         100,
			LastModified: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
			ContentType:  "text/csv",
		},
		HeldUntil: time.Date(2026, 1, 1, 0, 5, 0, 0, time.UTC),
		Reason:    "test",
	}

	data, err := json.Marshal(held)
	if err != nil {
		t.Fatalf("json.Marshal() error: %v", err)
	}

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("json.Unmarshal() error: %v", err)
	}

	expectedFields := []string{"key", "size", "last_modified", "content_type", "held_until", "reason"}
	for _, field := range expectedFields {
		if _, ok := raw[field]; !ok {
			t.Errorf("expected JSON field %q not found in output: %s", field, string(data))
		}
	}
}
