package aws_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"

	chaosaws "github.com/dwsmith1983/chaos-data/adapters/aws"
	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// Compile-time assertion: S3ManifestObserver satisfies EventEmitter.
var _ adapter.EventEmitter = (*chaosaws.S3ManifestObserver)(nil)

func TestS3ManifestObserver_Emit(t *testing.T) {
	t.Parallel()

	var capturedInput *s3.PutObjectInput
	mock := &mockS3API{
		PutObjectFn: func(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
			capturedInput = params
			return &s3.PutObjectOutput{}, nil
		},
	}

	obs := chaosaws.NewS3ManifestObserver(mock, "my-bucket", "chaos/manifests")
	evt := newTestChaosEvent()

	if err := obs.Emit(context.Background(), evt); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}

	if capturedInput == nil {
		t.Fatal("PutObject was not called")
	}

	// Verify bucket.
	if capturedInput.Bucket == nil || *capturedInput.Bucket != "my-bucket" {
		t.Errorf("Bucket = %v, want %q", capturedInput.Bucket, "my-bucket")
	}

	// Verify key is non-empty and starts with the prefix.
	if capturedInput.Key == nil || *capturedInput.Key == "" {
		t.Fatal("Key is nil or empty")
	}
	if !strings.HasPrefix(*capturedInput.Key, "chaos/manifests/") {
		t.Errorf("Key %q does not start with prefix %q", *capturedInput.Key, "chaos/manifests/")
	}

	// Verify body is valid JSON for the event.
	if capturedInput.Body == nil {
		t.Fatal("Body is nil")
	}
	bodyBytes, err := io.ReadAll(capturedInput.Body)
	if err != nil {
		t.Fatalf("ReadAll(Body) error = %v", err)
	}
	var got types.ChaosEvent
	if err := json.Unmarshal(bodyBytes, &got); err != nil {
		t.Fatalf("Body is not valid JSON: %v\nbody: %s", err, string(bodyBytes))
	}
	if got.ID != evt.ID {
		t.Errorf("decoded ID = %q, want %q", got.ID, evt.ID)
	}
	if got.ExperimentID != evt.ExperimentID {
		t.Errorf("decoded ExperimentID = %q, want %q", got.ExperimentID, evt.ExperimentID)
	}
}

func TestS3ManifestObserver_EmitMultiple(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	var capturedKeys []string

	mock := &mockS3API{
		PutObjectFn: func(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
			mu.Lock()
			defer mu.Unlock()
			if params.Key != nil {
				capturedKeys = append(capturedKeys, *params.Key)
			}
			return &s3.PutObjectOutput{}, nil
		},
	}

	obs := chaosaws.NewS3ManifestObserver(mock, "my-bucket", "chaos/manifests")

	events := []types.ChaosEvent{
		newTestChaosEvent(),
		{
			ID:           "evt-002",
			ExperimentID: "exp-001",
			Scenario:     "delay",
			Category:     "latency",
			Severity:     types.SeverityLow,
			Target:       "kafka://topic-a",
			Mutation:     "delay-ms",
			Params:       map[string]string{"ms": "500"},
			Timestamp:    newTestChaosEvent().Timestamp,
			Mode:         "deterministic",
		},
		{
			ID:           "evt-003",
			ExperimentID: "exp-001",
			Scenario:     "drop",
			Category:     "data-loss",
			Severity:     types.SeveritySevere,
			Target:       "s3://prod/critical.csv",
			Mutation:     "drop-rows",
			Params:       map[string]string{"pct": "10"},
			Timestamp:    newTestChaosEvent().Timestamp,
			Mode:         "deterministic",
		},
	}

	for _, ev := range events {
		if err := obs.Emit(context.Background(), ev); err != nil {
			t.Fatalf("Emit() error = %v", err)
		}
	}

	if len(capturedKeys) != len(events) {
		t.Fatalf("PutObject called %d times, want %d", len(capturedKeys), len(events))
	}

	// All keys must be unique.
	seen := make(map[string]bool, len(capturedKeys))
	for _, k := range capturedKeys {
		if seen[k] {
			t.Errorf("duplicate key %q", k)
		}
		seen[k] = true
	}
}

func TestS3ManifestObserver_PutObjectError(t *testing.T) {
	t.Parallel()

	apiErr := errors.New("s3: access denied")
	mock := &mockS3API{
		PutObjectFn: func(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
			return nil, apiErr
		},
	}

	obs := chaosaws.NewS3ManifestObserver(mock, "my-bucket", "chaos/manifests")
	err := obs.Emit(context.Background(), newTestChaosEvent())
	if err == nil {
		t.Fatal("Emit() = nil, want error")
	}
	if !errors.Is(err, apiErr) {
		t.Errorf("Emit() error = %v, want wrapped %v", err, apiErr)
	}
}

func TestS3ManifestObserver_KeyFormat(t *testing.T) {
	t.Parallel()

	var capturedKey string
	mock := &mockS3API{
		PutObjectFn: func(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
			if params.Key != nil {
				capturedKey = *params.Key
			}
			return &s3.PutObjectOutput{}, nil
		},
	}

	obs := chaosaws.NewS3ManifestObserver(mock, "my-bucket", "audit/logs")
	evt := newTestChaosEvent()

	if err := obs.Emit(context.Background(), evt); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}

	// Key must contain the event ID for traceability.
	if !strings.Contains(capturedKey, evt.ID) {
		t.Errorf("key %q does not contain event ID %q", capturedKey, evt.ID)
	}

	// Key must contain the timestamp for traceability (YYYYMMDD portion).
	timestampPrefix := evt.Timestamp.UTC().Format("20060102")
	if !strings.Contains(capturedKey, timestampPrefix) {
		t.Errorf("key %q does not contain timestamp prefix %q", capturedKey, timestampPrefix)
	}

	// Key must end with .jsonl.
	if !strings.HasSuffix(capturedKey, ".jsonl") {
		t.Errorf("key %q does not end with .jsonl", capturedKey)
	}

	// Key must start with the configured prefix.
	if !strings.HasPrefix(capturedKey, "audit/logs/") {
		t.Errorf("key %q does not start with prefix %q", capturedKey, "audit/logs/")
	}
}

func TestS3ManifestObserver_BodyContent(t *testing.T) {
	t.Parallel()

	var capturedBody []byte
	mock := &mockS3API{
		PutObjectFn: func(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
			if params.Body != nil {
				var err error
				capturedBody, err = io.ReadAll(params.Body)
				if err != nil {
					return nil, err
				}
			}
			return &s3.PutObjectOutput{}, nil
		},
	}

	obs := chaosaws.NewS3ManifestObserver(mock, "my-bucket", "chaos/manifests")
	evt := newTestChaosEvent()

	if err := obs.Emit(context.Background(), evt); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}

	// Body must be valid JSON.
	if !json.Valid(capturedBody) {
		t.Fatalf("body is not valid JSON: %s", string(capturedBody))
	}

	// Body must round-trip to the original event.
	var decoded types.ChaosEvent
	if err := json.Unmarshal(capturedBody, &decoded); err != nil {
		t.Fatalf("unmarshal body: %v", err)
	}
	if decoded.ID != evt.ID {
		t.Errorf("ID = %q, want %q", decoded.ID, evt.ID)
	}
	if decoded.Scenario != evt.Scenario {
		t.Errorf("Scenario = %q, want %q", decoded.Scenario, evt.Scenario)
	}
	if decoded.Severity != evt.Severity {
		t.Errorf("Severity = %v, want %v", decoded.Severity, evt.Severity)
	}
	if !decoded.Timestamp.Equal(evt.Timestamp) {
		t.Errorf("Timestamp = %v, want %v", decoded.Timestamp, evt.Timestamp)
	}
}

func TestS3ManifestObserver_EmptyPrefix(t *testing.T) {
	t.Parallel()

	var capturedKey string
	mock := &mockS3API{
		PutObjectFn: func(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
			if params.Key != nil {
				capturedKey = *params.Key
			}
			return &s3.PutObjectOutput{}, nil
		},
	}

	obs := chaosaws.NewS3ManifestObserver(mock, "my-bucket", "")
	evt := newTestChaosEvent()

	if err := obs.Emit(context.Background(), evt); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}

	// With empty prefix, key should still contain event ID and be non-empty.
	if capturedKey == "" {
		t.Fatal("key is empty with empty prefix")
	}
	if !strings.Contains(capturedKey, evt.ID) {
		t.Errorf("key %q does not contain event ID %q", capturedKey, evt.ID)
	}

	// Should not start with a leading slash.
	if strings.HasPrefix(capturedKey, "/") {
		t.Errorf("key %q has unexpected leading slash", capturedKey)
	}
}

// readAll is a helper that consumes an io.Reader into a byte slice.
// Declared here to avoid shadowing the io.ReadAll calls in tests.
var _ = bytes.NewReader // keep bytes import alive for interface check
