package aws

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// Compile-time interface assertion.
var _ adapter.EventEmitter = (*S3ManifestObserver)(nil)

// S3ManifestObserver implements adapter.EventEmitter by uploading each
// chaos event as an individual JSONL object to an S3 bucket.
// Each object gets a unique key derived from the event timestamp and ID
// to guarantee traceability and avoid collisions.
type S3ManifestObserver struct {
	api    S3API
	bucket string
	prefix string
}

// NewS3ManifestObserver creates an S3ManifestObserver that writes events to
// the given bucket under the given key prefix.
func NewS3ManifestObserver(api S3API, bucket, prefix string) *S3ManifestObserver {
	return &S3ManifestObserver{
		api:    api,
		bucket: bucket,
		prefix: prefix,
	}
}

// Emit marshals the ChaosEvent to JSON and uploads it as a single JSONL
// object to S3. The key is built as:
//
//	<prefix>/<timestamp>-<event-id>.jsonl
//
// With an empty prefix the leading slash is omitted.
func (o *S3ManifestObserver) Emit(ctx context.Context, event types.ChaosEvent) error {
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("s3 manifest observer: marshal event: %w", err)
	}

	key := o.buildKey(event)

	input := &s3.PutObjectInput{
		Bucket: &o.bucket,
		Key:    &key,
		Body:   bytes.NewReader(data),
	}

	if _, err := o.api.PutObject(ctx, input); err != nil {
		return fmt.Errorf("s3 manifest observer: put object: %w", err)
	}
	return nil
}

// buildKey constructs the S3 object key for the given event.
func (o *S3ManifestObserver) buildKey(event types.ChaosEvent) string {
	timestamp := event.Timestamp.UTC().Format("20060102T150405")
	filename := fmt.Sprintf("%s-%s.jsonl", timestamp, event.ID)
	if o.prefix == "" {
		return filename
	}
	return fmt.Sprintf("%s/%s", o.prefix, filename)
}
