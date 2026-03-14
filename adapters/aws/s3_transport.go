package aws

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// Compile-time interface assertion.
var _ adapter.DataTransport = (*S3Transport)(nil)

// S3Transport implements adapter.DataTransport using AWS S3 for data
// storage. Objects in the staging bucket are listed, read, and deleted.
// Objects are written to the pipeline bucket. Hold/Release use a
// configurable prefix within the pipeline bucket to temporarily hide
// objects from downstream consumers.
type S3Transport struct {
	api            S3API
	stagingBucket  string
	pipelineBucket string
	holdPrefix     string
}

// NewS3Transport creates an S3Transport from the given S3 API client and
// configuration. Call cfg.Defaults() before passing to ensure optional
// fields are populated.
func NewS3Transport(api S3API, cfg Config) *S3Transport {
	return &S3Transport{
		api:            api,
		stagingBucket:  cfg.StagingBucket,
		pipelineBucket: cfg.PipelineBucket,
		holdPrefix:     cfg.HoldPrefix,
	}
}

// holdMeta is the JSON structure stored in .meta sidecar objects.
type holdMeta struct {
	ReleaseAt   string `json:"release_at"`
	OriginalKey string `json:"original_key"`
}

// List returns DataObjects from the staging bucket whose keys start with
// prefix. Objects under the hold prefix are excluded from results.
// Pagination is handled transparently via continuation tokens.
func (t *S3Transport) List(ctx context.Context, prefix string) ([]types.DataObject, error) {
	var objects []types.DataObject
	var continuationToken *string

	for {
		input := &s3.ListObjectsV2Input{
			Bucket:            aws.String(t.stagingBucket),
			Prefix:            aws.String(prefix),
			ContinuationToken: continuationToken,
		}

		out, err := t.api.ListObjectsV2(ctx, input)
		if err != nil {
			return nil, fmt.Errorf("list objects in %s: %w", t.stagingBucket, err)
		}

		for _, obj := range out.Contents {
			key := aws.ToString(obj.Key)
			if strings.HasPrefix(key, t.holdPrefix) {
				continue
			}

			do := types.DataObject{
				Key: key,
			}
			if obj.Size != nil {
				do.Size = *obj.Size
			}
			if obj.LastModified != nil {
				do.LastModified = *obj.LastModified
			}
			objects = append(objects, do)
		}

		if !aws.ToBool(out.IsTruncated) {
			break
		}
		continuationToken = out.NextContinuationToken
	}

	return objects, nil
}

// Read returns the body of the object at key in the staging bucket.
func (t *S3Transport) Read(ctx context.Context, key string) (io.ReadCloser, error) {
	out, err := t.api.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(t.stagingBucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("read %s from %s: %w", key, t.stagingBucket, err)
	}
	return out.Body, nil
}

// Write puts data into the pipeline bucket at the given key.
func (t *S3Transport) Write(ctx context.Context, key string, data io.Reader) error {
	_, err := t.api.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(t.pipelineBucket),
		Key:    aws.String(key),
		Body:   data,
	})
	if err != nil {
		return fmt.Errorf("write %s to %s: %w", key, t.pipelineBucket, err)
	}
	return nil
}

// Delete removes the object at key from the staging bucket.
func (t *S3Transport) Delete(ctx context.Context, key string) error {
	_, err := t.api.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(t.stagingBucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("delete %s from %s: %w", key, t.stagingBucket, err)
	}
	return nil
}

// Hold moves an object from the staging bucket into the pipeline bucket
// under the hold prefix. A .meta sidecar is written first, then the
// object is copied, then the staging source is deleted.
//
// If the copy fails, the .meta sidecar is cleaned up. If the source
// delete fails, data is safe in hold but the error is still returned.
func (t *S3Transport) Hold(ctx context.Context, key string, until time.Time) error {
	holdKey := t.holdPrefix + key
	metaKey := holdKey + ".meta"

	// Step 1: Write .meta sidecar to pipeline bucket.
	meta := holdMeta{
		ReleaseAt:   until.Format(time.RFC3339),
		OriginalKey: key,
	}
	metaBytes, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("marshal hold metadata for %s: %w", key, err)
	}

	_, err = t.api.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(t.pipelineBucket),
		Key:    aws.String(metaKey),
		Body:   bytes.NewReader(metaBytes),
	})
	if err != nil {
		return fmt.Errorf("write hold metadata for %s: %w", key, err)
	}

	// Step 2: Copy object from staging to pipeline hold prefix.
	copySource := t.stagingBucket + "/" + key
	_, err = t.api.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:     aws.String(t.pipelineBucket),
		Key:        aws.String(holdKey),
		CopySource: aws.String(copySource),
	})
	if err != nil {
		// Clean up the .meta sidecar on copy failure.
		_, cleanupErr := t.api.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(t.pipelineBucket),
			Key:    aws.String(metaKey),
		})
		copyErr := fmt.Errorf("copy %s to hold: %w", key, err)
		if cleanupErr != nil {
			return fmt.Errorf("%w; additionally failed to clean up .meta sidecar: %v", copyErr, cleanupErr)
		}
		return copyErr
	}

	// Step 3: Delete original from staging.
	_, err = t.api.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(t.stagingBucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("delete source %s after hold copy: %w", key, err)
	}

	return nil
}

// Release copies a held object from the pipeline hold prefix to its
// original key in the pipeline bucket, then removes the held object and
// .meta sidecar.
//
// If the .meta sidecar is missing, the key itself is used as the
// destination (tolerant behavior). If the copy fails, the held object
// is not deleted so data is not lost.
func (t *S3Transport) Release(ctx context.Context, key string) error {
	holdKey := t.holdPrefix + key
	metaKey := holdKey + ".meta"

	// Step 1: Read .meta sidecar to determine original key.
	destKey := key // fallback if .meta missing
	out, err := t.api.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(t.pipelineBucket),
		Key:    aws.String(metaKey),
	})
	if err == nil {
		defer out.Body.Close()
		var meta holdMeta
		if decErr := json.NewDecoder(out.Body).Decode(&meta); decErr == nil && meta.OriginalKey != "" {
			destKey = meta.OriginalKey
		}
	}
	// If err != nil, we tolerate the missing .meta and use key directly.

	// Step 2: Copy held object to destination in pipeline bucket.
	copySource := t.pipelineBucket + "/" + holdKey
	_, err = t.api.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:     aws.String(t.pipelineBucket),
		Key:        aws.String(destKey),
		CopySource: aws.String(copySource),
	})
	if err != nil {
		return fmt.Errorf("release copy %s to %s: %w", holdKey, destKey, err)
	}

	// Step 3: Delete .meta sidecar first, then held object.
	// Ordering: .meta first so that if a crash occurs between deletes,
	// the held object still exists and re-running Release will succeed.
	_, err = t.api.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(t.pipelineBucket),
		Key:    aws.String(metaKey),
	})
	if err != nil {
		return fmt.Errorf("delete hold metadata %s: %w", metaKey, err)
	}

	_, err = t.api.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(t.pipelineBucket),
		Key:    aws.String(holdKey),
	})
	if err != nil {
		return fmt.Errorf("delete held object %s: %w", holdKey, err)
	}

	return nil
}
