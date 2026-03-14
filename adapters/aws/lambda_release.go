package aws

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-lambda-go/events"
)

// ReleaseHandler is a Lambda handler that scans held objects under the hold
// prefix and releases any whose release_at time has passed. It is designed
// to be triggered by a CloudWatch scheduled event.
type ReleaseHandler struct {
	transport      *S3Transport
	api            S3API
	pipelineBucket string
	holdPrefix     string

	// Now returns the current time. Defaults to time.Now but can be
	// overridden in tests for deterministic assertions.
	Now func() time.Time
}

// NewReleaseHandler creates a ReleaseHandler that scans pipelineBucket
// under holdPrefix for held objects and releases expired ones via transport.
func NewReleaseHandler(transport *S3Transport, api S3API, pipelineBucket, holdPrefix string) *ReleaseHandler {
	return &ReleaseHandler{
		transport:      transport,
		api:            api,
		pipelineBucket: pipelineBucket,
		holdPrefix:     holdPrefix,
		Now:            time.Now,
	}
}

// Handle processes a CloudWatch scheduled event by listing all objects under
// the hold prefix, reading each object's .meta sidecar, and releasing any
// object whose release_at time has passed. Individual release errors are
// collected and returned as an aggregated error.
func (h *ReleaseHandler) Handle(ctx context.Context, _ events.CloudWatchEvent) error {
	now := h.Now()

	dataKeys, err := h.listHeldDataKeys(ctx)
	if err != nil {
		return fmt.Errorf("list held objects: %w", err)
	}

	var releaseErrs []error

	for _, fullKey := range dataKeys {
		releaseAt, err := h.readReleaseTime(ctx, fullKey)
		if err != nil {
			log.Printf("WARN: skipping %s: malformed or unreadable .meta: %v", fullKey, err)
			continue
		}

		if !releaseAt.Before(now) {
			continue
		}

		// Strip the hold prefix to get the key that transport.Release expects.
		relativeKey := strings.TrimPrefix(fullKey, h.holdPrefix)
		if err := h.transport.Release(ctx, relativeKey); err != nil {
			releaseErrs = append(releaseErrs, fmt.Errorf("release %s: %w", relativeKey, err))
			continue
		}
	}

	return errors.Join(releaseErrs...)
}

// listHeldDataKeys pages through all objects under the hold prefix and
// returns keys that are NOT .meta sidecars (i.e., actual data objects).
func (h *ReleaseHandler) listHeldDataKeys(ctx context.Context) ([]string, error) {
	var keys []string
	var continuationToken *string

	for {
		input := &s3.ListObjectsV2Input{
			Bucket:            awssdk.String(h.pipelineBucket),
			Prefix:            awssdk.String(h.holdPrefix),
			ContinuationToken: continuationToken,
		}

		out, err := h.api.ListObjectsV2(ctx, input)
		if err != nil {
			return nil, fmt.Errorf("list objects in %s/%s: %w", h.pipelineBucket, h.holdPrefix, err)
		}

		for _, obj := range out.Contents {
			key := awssdk.ToString(obj.Key)
			if strings.HasSuffix(key, ".meta") {
				continue
			}
			keys = append(keys, key)
		}

		if !awssdk.ToBool(out.IsTruncated) {
			break
		}
		continuationToken = out.NextContinuationToken
	}

	return keys, nil
}

// readReleaseTime reads the .meta sidecar for a held object and returns
// the parsed release_at time.
func (h *ReleaseHandler) readReleaseTime(ctx context.Context, fullKey string) (time.Time, error) {
	metaKey := fullKey + ".meta"

	out, err := h.api.GetObject(ctx, &s3.GetObjectInput{
		Bucket: awssdk.String(h.pipelineBucket),
		Key:    awssdk.String(metaKey),
	})
	if err != nil {
		return time.Time{}, fmt.Errorf("read .meta %s: %w", metaKey, err)
	}
	defer out.Body.Close()

	var meta holdMeta
	if err := json.NewDecoder(out.Body).Decode(&meta); err != nil {
		return time.Time{}, fmt.Errorf("decode .meta %s: %w", metaKey, err)
	}

	releaseAt, err := time.Parse(time.RFC3339, meta.ReleaseAt)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse release_at in %s: %w", metaKey, err)
	}

	return releaseAt, nil
}
