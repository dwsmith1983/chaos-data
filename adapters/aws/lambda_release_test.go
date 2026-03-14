package aws_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-lambda-go/events"

	chaosaws "github.com/dwsmith1983/chaos-data/adapters/aws"
)

// newTestReleaseHandler creates a ReleaseHandler with the given mock, a test
// transport, and a fixed "now" time for deterministic assertions.
func newTestReleaseHandler(mock *mockS3API, now time.Time) *chaosaws.ReleaseHandler {
	transport := newTestTransport(mock)
	h := chaosaws.NewReleaseHandler(transport, mock, "pipeline-bucket", "chaos-hold/")
	h.Now = func() time.Time { return now }
	return h
}

// makeMeta returns JSON bytes for a hold .meta sidecar with the given release time.
func makeMeta(releaseAt time.Time, originalKey string) string {
	m := struct {
		ReleaseAt   string `json:"release_at"`
		OriginalKey string `json:"original_key"`
	}{
		ReleaseAt:   releaseAt.Format(time.RFC3339),
		OriginalKey: originalKey,
	}
	b, _ := json.Marshal(m)
	return string(b)
}

func TestReleaseHandler_NoHeldObjects(t *testing.T) {
	t.Parallel()

	now := time.Date(2025, 7, 15, 12, 0, 0, 0, time.UTC)
	mock := &mockS3API{
		ListObjectsV2Fn: func(_ context.Context, _ *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
			return &s3.ListObjectsV2Output{
				Contents: nil,
			}, nil
		},
	}

	h := newTestReleaseHandler(mock, now)
	err := h.Handle(context.Background(), events.CloudWatchEvent{})
	if err != nil {
		t.Fatalf("Handle() error = %v, want nil", err)
	}
}

func TestReleaseHandler_ReleasesExpiredObjects(t *testing.T) {
	t.Parallel()

	now := time.Date(2025, 7, 15, 12, 0, 0, 0, time.UTC)
	pastTime1 := now.Add(-2 * time.Hour)
	pastTime2 := now.Add(-30 * time.Minute)
	futureTime := now.Add(2 * time.Hour)

	meta1 := makeMeta(pastTime1, "data/file1.csv")
	meta2 := makeMeta(pastTime2, "data/file2.csv")
	meta3 := makeMeta(futureTime, "data/file3.csv")

	mock := &mockS3API{
		ListObjectsV2Fn: func(_ context.Context, params *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
			if aws.ToString(params.Bucket) != "pipeline-bucket" {
				t.Errorf("ListObjectsV2 bucket = %q, want %q", aws.ToString(params.Bucket), "pipeline-bucket")
			}
			if aws.ToString(params.Prefix) != "chaos-hold/" {
				t.Errorf("ListObjectsV2 prefix = %q, want %q", aws.ToString(params.Prefix), "chaos-hold/")
			}
			return &s3.ListObjectsV2Output{
				Contents: []s3types.Object{
					{Key: aws.String("chaos-hold/data/file1.csv")},
					{Key: aws.String("chaos-hold/data/file1.csv.meta")},
					{Key: aws.String("chaos-hold/data/file2.csv")},
					{Key: aws.String("chaos-hold/data/file2.csv.meta")},
					{Key: aws.String("chaos-hold/data/file3.csv")},
					{Key: aws.String("chaos-hold/data/file3.csv.meta")},
				},
			}, nil
		},
		GetObjectFn: func(_ context.Context, params *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
			key := aws.ToString(params.Key)
			var body string
			switch key {
			case "chaos-hold/data/file1.csv.meta":
				body = meta1
			case "chaos-hold/data/file2.csv.meta":
				body = meta2
			case "chaos-hold/data/file3.csv.meta":
				body = meta3
			default:
				// Release() also calls GetObject for the .meta during release;
				// return appropriate meta for those calls too.
				if strings.HasSuffix(key, "file1.csv.meta") {
					body = meta1
				} else if strings.HasSuffix(key, "file2.csv.meta") {
					body = meta2
				} else {
					return nil, fmt.Errorf("unexpected GetObject key: %s", key)
				}
			}
			return &s3.GetObjectOutput{
				Body: io.NopCloser(strings.NewReader(body)),
			}, nil
		},
		CopyObjectFn: func(_ context.Context, _ *s3.CopyObjectInput, _ ...func(*s3.Options)) (*s3.CopyObjectOutput, error) {
			return &s3.CopyObjectOutput{}, nil
		},
		DeleteObjectFn: func(_ context.Context, _ *s3.DeleteObjectInput, _ ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
			return &s3.DeleteObjectOutput{}, nil
		},
	}

	// Track CopyObject calls to verify exactly which objects were released.
	var copyKeys []string
	mock.CopyObjectFn = func(_ context.Context, params *s3.CopyObjectInput, _ ...func(*s3.Options)) (*s3.CopyObjectOutput, error) {
		copyKeys = append(copyKeys, aws.ToString(params.Key))
		return &s3.CopyObjectOutput{}, nil
	}

	h := newTestReleaseHandler(mock, now)
	err := h.Handle(context.Background(), events.CloudWatchEvent{})
	if err != nil {
		t.Fatalf("Handle() error = %v, want nil", err)
	}

	// Release calls CopyObject once per released object. 2 expired objects
	// should be released, so we expect 2 copy calls (for the release copy).
	if len(copyKeys) != 2 {
		t.Fatalf("CopyObject called %d times, want 2; keys = %v", len(copyKeys), copyKeys)
	}

	// The released objects should have their original keys as the copy destination.
	released := make(map[string]bool)
	for _, k := range copyKeys {
		released[k] = true
	}
	if !released["data/file1.csv"] {
		t.Error("expected file1.csv to be released (expired)")
	}
	if !released["data/file2.csv"] {
		t.Error("expected file2.csv to be released (expired)")
	}
	if released["data/file3.csv"] {
		t.Error("file3.csv should NOT be released (future release_at)")
	}
}

func TestReleaseHandler_SkipsMetaFiles(t *testing.T) {
	t.Parallel()

	now := time.Date(2025, 7, 15, 12, 0, 0, 0, time.UTC)
	pastTime := now.Add(-1 * time.Hour)
	meta := makeMeta(pastTime, "data/file.csv")

	getObjectCalls := 0
	mock := &mockS3API{
		ListObjectsV2Fn: func(_ context.Context, _ *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
			return &s3.ListObjectsV2Output{
				Contents: []s3types.Object{
					// Only .meta files in the listing -- no data objects.
					{Key: aws.String("chaos-hold/data/file.csv.meta")},
					{Key: aws.String("chaos-hold/data/other.csv.meta")},
				},
			}, nil
		},
		GetObjectFn: func(_ context.Context, params *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
			getObjectCalls++
			return &s3.GetObjectOutput{
				Body: io.NopCloser(strings.NewReader(meta)),
			}, nil
		},
		CopyObjectFn: func(_ context.Context, _ *s3.CopyObjectInput, _ ...func(*s3.Options)) (*s3.CopyObjectOutput, error) {
			t.Error("CopyObject should not be called when only .meta files are listed")
			return &s3.CopyObjectOutput{}, nil
		},
		DeleteObjectFn: func(_ context.Context, _ *s3.DeleteObjectInput, _ ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
			return &s3.DeleteObjectOutput{}, nil
		},
	}

	h := newTestReleaseHandler(mock, now)
	err := h.Handle(context.Background(), events.CloudWatchEvent{})
	if err != nil {
		t.Fatalf("Handle() error = %v, want nil", err)
	}
	if getObjectCalls != 0 {
		t.Errorf("GetObject called %d times, want 0 (.meta files should be skipped entirely)", getObjectCalls)
	}
}

func TestReleaseHandler_MalformedMeta(t *testing.T) {
	t.Parallel()

	now := time.Date(2025, 7, 15, 12, 0, 0, 0, time.UTC)

	mock := &mockS3API{
		ListObjectsV2Fn: func(_ context.Context, _ *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
			return &s3.ListObjectsV2Output{
				Contents: []s3types.Object{
					{Key: aws.String("chaos-hold/data/bad.csv")},
					{Key: aws.String("chaos-hold/data/bad.csv.meta")},
				},
			}, nil
		},
		GetObjectFn: func(_ context.Context, params *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
			// Return malformed JSON for the .meta sidecar.
			return &s3.GetObjectOutput{
				Body: io.NopCloser(strings.NewReader("not valid json{{")),
			}, nil
		},
		CopyObjectFn: func(_ context.Context, _ *s3.CopyObjectInput, _ ...func(*s3.Options)) (*s3.CopyObjectOutput, error) {
			t.Error("CopyObject should not be called for objects with malformed .meta")
			return &s3.CopyObjectOutput{}, nil
		},
		DeleteObjectFn: func(_ context.Context, _ *s3.DeleteObjectInput, _ ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
			return &s3.DeleteObjectOutput{}, nil
		},
	}

	h := newTestReleaseHandler(mock, now)
	err := h.Handle(context.Background(), events.CloudWatchEvent{})
	// Malformed .meta should be skipped — not cause a fatal error.
	if err != nil {
		t.Fatalf("Handle() error = %v, want nil (malformed .meta should be skipped)", err)
	}
}

func TestReleaseHandler_ContinuesOnReleaseError(t *testing.T) {
	t.Parallel()

	now := time.Date(2025, 7, 15, 12, 0, 0, 0, time.UTC)
	pastTime := now.Add(-1 * time.Hour)
	meta1 := makeMeta(pastTime, "data/file1.csv")
	meta2 := makeMeta(pastTime, "data/file2.csv")

	copyCallCount := 0
	mock := &mockS3API{
		ListObjectsV2Fn: func(_ context.Context, _ *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
			return &s3.ListObjectsV2Output{
				Contents: []s3types.Object{
					{Key: aws.String("chaos-hold/data/file1.csv")},
					{Key: aws.String("chaos-hold/data/file1.csv.meta")},
					{Key: aws.String("chaos-hold/data/file2.csv")},
					{Key: aws.String("chaos-hold/data/file2.csv.meta")},
				},
			}, nil
		},
		GetObjectFn: func(_ context.Context, params *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
			key := aws.ToString(params.Key)
			if strings.HasSuffix(key, "file1.csv.meta") {
				return &s3.GetObjectOutput{
					Body: io.NopCloser(strings.NewReader(meta1)),
				}, nil
			}
			if strings.HasSuffix(key, "file2.csv.meta") {
				return &s3.GetObjectOutput{
					Body: io.NopCloser(strings.NewReader(meta2)),
				}, nil
			}
			return nil, fmt.Errorf("unexpected GetObject key: %s", key)
		},
		CopyObjectFn: func(_ context.Context, params *s3.CopyObjectInput, _ ...func(*s3.Options)) (*s3.CopyObjectOutput, error) {
			copyCallCount++
			destKey := aws.ToString(params.Key)
			// First release (file1) fails at copy stage.
			if destKey == "data/file1.csv" {
				return nil, errors.New("copy failed for file1")
			}
			return &s3.CopyObjectOutput{}, nil
		},
		DeleteObjectFn: func(_ context.Context, _ *s3.DeleteObjectInput, _ ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
			return &s3.DeleteObjectOutput{}, nil
		},
	}

	h := newTestReleaseHandler(mock, now)
	err := h.Handle(context.Background(), events.CloudWatchEvent{})

	// Should return an error (aggregated), not nil.
	if err == nil {
		t.Fatal("Handle() error = nil, want aggregated error")
	}

	// Both expired objects should have been attempted (continue on error).
	if copyCallCount != 2 {
		t.Errorf("CopyObject called %d times, want 2 (should attempt both despite first failure)", copyCallCount)
	}

	// Error message should mention the failed release.
	if !strings.Contains(err.Error(), "file1") {
		t.Errorf("error = %q, want it to mention file1", err.Error())
	}
}

func TestReleaseHandler_Pagination(t *testing.T) {
	t.Parallel()

	now := time.Date(2025, 7, 15, 12, 0, 0, 0, time.UTC)
	pastTime := now.Add(-1 * time.Hour)
	meta1 := makeMeta(pastTime, "data/page1.csv")
	meta2 := makeMeta(pastTime, "data/page2.csv")

	listCallCount := 0
	var copyKeys []string
	mock := &mockS3API{
		ListObjectsV2Fn: func(_ context.Context, params *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
			listCallCount++
			if listCallCount == 1 {
				return &s3.ListObjectsV2Output{
					Contents: []s3types.Object{
						{Key: aws.String("chaos-hold/data/page1.csv")},
						{Key: aws.String("chaos-hold/data/page1.csv.meta")},
					},
					IsTruncated:           aws.Bool(true),
					NextContinuationToken: aws.String("page-token"),
				}, nil
			}
			if aws.ToString(params.ContinuationToken) != "page-token" {
				t.Errorf("ContinuationToken = %q, want %q", aws.ToString(params.ContinuationToken), "page-token")
			}
			return &s3.ListObjectsV2Output{
				Contents: []s3types.Object{
					{Key: aws.String("chaos-hold/data/page2.csv")},
					{Key: aws.String("chaos-hold/data/page2.csv.meta")},
				},
			}, nil
		},
		GetObjectFn: func(_ context.Context, params *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
			key := aws.ToString(params.Key)
			if strings.HasSuffix(key, "page1.csv.meta") {
				return &s3.GetObjectOutput{
					Body: io.NopCloser(strings.NewReader(meta1)),
				}, nil
			}
			if strings.HasSuffix(key, "page2.csv.meta") {
				return &s3.GetObjectOutput{
					Body: io.NopCloser(strings.NewReader(meta2)),
				}, nil
			}
			return nil, fmt.Errorf("unexpected GetObject key: %s", key)
		},
		CopyObjectFn: func(_ context.Context, params *s3.CopyObjectInput, _ ...func(*s3.Options)) (*s3.CopyObjectOutput, error) {
			copyKeys = append(copyKeys, aws.ToString(params.Key))
			return &s3.CopyObjectOutput{}, nil
		},
		DeleteObjectFn: func(_ context.Context, _ *s3.DeleteObjectInput, _ ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
			return &s3.DeleteObjectOutput{}, nil
		},
	}

	h := newTestReleaseHandler(mock, now)
	err := h.Handle(context.Background(), events.CloudWatchEvent{})
	if err != nil {
		t.Fatalf("Handle() error = %v, want nil", err)
	}
	if listCallCount != 2 {
		t.Errorf("ListObjectsV2 called %d times, want 2", listCallCount)
	}
	if len(copyKeys) != 2 {
		t.Fatalf("CopyObject called %d times, want 2; keys = %v", len(copyKeys), copyKeys)
	}
}
