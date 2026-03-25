package aws_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"

	chaosaws "github.com/dwsmith1983/chaos-data/adapters/aws"
)

// newTestTransport creates an S3Transport with the given mock and default config.
func newTestTransport(api *mockS3API) *chaosaws.S3Transport {
	cfg := chaosaws.Config{
		StagingBucket:  "staging-bucket",
		PipelineBucket: "pipeline-bucket",
		TableName:      "chaos-table",
	}
	cfg.Defaults()
	return chaosaws.NewS3Transport(api, cfg)
}

// --- List tests ---

func TestS3Transport_List_Empty(t *testing.T) {
	t.Parallel()

	mock := &mockS3API{
		ListObjectsV2Fn: func(_ context.Context, params *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
			return &s3.ListObjectsV2Output{
				Contents: nil,
			}, nil
		},
	}

	tr := newTestTransport(mock)
	objs, err := tr.List(context.Background(), "")
	if err != nil {
		t.Fatalf("List() error = %v, want nil", err)
	}
	if len(objs) != 0 {
		t.Errorf("List() returned %d objects, want 0", len(objs))
	}
}

func TestS3Transport_List_WithPrefix(t *testing.T) {
	t.Parallel()

	now := time.Date(2025, 6, 15, 12, 0, 0, 0, time.UTC)
	mock := &mockS3API{
		ListObjectsV2Fn: func(_ context.Context, params *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
			if aws.ToString(params.Bucket) != "staging-bucket" {
				t.Errorf("Bucket = %q, want %q", aws.ToString(params.Bucket), "staging-bucket")
			}
			if aws.ToString(params.Prefix) != "data/" {
				t.Errorf("Prefix = %q, want %q", aws.ToString(params.Prefix), "data/")
			}
			return &s3.ListObjectsV2Output{
				Contents: []s3types.Object{
					{Key: aws.String("data/file1.csv"), Size: aws.Int64(100), LastModified: &now},
					{Key: aws.String("data/file2.csv"), Size: aws.Int64(200), LastModified: &now},
				},
			}, nil
		},
	}

	tr := newTestTransport(mock)
	objs, err := tr.List(context.Background(), "data/")
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if len(objs) != 2 {
		t.Fatalf("List() returned %d objects, want 2", len(objs))
	}
	if objs[0].Key != "data/file1.csv" {
		t.Errorf("objs[0].Key = %q, want %q", objs[0].Key, "data/file1.csv")
	}
	if objs[0].Size != 100 {
		t.Errorf("objs[0].Size = %d, want 100", objs[0].Size)
	}
	if !objs[0].LastModified.Equal(now) {
		t.Errorf("objs[0].LastModified = %v, want %v", objs[0].LastModified, now)
	}
	if objs[1].Key != "data/file2.csv" {
		t.Errorf("objs[1].Key = %q, want %q", objs[1].Key, "data/file2.csv")
	}
}

func TestS3Transport_List_Pagination(t *testing.T) {
	t.Parallel()

	callCount := 0
	mock := &mockS3API{
		ListObjectsV2Fn: func(_ context.Context, params *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
			callCount++
			if callCount == 1 {
				return &s3.ListObjectsV2Output{
					Contents:              []s3types.Object{{Key: aws.String("page1.csv")}},
					IsTruncated:           aws.Bool(true),
					NextContinuationToken: aws.String("token-abc"),
				}, nil
			}
			if aws.ToString(params.ContinuationToken) != "token-abc" {
				t.Errorf("ContinuationToken = %q, want %q", aws.ToString(params.ContinuationToken), "token-abc")
			}
			return &s3.ListObjectsV2Output{
				Contents: []s3types.Object{{Key: aws.String("page2.csv")}},
			}, nil
		},
	}

	tr := newTestTransport(mock)
	objs, err := tr.List(context.Background(), "")
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if len(objs) != 2 {
		t.Fatalf("List() returned %d objects, want 2", len(objs))
	}
	if callCount != 2 {
		t.Errorf("ListObjectsV2 called %d times, want 2", callCount)
	}
}

func TestS3Transport_List_ExcludesHoldPrefix(t *testing.T) {
	t.Parallel()

	mock := &mockS3API{
		ListObjectsV2Fn: func(_ context.Context, _ *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
			return &s3.ListObjectsV2Output{
				Contents: []s3types.Object{
					{Key: aws.String("visible.csv")},
					{Key: aws.String("chaos-hold/hidden.csv")},
					{Key: aws.String("chaos-hold/hidden.csv.meta")},
					{Key: aws.String("also-visible.csv")},
				},
			}, nil
		},
	}

	tr := newTestTransport(mock)
	objs, err := tr.List(context.Background(), "")
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if len(objs) != 2 {
		t.Fatalf("List() returned %d objects, want 2", len(objs))
	}
	for _, o := range objs {
		if strings.HasPrefix(o.Key, "chaos-hold/") {
			t.Errorf("List() returned held object %q, want it excluded", o.Key)
		}
	}
}

func TestS3Transport_List_Error(t *testing.T) {
	t.Parallel()

	mock := &mockS3API{
		ListObjectsV2Fn: func(_ context.Context, _ *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
			return nil, errors.New("access denied")
		},
	}

	tr := newTestTransport(mock)
	_, err := tr.List(context.Background(), "")
	if err == nil {
		t.Fatal("List() error = nil, want error")
	}
}

// --- Read tests ---

func TestS3Transport_Read_Happy(t *testing.T) {
	t.Parallel()

	body := "file content here"
	mock := &mockS3API{
		GetObjectFn: func(_ context.Context, params *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
			if aws.ToString(params.Bucket) != "staging-bucket" {
				t.Errorf("Bucket = %q, want %q", aws.ToString(params.Bucket), "staging-bucket")
			}
			if aws.ToString(params.Key) != "data/test.csv" {
				t.Errorf("Key = %q, want %q", aws.ToString(params.Key), "data/test.csv")
			}
			return &s3.GetObjectOutput{
				Body: io.NopCloser(strings.NewReader(body)),
			}, nil
		},
	}

	tr := newTestTransport(mock)
	rc, err := tr.Read(context.Background(), "data/test.csv")
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	defer rc.Close()

	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if string(got) != body {
		t.Errorf("Read() content = %q, want %q", string(got), body)
	}
}

func TestS3Transport_Read_Error(t *testing.T) {
	t.Parallel()

	mock := &mockS3API{
		GetObjectFn: func(_ context.Context, _ *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
			return nil, errors.New("no such key")
		},
	}

	tr := newTestTransport(mock)
	_, err := tr.Read(context.Background(), "missing.csv")
	if err == nil {
		t.Fatal("Read() error = nil, want error")
	}
}

// --- Write tests ---

func TestS3Transport_Write_Happy(t *testing.T) {
	t.Parallel()

	var capturedBucket, capturedKey string
	var capturedBody string
	mock := &mockS3API{
		PutObjectFn: func(_ context.Context, params *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
			capturedBucket = aws.ToString(params.Bucket)
			capturedKey = aws.ToString(params.Key)
			b, _ := io.ReadAll(params.Body)
			capturedBody = string(b)
			return &s3.PutObjectOutput{}, nil
		},
	}

	tr := newTestTransport(mock)
	err := tr.Write(context.Background(), "output/result.csv", strings.NewReader("result data"))
	if err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if capturedBucket != "pipeline-bucket" {
		t.Errorf("PutObject bucket = %q, want %q", capturedBucket, "pipeline-bucket")
	}
	if capturedKey != "output/result.csv" {
		t.Errorf("PutObject key = %q, want %q", capturedKey, "output/result.csv")
	}
	if capturedBody != "result data" {
		t.Errorf("PutObject body = %q, want %q", capturedBody, "result data")
	}
}

func TestS3Transport_Write_Error(t *testing.T) {
	t.Parallel()

	mock := &mockS3API{
		PutObjectFn: func(_ context.Context, _ *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
			return nil, errors.New("write failed")
		},
	}

	tr := newTestTransport(mock)
	err := tr.Write(context.Background(), "key.csv", strings.NewReader("data"))
	if err == nil {
		t.Fatal("Write() error = nil, want error")
	}
}

// --- Delete tests ---

func TestS3Transport_Delete_Happy(t *testing.T) {
	t.Parallel()

	var capturedBucket, capturedKey string
	mock := &mockS3API{
		DeleteObjectFn: func(_ context.Context, params *s3.DeleteObjectInput, _ ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
			capturedBucket = aws.ToString(params.Bucket)
			capturedKey = aws.ToString(params.Key)
			return &s3.DeleteObjectOutput{}, nil
		},
	}

	tr := newTestTransport(mock)
	err := tr.Delete(context.Background(), "data/old.csv")
	if err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if capturedBucket != "staging-bucket" {
		t.Errorf("DeleteObject bucket = %q, want %q", capturedBucket, "staging-bucket")
	}
	if capturedKey != "data/old.csv" {
		t.Errorf("DeleteObject key = %q, want %q", capturedKey, "data/old.csv")
	}
}

func TestS3Transport_Delete_Error(t *testing.T) {
	t.Parallel()

	mock := &mockS3API{
		DeleteObjectFn: func(_ context.Context, _ *s3.DeleteObjectInput, _ ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
			return nil, errors.New("delete denied")
		},
	}

	tr := newTestTransport(mock)
	err := tr.Delete(context.Background(), "data/old.csv")
	if err == nil {
		t.Fatal("Delete() error = nil, want error")
	}
}

// --- Hold tests ---

func TestS3Transport_Hold_Happy(t *testing.T) {
	t.Parallel()

	until := time.Date(2025, 7, 1, 0, 0, 0, 0, time.UTC)
	var putCalls []s3.PutObjectInput
	var copyCalls []s3.CopyObjectInput
	var deleteCalls []s3.DeleteObjectInput

	mock := &mockS3API{
		PutObjectFn: func(_ context.Context, params *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
			putCalls = append(putCalls, *params)
			return &s3.PutObjectOutput{}, nil
		},
		CopyObjectFn: func(_ context.Context, params *s3.CopyObjectInput, _ ...func(*s3.Options)) (*s3.CopyObjectOutput, error) {
			copyCalls = append(copyCalls, *params)
			return &s3.CopyObjectOutput{}, nil
		},
		DeleteObjectFn: func(_ context.Context, params *s3.DeleteObjectInput, _ ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
			deleteCalls = append(deleteCalls, *params)
			return &s3.DeleteObjectOutput{}, nil
		},
	}

	tr := newTestTransport(mock)
	err := tr.Hold(context.Background(), "data/file.csv", until)
	if err != nil {
		t.Fatalf("Hold() error = %v", err)
	}

	// Step 1: .meta sidecar written to pipeline bucket
	if len(putCalls) != 1 {
		t.Fatalf("PutObject called %d times, want 1", len(putCalls))
	}
	put := putCalls[0]
	if aws.ToString(put.Bucket) != "pipeline-bucket" {
		t.Errorf("PutObject bucket = %q, want %q", aws.ToString(put.Bucket), "pipeline-bucket")
	}
	wantMetaKey := "chaos-hold/data/file.csv.meta"
	if aws.ToString(put.Key) != wantMetaKey {
		t.Errorf("PutObject key = %q, want %q", aws.ToString(put.Key), wantMetaKey)
	}
	// Verify meta content
	metaBody, _ := io.ReadAll(put.Body)
	var meta struct {
		ReleaseAt   string `json:"release_at"`
		OriginalKey string `json:"original_key"`
	}
	if err := json.Unmarshal(metaBody, &meta); err != nil {
		t.Fatalf("unmarshal meta: %v", err)
	}
	if meta.OriginalKey != "data/file.csv" {
		t.Errorf("meta.original_key = %q, want %q", meta.OriginalKey, "data/file.csv")
	}
	parsedTime, err := time.Parse(time.RFC3339, meta.ReleaseAt)
	if err != nil {
		t.Fatalf("parse release_at: %v", err)
	}
	if !parsedTime.Equal(until) {
		t.Errorf("meta.release_at = %v, want %v", parsedTime, until)
	}

	// Step 2: CopyObject from staging to pipeline hold prefix
	if len(copyCalls) != 1 {
		t.Fatalf("CopyObject called %d times, want 1", len(copyCalls))
	}
	cp := copyCalls[0]
	if aws.ToString(cp.Bucket) != "pipeline-bucket" {
		t.Errorf("CopyObject dest bucket = %q, want %q", aws.ToString(cp.Bucket), "pipeline-bucket")
	}
	wantCopyKey := "chaos-hold/data/file.csv"
	if aws.ToString(cp.Key) != wantCopyKey {
		t.Errorf("CopyObject dest key = %q, want %q", aws.ToString(cp.Key), wantCopyKey)
	}
	wantCopySource := "staging-bucket/data/file.csv"
	if aws.ToString(cp.CopySource) != wantCopySource {
		t.Errorf("CopyObject source = %q, want %q", aws.ToString(cp.CopySource), wantCopySource)
	}

	// Step 3: DeleteObject from staging
	if len(deleteCalls) != 1 {
		t.Fatalf("DeleteObject called %d times, want 1", len(deleteCalls))
	}
	del := deleteCalls[0]
	if aws.ToString(del.Bucket) != "staging-bucket" {
		t.Errorf("DeleteObject bucket = %q, want %q", aws.ToString(del.Bucket), "staging-bucket")
	}
	if aws.ToString(del.Key) != "data/file.csv" {
		t.Errorf("DeleteObject key = %q, want %q", aws.ToString(del.Key), "data/file.csv")
	}
}

func TestS3Transport_Hold_MetaWriteFails(t *testing.T) {
	t.Parallel()

	copyCallCount := 0
	deleteCallCount := 0
	mock := &mockS3API{
		PutObjectFn: func(_ context.Context, _ *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
			return nil, errors.New("put failed")
		},
		CopyObjectFn: func(_ context.Context, _ *s3.CopyObjectInput, _ ...func(*s3.Options)) (*s3.CopyObjectOutput, error) {
			copyCallCount++
			return &s3.CopyObjectOutput{}, nil
		},
		DeleteObjectFn: func(_ context.Context, _ *s3.DeleteObjectInput, _ ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
			deleteCallCount++
			return &s3.DeleteObjectOutput{}, nil
		},
	}

	tr := newTestTransport(mock)
	err := tr.Hold(context.Background(), "data/file.csv", time.Now().Add(time.Hour))
	if err == nil {
		t.Fatal("Hold() error = nil, want error")
	}
	if copyCallCount != 0 {
		t.Errorf("CopyObject called %d times, want 0 (no copy when meta write fails)", copyCallCount)
	}
	if deleteCallCount != 0 {
		t.Errorf("DeleteObject called %d times, want 0 (no delete when meta write fails)", deleteCallCount)
	}
}

func TestS3Transport_Hold_CopyFails_CleansUpMeta(t *testing.T) {
	t.Parallel()

	var deleteCalls []s3.DeleteObjectInput
	mock := &mockS3API{
		PutObjectFn: func(_ context.Context, _ *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
			return &s3.PutObjectOutput{}, nil
		},
		CopyObjectFn: func(_ context.Context, _ *s3.CopyObjectInput, _ ...func(*s3.Options)) (*s3.CopyObjectOutput, error) {
			return nil, errors.New("copy failed")
		},
		DeleteObjectFn: func(_ context.Context, params *s3.DeleteObjectInput, _ ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
			deleteCalls = append(deleteCalls, *params)
			return &s3.DeleteObjectOutput{}, nil
		},
	}

	tr := newTestTransport(mock)
	err := tr.Hold(context.Background(), "data/file.csv", time.Now().Add(time.Hour))
	if err == nil {
		t.Fatal("Hold() error = nil, want error")
	}

	// Should have cleaned up the .meta sidecar
	if len(deleteCalls) != 1 {
		t.Fatalf("DeleteObject called %d times, want 1 (cleanup .meta)", len(deleteCalls))
	}
	del := deleteCalls[0]
	if aws.ToString(del.Bucket) != "pipeline-bucket" {
		t.Errorf("cleanup DeleteObject bucket = %q, want %q", aws.ToString(del.Bucket), "pipeline-bucket")
	}
	wantMetaKey := "chaos-hold/data/file.csv.meta"
	if aws.ToString(del.Key) != wantMetaKey {
		t.Errorf("cleanup DeleteObject key = %q, want %q", aws.ToString(del.Key), wantMetaKey)
	}
}

func TestS3Transport_Hold_DeleteSourceFails(t *testing.T) {
	t.Parallel()

	mock := &mockS3API{
		PutObjectFn: func(_ context.Context, _ *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
			return &s3.PutObjectOutput{}, nil
		},
		CopyObjectFn: func(_ context.Context, _ *s3.CopyObjectInput, _ ...func(*s3.Options)) (*s3.CopyObjectOutput, error) {
			return &s3.CopyObjectOutput{}, nil
		},
		DeleteObjectFn: func(_ context.Context, _ *s3.DeleteObjectInput, _ ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
			return nil, errors.New("delete source failed")
		},
	}

	tr := newTestTransport(mock)
	err := tr.Hold(context.Background(), "data/file.csv", time.Now().Add(time.Hour))
	if err == nil {
		t.Fatal("Hold() error = nil, want error when delete source fails")
	}
	// Data is safe in hold even though source delete failed — caller
	// should be alerted via the returned error.
}

// --- Release tests ---

func TestS3Transport_Release_Happy(t *testing.T) {
	t.Parallel()

	var copyCalls []s3.CopyObjectInput
	var deleteCalls []s3.DeleteObjectInput

	mock := &mockS3API{
		GetObjectFn: func(_ context.Context, params *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
			// Return .meta content with original_key
			meta := `{"release_at":"2025-07-01T00:00:00Z","original_key":"data/file.csv"}`
			return &s3.GetObjectOutput{
				Body: io.NopCloser(strings.NewReader(meta)),
			}, nil
		},
		CopyObjectFn: func(_ context.Context, params *s3.CopyObjectInput, _ ...func(*s3.Options)) (*s3.CopyObjectOutput, error) {
			copyCalls = append(copyCalls, *params)
			return &s3.CopyObjectOutput{}, nil
		},
		DeleteObjectFn: func(_ context.Context, params *s3.DeleteObjectInput, _ ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
			deleteCalls = append(deleteCalls, *params)
			return &s3.DeleteObjectOutput{}, nil
		},
	}

	tr := newTestTransport(mock)
	err := tr.Release(context.Background(), "data/file.csv")
	if err != nil {
		t.Fatalf("Release() error = %v", err)
	}

	// Step 2: CopyObject from hold to original key in pipeline bucket
	if len(copyCalls) != 1 {
		t.Fatalf("CopyObject called %d times, want 1", len(copyCalls))
	}
	cp := copyCalls[0]
	if aws.ToString(cp.Bucket) != "pipeline-bucket" {
		t.Errorf("CopyObject dest bucket = %q, want %q", aws.ToString(cp.Bucket), "pipeline-bucket")
	}
	if aws.ToString(cp.Key) != "data/file.csv" {
		t.Errorf("CopyObject dest key = %q, want %q", aws.ToString(cp.Key), "data/file.csv")
	}
	wantCopySource := "pipeline-bucket/chaos-hold/data/file.csv"
	if aws.ToString(cp.CopySource) != wantCopySource {
		t.Errorf("CopyObject source = %q, want %q", aws.ToString(cp.CopySource), wantCopySource)
	}

	// Step 3: Delete held object and .meta sidecar
	if len(deleteCalls) != 2 {
		t.Fatalf("DeleteObject called %d times, want 2", len(deleteCalls))
	}
	// Both deletes should target pipeline-bucket
	for i, del := range deleteCalls {
		if aws.ToString(del.Bucket) != "pipeline-bucket" {
			t.Errorf("deleteCalls[%d] bucket = %q, want %q", i, aws.ToString(del.Bucket), "pipeline-bucket")
		}
	}
	// Check that one is the held object and one is the .meta
	deletedKeys := make(map[string]bool)
	for _, del := range deleteCalls {
		deletedKeys[aws.ToString(del.Key)] = true
	}
	if !deletedKeys["chaos-hold/data/file.csv"] {
		t.Error("held object not deleted")
	}
	if !deletedKeys["chaos-hold/data/file.csv.meta"] {
		t.Error(".meta sidecar not deleted")
	}
}

func TestS3Transport_Release_MetaMissing(t *testing.T) {
	t.Parallel()

	var copyCalls []s3.CopyObjectInput
	var deleteCalls []s3.DeleteObjectInput

	mock := &mockS3API{
		GetObjectFn: func(_ context.Context, _ *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
			return nil, errors.New("NoSuchKey: no such key")
		},
		CopyObjectFn: func(_ context.Context, params *s3.CopyObjectInput, _ ...func(*s3.Options)) (*s3.CopyObjectOutput, error) {
			copyCalls = append(copyCalls, *params)
			return &s3.CopyObjectOutput{}, nil
		},
		DeleteObjectFn: func(_ context.Context, params *s3.DeleteObjectInput, _ ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
			deleteCalls = append(deleteCalls, *params)
			return &s3.DeleteObjectOutput{}, nil
		},
	}

	tr := newTestTransport(mock)
	err := tr.Release(context.Background(), "data/file.csv")
	if err != nil {
		t.Fatalf("Release() error = %v, want nil for missing .meta", err)
	}

	// Should use key directly as destination
	if len(copyCalls) != 1 {
		t.Fatalf("CopyObject called %d times, want 1", len(copyCalls))
	}
	if aws.ToString(copyCalls[0].Key) != "data/file.csv" {
		t.Errorf("CopyObject dest key = %q, want %q", aws.ToString(copyCalls[0].Key), "data/file.csv")
	}
}

// --- ListHeld tests ---

func TestS3Transport_ListHeld_Empty(t *testing.T) {
	t.Parallel()

	mock := &mockS3API{
		ListObjectsV2Fn: func(_ context.Context, params *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
			if aws.ToString(params.Bucket) != "pipeline-bucket" {
				t.Errorf("Bucket = %q, want %q", aws.ToString(params.Bucket), "pipeline-bucket")
			}
			if aws.ToString(params.Prefix) != "chaos-hold/" {
				t.Errorf("Prefix = %q, want %q", aws.ToString(params.Prefix), "chaos-hold/")
			}
			return &s3.ListObjectsV2Output{
				Contents: nil,
			}, nil
		},
	}

	tr := newTestTransport(mock)
	objs, err := tr.ListHeld(context.Background())
	if err != nil {
		t.Fatalf("ListHeld() error = %v, want nil", err)
	}
	if len(objs) != 0 {
		t.Errorf("ListHeld() returned %d objects, want 0", len(objs))
	}
}

func TestS3Transport_ListHeld_WithHeldObjects(t *testing.T) {
	t.Parallel()

	now := time.Date(2025, 6, 15, 12, 0, 0, 0, time.UTC)
	mock := &mockS3API{
		ListObjectsV2Fn: func(_ context.Context, _ *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
			return &s3.ListObjectsV2Output{
				Contents: []s3types.Object{
					{Key: aws.String("chaos-hold/data1.csv"), Size: aws.Int64(100), LastModified: &now},
					{Key: aws.String("chaos-hold/data1.csv.meta"), Size: aws.Int64(50), LastModified: &now},
					{Key: aws.String("chaos-hold/data2.csv"), Size: aws.Int64(200), LastModified: &now},
					{Key: aws.String("chaos-hold/data2.csv.meta"), Size: aws.Int64(50), LastModified: &now},
				},
			}, nil
		},
	}

	tr := newTestTransport(mock)
	objs, err := tr.ListHeld(context.Background())
	if err != nil {
		t.Fatalf("ListHeld() error = %v", err)
	}
	if len(objs) != 2 {
		t.Fatalf("ListHeld() returned %d objects, want 2", len(objs))
	}

	// Keys should have the hold prefix stripped.
	if objs[0].Key != "data1.csv" {
		t.Errorf("objs[0].Key = %q, want %q", objs[0].Key, "data1.csv")
	}
	if objs[0].Size != 100 {
		t.Errorf("objs[0].Size = %d, want 100", objs[0].Size)
	}
	if !objs[0].LastModified.Equal(now) {
		t.Errorf("objs[0].LastModified = %v, want %v", objs[0].LastModified, now)
	}
	if objs[1].Key != "data2.csv" {
		t.Errorf("objs[1].Key = %q, want %q", objs[1].Key, "data2.csv")
	}
	if objs[1].Size != 200 {
		t.Errorf("objs[1].Size = %d, want 200", objs[1].Size)
	}
}

func TestS3Transport_ListHeld_Pagination(t *testing.T) {
	t.Parallel()

	callCount := 0
	mock := &mockS3API{
		ListObjectsV2Fn: func(_ context.Context, params *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
			callCount++
			if callCount == 1 {
				return &s3.ListObjectsV2Output{
					Contents: []s3types.Object{
						{Key: aws.String("chaos-hold/page1.csv")},
					},
					IsTruncated:           aws.Bool(true),
					NextContinuationToken: aws.String("token-xyz"),
				}, nil
			}
			if aws.ToString(params.ContinuationToken) != "token-xyz" {
				t.Errorf("ContinuationToken = %q, want %q", aws.ToString(params.ContinuationToken), "token-xyz")
			}
			return &s3.ListObjectsV2Output{
				Contents: []s3types.Object{
					{Key: aws.String("chaos-hold/page2.csv")},
				},
			}, nil
		},
	}

	tr := newTestTransport(mock)
	objs, err := tr.ListHeld(context.Background())
	if err != nil {
		t.Fatalf("ListHeld() error = %v", err)
	}
	if len(objs) != 2 {
		t.Fatalf("ListHeld() returned %d objects, want 2", len(objs))
	}
	if callCount != 2 {
		t.Errorf("ListObjectsV2 called %d times, want 2", callCount)
	}
	if objs[0].Key != "page1.csv" {
		t.Errorf("objs[0].Key = %q, want %q", objs[0].Key, "page1.csv")
	}
	if objs[1].Key != "page2.csv" {
		t.Errorf("objs[1].Key = %q, want %q", objs[1].Key, "page2.csv")
	}
}

func TestS3Transport_ListHeld_Error(t *testing.T) {
	t.Parallel()

	mock := &mockS3API{
		ListObjectsV2Fn: func(_ context.Context, _ *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
			return nil, errors.New("access denied")
		},
	}

	tr := newTestTransport(mock)
	_, err := tr.ListHeld(context.Background())
	if err == nil {
		t.Fatal("ListHeld() error = nil, want error")
	}
}

// --- ReleaseAll tests ---

func TestS3Transport_ReleaseAll_EmptyHold(t *testing.T) {
	t.Parallel()

	mock := &mockS3API{
		ListObjectsV2Fn: func(_ context.Context, params *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
			if aws.ToString(params.Bucket) != "pipeline-bucket" {
				t.Errorf("ListObjectsV2 bucket = %q, want %q", aws.ToString(params.Bucket), "pipeline-bucket")
			}
			if aws.ToString(params.Prefix) != "chaos-hold/" {
				t.Errorf("ListObjectsV2 prefix = %q, want %q", aws.ToString(params.Prefix), "chaos-hold/")
			}
			return &s3.ListObjectsV2Output{Contents: nil}, nil
		},
	}

	tr := newTestTransport(mock)
	if err := tr.ReleaseAll(context.Background()); err != nil {
		t.Fatalf("ReleaseAll() error = %v, want nil for empty hold", err)
	}
}

func TestS3Transport_ReleaseAll_ReleasesAll(t *testing.T) {
	t.Parallel()

	var releasedKeys []string

	mock := &mockS3API{
		// ListObjectsV2 returns 2 held objects (plus their .meta sidecars, which
		// should be excluded from the keys passed to Release).
		ListObjectsV2Fn: func(_ context.Context, _ *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
			return &s3.ListObjectsV2Output{
				Contents: []s3types.Object{
					{Key: aws.String("chaos-hold/file1.csv")},
					{Key: aws.String("chaos-hold/file1.csv.meta")},
					{Key: aws.String("chaos-hold/file2.csv")},
					{Key: aws.String("chaos-hold/file2.csv.meta")},
				},
			}, nil
		},
		// GetObject is called by Release to read the .meta sidecar.
		GetObjectFn: func(_ context.Context, params *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
			key := aws.ToString(params.Key)
			// Return a valid meta for each sidecar lookup.
			switch key {
			case "chaos-hold/file1.csv.meta":
				return &s3.GetObjectOutput{
					Body: io.NopCloser(strings.NewReader(`{"release_at":"2025-07-01T00:00:00Z","original_key":"file1.csv"}`)),
				}, nil
			case "chaos-hold/file2.csv.meta":
				return &s3.GetObjectOutput{
					Body: io.NopCloser(strings.NewReader(`{"release_at":"2025-07-01T00:00:00Z","original_key":"file2.csv"}`)),
				}, nil
			}
			return nil, errors.New("no such key")
		},
		// CopyObject is called by Release to move object to destination.
		CopyObjectFn: func(_ context.Context, params *s3.CopyObjectInput, _ ...func(*s3.Options)) (*s3.CopyObjectOutput, error) {
			releasedKeys = append(releasedKeys, aws.ToString(params.Key))
			return &s3.CopyObjectOutput{}, nil
		},
		// DeleteObject is called by Release to remove held object and .meta.
		DeleteObjectFn: func(_ context.Context, _ *s3.DeleteObjectInput, _ ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
			return &s3.DeleteObjectOutput{}, nil
		},
	}

	tr := newTestTransport(mock)
	if err := tr.ReleaseAll(context.Background()); err != nil {
		t.Fatalf("ReleaseAll() error = %v", err)
	}

	// Both data files should have been released (CopyObject called for each).
	if len(releasedKeys) != 2 {
		t.Fatalf("expected 2 release copy calls, got %d: %v", len(releasedKeys), releasedKeys)
	}

	releasedSet := make(map[string]bool, len(releasedKeys))
	for _, k := range releasedKeys {
		releasedSet[k] = true
	}
	for _, want := range []string{"file1.csv", "file2.csv"} {
		if !releasedSet[want] {
			t.Errorf("ReleaseAll() did not release %q", want)
		}
	}
}

func TestS3Transport_Release_CopyFails(t *testing.T) {
	t.Parallel()

	deleteCallCount := 0
	mock := &mockS3API{
		GetObjectFn: func(_ context.Context, _ *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
			meta := `{"release_at":"2025-07-01T00:00:00Z","original_key":"data/file.csv"}`
			return &s3.GetObjectOutput{
				Body: io.NopCloser(strings.NewReader(meta)),
			}, nil
		},
		CopyObjectFn: func(_ context.Context, _ *s3.CopyObjectInput, _ ...func(*s3.Options)) (*s3.CopyObjectOutput, error) {
			return nil, errors.New("copy failed")
		},
		DeleteObjectFn: func(_ context.Context, _ *s3.DeleteObjectInput, _ ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
			deleteCallCount++
			return &s3.DeleteObjectOutput{}, nil
		},
	}

	tr := newTestTransport(mock)
	err := tr.Release(context.Background(), "data/file.csv")
	if err == nil {
		t.Fatal("Release() error = nil, want error")
	}
	if deleteCallCount != 0 {
		t.Errorf("DeleteObject called %d times, want 0 (held object should not be deleted on copy failure)", deleteCallCount)
	}
}

// --- HoldData tests ---

func TestS3Transport_HoldData_Happy(t *testing.T) {
	t.Parallel()

	until := time.Date(2025, 7, 1, 0, 0, 0, 0, time.UTC)
	var putCalls []s3.PutObjectInput
	var putBodies [][]byte

	mock := &mockS3API{
		PutObjectFn: func(_ context.Context, params *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
			// Capture the body before it is consumed.
			var body []byte
			if params.Body != nil {
				body, _ = io.ReadAll(params.Body)
			}
			putBodies = append(putBodies, body)
			putCalls = append(putCalls, *params)
			return &s3.PutObjectOutput{}, nil
		},
	}

	tr := newTestTransport(mock)
	data := bytes.NewReader([]byte("held payload"))
	err := tr.HoldData(context.Background(), "data/file.csv", data, until)
	if err != nil {
		t.Fatalf("HoldData() error = %v, want nil", err)
	}

	// Expect exactly 2 PutObject calls: data object + .meta sidecar.
	if len(putCalls) != 2 {
		t.Fatalf("PutObject called %d times, want 2", len(putCalls))
	}

	// First call: data object at {holdPrefix}{key} in pipeline-bucket.
	dataPut := putCalls[0]
	if aws.ToString(dataPut.Bucket) != "pipeline-bucket" {
		t.Errorf("data PutObject bucket = %q, want %q", aws.ToString(dataPut.Bucket), "pipeline-bucket")
	}
	wantDataKey := "chaos-hold/data/file.csv"
	if aws.ToString(dataPut.Key) != wantDataKey {
		t.Errorf("data PutObject key = %q, want %q", aws.ToString(dataPut.Key), wantDataKey)
	}
	if string(putBodies[0]) != "held payload" {
		t.Errorf("data PutObject body = %q, want %q", string(putBodies[0]), "held payload")
	}

	// Second call: .meta sidecar at {holdPrefix}{key}.meta in pipeline-bucket.
	metaPut := putCalls[1]
	if aws.ToString(metaPut.Bucket) != "pipeline-bucket" {
		t.Errorf("meta PutObject bucket = %q, want %q", aws.ToString(metaPut.Bucket), "pipeline-bucket")
	}
	wantMetaKey := "chaos-hold/data/file.csv.meta"
	if aws.ToString(metaPut.Key) != wantMetaKey {
		t.Errorf("meta PutObject key = %q, want %q", aws.ToString(metaPut.Key), wantMetaKey)
	}

	// Verify .meta content is valid JSON with correct fields.
	var meta struct {
		ReleaseAt   string `json:"release_at"`
		OriginalKey string `json:"original_key"`
	}
	if err := json.Unmarshal(putBodies[1], &meta); err != nil {
		t.Fatalf("unmarshal .meta body: %v", err)
	}
	if meta.OriginalKey != "data/file.csv" {
		t.Errorf("meta.original_key = %q, want %q", meta.OriginalKey, "data/file.csv")
	}
	parsedTime, err := time.Parse(time.RFC3339, meta.ReleaseAt)
	if err != nil {
		t.Fatalf("parse release_at %q: %v", meta.ReleaseAt, err)
	}
	if !parsedTime.Equal(until) {
		t.Errorf("meta.release_at = %v, want %v", parsedTime, until)
	}
}

func TestS3Transport_HoldData_DataWriteFails(t *testing.T) {
	t.Parallel()

	mock := &mockS3API{
		PutObjectFn: func(_ context.Context, _ *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
			return nil, errors.New("put data failed")
		},
	}

	tr := newTestTransport(mock)
	err := tr.HoldData(context.Background(), "data/file.csv", strings.NewReader("payload"), time.Now().Add(time.Hour))
	if err == nil {
		t.Fatal("HoldData() error = nil, want error when data write fails")
	}
}

func TestS3Transport_HoldData_SidecarWriteFails_CleansUpData(t *testing.T) {
	t.Parallel()

	putCallCount := 0
	var deleteCalls []s3.DeleteObjectInput

	mock := &mockS3API{
		PutObjectFn: func(_ context.Context, params *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
			putCallCount++
			// First call (data object) succeeds; second call (.meta sidecar) fails.
			if putCallCount == 1 {
				return &s3.PutObjectOutput{}, nil
			}
			return nil, errors.New("meta write failed")
		},
		DeleteObjectFn: func(_ context.Context, params *s3.DeleteObjectInput, _ ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
			deleteCalls = append(deleteCalls, *params)
			return &s3.DeleteObjectOutput{}, nil
		},
	}

	tr := newTestTransport(mock)
	err := tr.HoldData(context.Background(), "data/file.csv", strings.NewReader("payload"), time.Now().Add(time.Hour))
	if err == nil {
		t.Fatal("HoldData() error = nil, want error when sidecar write fails")
	}

	// The data object should have been cleaned up via DeleteObject.
	if len(deleteCalls) != 1 {
		t.Fatalf("DeleteObject called %d times, want 1 (cleanup data object)", len(deleteCalls))
	}
	del := deleteCalls[0]
	if aws.ToString(del.Bucket) != "pipeline-bucket" {
		t.Errorf("cleanup DeleteObject bucket = %q, want %q", aws.ToString(del.Bucket), "pipeline-bucket")
	}
	wantDataKey := "chaos-hold/data/file.csv"
	if aws.ToString(del.Key) != wantDataKey {
		t.Errorf("cleanup DeleteObject key = %q, want %q", aws.ToString(del.Key), wantDataKey)
	}
}

// --- ListHeld HeldUntil population tests ---

func TestS3Transport_ListHeld_PopulatesHeldUntil(t *testing.T) {
	t.Parallel()

	now := time.Date(2025, 6, 15, 12, 0, 0, 0, time.UTC)
	releaseAt := time.Date(2025, 7, 1, 0, 0, 0, 0, time.UTC)

	mock := &mockS3API{
		ListObjectsV2Fn: func(_ context.Context, _ *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
			return &s3.ListObjectsV2Output{
				Contents: []s3types.Object{
					{Key: aws.String("chaos-hold/data/file.csv"), Size: aws.Int64(100), LastModified: &now},
					{Key: aws.String("chaos-hold/data/file.csv.meta"), Size: aws.Int64(50), LastModified: &now},
				},
			}, nil
		},
		GetObjectFn: func(_ context.Context, params *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
			wantKey := "chaos-hold/data/file.csv.meta"
			if aws.ToString(params.Key) != wantKey {
				t.Errorf("GetObject key = %q, want %q", aws.ToString(params.Key), wantKey)
			}
			if aws.ToString(params.Bucket) != "pipeline-bucket" {
				t.Errorf("GetObject bucket = %q, want %q", aws.ToString(params.Bucket), "pipeline-bucket")
			}
			meta := `{"release_at":"2025-07-01T00:00:00Z","original_key":"data/file.csv"}`
			return &s3.GetObjectOutput{
				Body: io.NopCloser(strings.NewReader(meta)),
			}, nil
		},
	}

	tr := newTestTransport(mock)
	objs, err := tr.ListHeld(context.Background())
	if err != nil {
		t.Fatalf("ListHeld() error = %v, want nil", err)
	}
	if len(objs) != 1 {
		t.Fatalf("ListHeld() returned %d objects, want 1", len(objs))
	}
	if !objs[0].HeldUntil.Equal(releaseAt) {
		t.Errorf("objs[0].HeldUntil = %v, want %v", objs[0].HeldUntil, releaseAt)
	}
}

func TestS3Transport_ListHeld_MissingSidecar_ZeroHeldUntil(t *testing.T) {
	t.Parallel()

	now := time.Date(2025, 6, 15, 12, 0, 0, 0, time.UTC)
	mock := &mockS3API{
		ListObjectsV2Fn: func(_ context.Context, _ *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
			return &s3.ListObjectsV2Output{
				Contents: []s3types.Object{
					{Key: aws.String("chaos-hold/data/file.csv"), Size: aws.Int64(100), LastModified: &now},
				},
			}, nil
		},
		GetObjectFn: func(_ context.Context, _ *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
			return nil, errors.New("NoSuchKey: no such key")
		},
	}

	tr := newTestTransport(mock)
	objs, err := tr.ListHeld(context.Background())
	if err != nil {
		t.Fatalf("ListHeld() error = %v, want nil", err)
	}
	if len(objs) != 1 {
		t.Fatalf("ListHeld() returned %d objects, want 1", len(objs))
	}
	if !objs[0].HeldUntil.IsZero() {
		t.Errorf("objs[0].HeldUntil = %v, want zero time", objs[0].HeldUntil)
	}
}

func TestS3Transport_ListHeld_BadJSON_ZeroHeldUntil(t *testing.T) {
	t.Parallel()

	now := time.Date(2025, 6, 15, 12, 0, 0, 0, time.UTC)
	mock := &mockS3API{
		ListObjectsV2Fn: func(_ context.Context, _ *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
			return &s3.ListObjectsV2Output{
				Contents: []s3types.Object{
					{Key: aws.String("chaos-hold/data/file.csv"), Size: aws.Int64(100), LastModified: &now},
					{Key: aws.String("chaos-hold/data/file.csv.meta"), Size: aws.Int64(10), LastModified: &now},
				},
			}, nil
		},
		GetObjectFn: func(_ context.Context, _ *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
			return &s3.GetObjectOutput{
				Body: io.NopCloser(strings.NewReader("not json")),
			}, nil
		},
	}

	tr := newTestTransport(mock)
	objs, err := tr.ListHeld(context.Background())
	if err != nil {
		t.Fatalf("ListHeld() error = %v, want nil", err)
	}
	if len(objs) != 1 {
		t.Fatalf("ListHeld() returned %d objects, want 1", len(objs))
	}
	if !objs[0].HeldUntil.IsZero() {
		t.Errorf("objs[0].HeldUntil = %v, want zero time", objs[0].HeldUntil)
	}
}

func TestS3Transport_ListHeld_BadTimestamp_ZeroHeldUntil(t *testing.T) {
	t.Parallel()

	now := time.Date(2025, 6, 15, 12, 0, 0, 0, time.UTC)
	mock := &mockS3API{
		ListObjectsV2Fn: func(_ context.Context, _ *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
			return &s3.ListObjectsV2Output{
				Contents: []s3types.Object{
					{Key: aws.String("chaos-hold/data/file.csv"), Size: aws.Int64(100), LastModified: &now},
					{Key: aws.String("chaos-hold/data/file.csv.meta"), Size: aws.Int64(50), LastModified: &now},
				},
			}, nil
		},
		GetObjectFn: func(_ context.Context, _ *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
			meta := `{"release_at":"not-a-date","original_key":"data/file.csv"}`
			return &s3.GetObjectOutput{
				Body: io.NopCloser(strings.NewReader(meta)),
			}, nil
		},
	}

	tr := newTestTransport(mock)
	objs, err := tr.ListHeld(context.Background())
	if err != nil {
		t.Fatalf("ListHeld() error = %v, want nil", err)
	}
	if len(objs) != 1 {
		t.Fatalf("ListHeld() returned %d objects, want 1", len(objs))
	}
	if !objs[0].HeldUntil.IsZero() {
		t.Errorf("objs[0].HeldUntil = %v, want zero time", objs[0].HeldUntil)
	}
}
