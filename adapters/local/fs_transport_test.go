package local_test

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dwsmith1983/chaos-data/adapters/local"
)

func TestFSTransport_List(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		files    []string // files to create in stagingDir
		prefix   string
		wantKeys []string
	}{
		{
			name:     "empty directory",
			files:    nil,
			prefix:   "",
			wantKeys: nil,
		},
		{
			name:     "all files no prefix",
			files:    []string{"a.csv", "b.csv", "c.txt"},
			prefix:   "",
			wantKeys: []string{"a.csv", "b.csv", "c.txt"},
		},
		{
			name:     "prefix filter",
			files:    []string{"data-001.csv", "data-002.csv", "report.csv"},
			prefix:   "data-",
			wantKeys: []string{"data-001.csv", "data-002.csv"},
		},
		{
			name:     "prefix matches none",
			files:    []string{"a.csv", "b.csv"},
			prefix:   "z-",
			wantKeys: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			stagingDir := t.TempDir()
			outputDir := t.TempDir()

			for _, f := range tt.files {
				if err := os.WriteFile(filepath.Join(stagingDir, f), []byte("content"), 0o644); err != nil {
					t.Fatalf("setup: write file %s: %v", f, err)
				}
			}

			tr := local.NewFSTransport(stagingDir, outputDir)
			objs, err := tr.List(context.Background(), tt.prefix)
			if err != nil {
				t.Fatalf("List() error = %v", err)
			}

			gotKeys := make([]string, len(objs))
			for i, o := range objs {
				gotKeys[i] = o.Key
			}

			if len(gotKeys) != len(tt.wantKeys) {
				t.Fatalf("List() returned %d keys %v, want %d keys %v", len(gotKeys), gotKeys, len(tt.wantKeys), tt.wantKeys)
			}

			wantSet := make(map[string]bool, len(tt.wantKeys))
			for _, k := range tt.wantKeys {
				wantSet[k] = true
			}
			for _, k := range gotKeys {
				if !wantSet[k] {
					t.Errorf("List() returned unexpected key %q", k)
				}
			}
		})
	}
}

func TestFSTransport_List_PopulatesDataObject(t *testing.T) {
	t.Parallel()
	stagingDir := t.TempDir()
	outputDir := t.TempDir()

	content := []byte("hello world")
	fpath := filepath.Join(stagingDir, "test.csv")
	if err := os.WriteFile(fpath, content, 0o644); err != nil {
		t.Fatalf("setup: %v", err)
	}

	tr := local.NewFSTransport(stagingDir, outputDir)
	objs, err := tr.List(context.Background(), "")
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if len(objs) != 1 {
		t.Fatalf("List() returned %d objects, want 1", len(objs))
	}

	obj := objs[0]
	if obj.Key != "test.csv" {
		t.Errorf("Key = %q, want %q", obj.Key, "test.csv")
	}
	if obj.Size != int64(len(content)) {
		t.Errorf("Size = %d, want %d", obj.Size, len(content))
	}
	if obj.LastModified.IsZero() {
		t.Error("LastModified is zero, want non-zero")
	}
}

func TestFSTransport_List_ExcludesHeldFiles(t *testing.T) {
	t.Parallel()
	stagingDir := t.TempDir()
	outputDir := t.TempDir()

	// Create a normal file and a file that will be held.
	for _, name := range []string{"visible.csv", "to-hold.csv"} {
		if err := os.WriteFile(filepath.Join(stagingDir, name), []byte("data"), 0o644); err != nil {
			t.Fatalf("setup: %v", err)
		}
	}

	tr := local.NewFSTransport(stagingDir, outputDir)

	// Hold one file.
	if err := tr.Hold(context.Background(), "to-hold.csv", time.Now().Add(time.Hour)); err != nil {
		t.Fatalf("Hold() error = %v", err)
	}

	// List should only return the non-held file.
	objs, err := tr.List(context.Background(), "")
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}

	for _, o := range objs {
		if o.Key == "to-hold.csv" {
			t.Error("List() returned held file 'to-hold.csv', want it excluded")
		}
	}
	if len(objs) != 1 {
		t.Fatalf("List() returned %d objects, want 1", len(objs))
	}
	if objs[0].Key != "visible.csv" {
		t.Errorf("List() returned %q, want 'visible.csv'", objs[0].Key)
	}
}

func TestFSTransport_Read(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		key     string
		content string
		setup   bool // whether to create the file
		wantErr bool
	}{
		{
			name:    "read existing file",
			key:     "data.csv",
			content: "col1,col2\na,b\n",
			setup:   true,
			wantErr: false,
		},
		{
			name:    "read nonexistent file",
			key:     "missing.csv",
			content: "",
			setup:   false,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			stagingDir := t.TempDir()
			outputDir := t.TempDir()

			if tt.setup {
				if err := os.WriteFile(filepath.Join(stagingDir, tt.key), []byte(tt.content), 0o644); err != nil {
					t.Fatalf("setup: %v", err)
				}
			}

			tr := local.NewFSTransport(stagingDir, outputDir)
			rc, err := tr.Read(context.Background(), tt.key)
			if tt.wantErr {
				if err == nil {
					rc.Close()
					t.Fatal("Read() error = nil, want error")
				}
				return
			}
			if err != nil {
				t.Fatalf("Read() error = %v", err)
			}
			defer rc.Close()

			got, err := io.ReadAll(rc)
			if err != nil {
				t.Fatalf("ReadAll() error = %v", err)
			}
			if string(got) != tt.content {
				t.Errorf("Read() content = %q, want %q", string(got), tt.content)
			}
		})
	}
}

func TestFSTransport_Write(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		key     string
		content string
	}{
		{
			name:    "write simple file",
			key:     "output.csv",
			content: "result data",
		},
		{
			name:    "write to subdirectory",
			key:     "sub/dir/output.csv",
			content: "nested result",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			stagingDir := t.TempDir()
			outputDir := t.TempDir()

			tr := local.NewFSTransport(stagingDir, outputDir)
			err := tr.Write(context.Background(), tt.key, strings.NewReader(tt.content))
			if err != nil {
				t.Fatalf("Write() error = %v", err)
			}

			got, err := os.ReadFile(filepath.Join(outputDir, tt.key))
			if err != nil {
				t.Fatalf("read written file: %v", err)
			}
			if string(got) != tt.content {
				t.Errorf("written content = %q, want %q", string(got), tt.content)
			}
		})
	}
}

func TestFSTransport_Delete(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		key     string
		setup   bool
		wantErr bool
	}{
		{
			name:    "delete existing file",
			key:     "to-delete.csv",
			setup:   true,
			wantErr: false,
		},
		{
			name:    "delete nonexistent file",
			key:     "missing.csv",
			setup:   false,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			stagingDir := t.TempDir()
			outputDir := t.TempDir()

			if tt.setup {
				if err := os.WriteFile(filepath.Join(stagingDir, tt.key), []byte("data"), 0o644); err != nil {
					t.Fatalf("setup: %v", err)
				}
			}

			tr := local.NewFSTransport(stagingDir, outputDir)
			err := tr.Delete(context.Background(), tt.key)
			if tt.wantErr {
				if err == nil {
					t.Fatal("Delete() error = nil, want error")
				}
				return
			}
			if err != nil {
				t.Fatalf("Delete() error = %v", err)
			}

			if _, err := os.Stat(filepath.Join(stagingDir, tt.key)); !os.IsNotExist(err) {
				t.Error("file still exists after Delete()")
			}
		})
	}
}

func TestFSTransport_Hold(t *testing.T) {
	t.Parallel()
	stagingDir := t.TempDir()
	outputDir := t.TempDir()

	// Create a file to hold.
	key := "holdme.csv"
	if err := os.WriteFile(filepath.Join(stagingDir, key), []byte("hold data"), 0o644); err != nil {
		t.Fatalf("setup: %v", err)
	}

	tr := local.NewFSTransport(stagingDir, outputDir)
	until := time.Now().Add(1 * time.Hour)
	if err := tr.Hold(context.Background(), key, until); err != nil {
		t.Fatalf("Hold() error = %v", err)
	}

	// Original file should be gone from staging.
	if _, err := os.Stat(filepath.Join(stagingDir, key)); !os.IsNotExist(err) {
		t.Error("file still exists in stagingDir after Hold()")
	}

	// File should exist in holdDir.
	holdDir := filepath.Join(stagingDir, ".chaos-hold")
	heldData, err := os.ReadFile(filepath.Join(holdDir, key))
	if err != nil {
		t.Fatalf("held file not found: %v", err)
	}
	if string(heldData) != "hold data" {
		t.Errorf("held file content = %q, want %q", string(heldData), "hold data")
	}

	// Meta sidecar file should exist.
	metaData, err := os.ReadFile(filepath.Join(holdDir, key+".meta"))
	if err != nil {
		t.Fatalf("meta file not found: %v", err)
	}
	if len(metaData) == 0 {
		t.Error("meta file is empty")
	}
}

func TestFSTransport_Hold_MissingSource(t *testing.T) {
	t.Parallel()
	stagingDir := t.TempDir()
	outputDir := t.TempDir()

	tr := local.NewFSTransport(stagingDir, outputDir)
	err := tr.Hold(context.Background(), "nonexistent.csv", time.Now().Add(time.Hour))
	if err == nil {
		t.Fatal("Hold() error = nil, want error for missing source file")
	}

	// The .meta sidecar should have been cleaned up on rename failure.
	holdDir := filepath.Join(stagingDir, ".chaos-hold")
	metaPath := filepath.Join(holdDir, "nonexistent.csv.meta")
	if _, statErr := os.Stat(metaPath); statErr == nil {
		t.Error(".meta sidecar was not cleaned up after rename failure")
	}
}

func TestFSTransport_Hold_ReadOnlyHoldDir(t *testing.T) {
	t.Parallel()
	stagingDir := t.TempDir()
	outputDir := t.TempDir()

	// Create the hold directory as read-only so meta write fails.
	holdDir := filepath.Join(stagingDir, ".chaos-hold")
	if err := os.MkdirAll(holdDir, 0o755); err != nil {
		t.Fatalf("setup: %v", err)
	}
	if err := os.Chmod(holdDir, 0o555); err != nil {
		t.Fatalf("setup chmod: %v", err)
	}
	t.Cleanup(func() { os.Chmod(holdDir, 0o755) })

	// Create a source file.
	key := "blocked.csv"
	if err := os.WriteFile(filepath.Join(stagingDir, key), []byte("data"), 0o644); err != nil {
		t.Fatalf("setup: %v", err)
	}

	tr := local.NewFSTransport(stagingDir, outputDir)
	err := tr.Hold(context.Background(), key, time.Now().Add(time.Hour))
	if err == nil {
		t.Fatal("Hold() error = nil, want error when hold dir is read-only")
	}
}

func TestFSTransport_Release(t *testing.T) {
	t.Parallel()
	stagingDir := t.TempDir()
	outputDir := t.TempDir()

	// Create a file and hold it first.
	key := "release-me.csv"
	content := "release data"
	if err := os.WriteFile(filepath.Join(stagingDir, key), []byte(content), 0o644); err != nil {
		t.Fatalf("setup: %v", err)
	}

	tr := local.NewFSTransport(stagingDir, outputDir)
	until := time.Now().Add(1 * time.Hour)
	if err := tr.Hold(context.Background(), key, until); err != nil {
		t.Fatalf("Hold() error = %v", err)
	}

	// Now release it.
	if err := tr.Release(context.Background(), key); err != nil {
		t.Fatalf("Release() error = %v", err)
	}

	// File should be in outputDir.
	got, err := os.ReadFile(filepath.Join(outputDir, key))
	if err != nil {
		t.Fatalf("released file not found in outputDir: %v", err)
	}
	if string(got) != content {
		t.Errorf("released content = %q, want %q", string(got), content)
	}

	// File should be gone from holdDir.
	holdDir := filepath.Join(stagingDir, ".chaos-hold")
	if _, err := os.Stat(filepath.Join(holdDir, key)); !os.IsNotExist(err) {
		t.Error("file still in holdDir after Release()")
	}

	// Meta file should be gone.
	if _, err := os.Stat(filepath.Join(holdDir, key+".meta")); !os.IsNotExist(err) {
		t.Error("meta file still in holdDir after Release()")
	}
}

func TestFSTransport_Release_MissingMeta(t *testing.T) {
	t.Parallel()
	stagingDir := t.TempDir()
	outputDir := t.TempDir()

	// Simulate a held file without a .meta sidecar.
	holdDir := filepath.Join(stagingDir, ".chaos-hold")
	if err := os.MkdirAll(holdDir, 0o755); err != nil {
		t.Fatalf("setup: %v", err)
	}
	key := "no-meta.csv"
	if err := os.WriteFile(filepath.Join(holdDir, key), []byte("data"), 0o644); err != nil {
		t.Fatalf("setup: %v", err)
	}

	tr := local.NewFSTransport(stagingDir, outputDir)
	// Release should succeed even without .meta.
	if err := tr.Release(context.Background(), key); err != nil {
		t.Fatalf("Release() error = %v, want nil for missing .meta", err)
	}

	// File should be in outputDir.
	if _, err := os.ReadFile(filepath.Join(outputDir, key)); err != nil {
		t.Fatalf("released file not found: %v", err)
	}
}

func TestFSTransport_Release_MissingHeldFile(t *testing.T) {
	t.Parallel()
	stagingDir := t.TempDir()
	outputDir := t.TempDir()

	// Ensure hold dir exists but the file does not.
	holdDir := filepath.Join(stagingDir, ".chaos-hold")
	if err := os.MkdirAll(holdDir, 0o755); err != nil {
		t.Fatalf("setup: %v", err)
	}

	tr := local.NewFSTransport(stagingDir, outputDir)
	err := tr.Release(context.Background(), "nonexistent.csv")
	if err == nil {
		t.Fatal("Release() error = nil, want error for missing held file")
	}
}

func TestFSTransport_ListHeld_Empty(t *testing.T) {
	t.Parallel()
	stagingDir := t.TempDir()
	outputDir := t.TempDir()

	// Create an empty hold directory.
	holdDir := filepath.Join(stagingDir, ".chaos-hold")
	if err := os.MkdirAll(holdDir, 0o755); err != nil {
		t.Fatalf("setup: %v", err)
	}

	tr := local.NewFSTransport(stagingDir, outputDir)
	objs, err := tr.ListHeld(context.Background())
	if err != nil {
		t.Fatalf("ListHeld() error = %v", err)
	}
	if len(objs) != 0 {
		t.Errorf("ListHeld() returned %d objects, want 0", len(objs))
	}
}

func TestFSTransport_ListHeld_WithHeldFiles(t *testing.T) {
	t.Parallel()
	stagingDir := t.TempDir()
	outputDir := t.TempDir()

	// Create files in the hold directory.
	holdDir := filepath.Join(stagingDir, ".chaos-hold")
	if err := os.MkdirAll(holdDir, 0o755); err != nil {
		t.Fatalf("setup: %v", err)
	}
	for _, name := range []string{"data1.csv", "data2.csv"} {
		if err := os.WriteFile(filepath.Join(holdDir, name), []byte("held content"), 0o644); err != nil {
			t.Fatalf("setup: write %s: %v", name, err)
		}
	}

	tr := local.NewFSTransport(stagingDir, outputDir)
	objs, err := tr.ListHeld(context.Background())
	if err != nil {
		t.Fatalf("ListHeld() error = %v", err)
	}
	if len(objs) != 2 {
		t.Fatalf("ListHeld() returned %d objects, want 2", len(objs))
	}

	gotKeys := make(map[string]bool, len(objs))
	for _, o := range objs {
		gotKeys[o.Key] = true
		if o.Size != int64(len("held content")) {
			t.Errorf("object %q Size = %d, want %d", o.Key, o.Size, len("held content"))
		}
		if o.LastModified.IsZero() {
			t.Errorf("object %q LastModified is zero", o.Key)
		}
	}
	for _, want := range []string{"data1.csv", "data2.csv"} {
		if !gotKeys[want] {
			t.Errorf("ListHeld() missing expected key %q", want)
		}
	}
}

func TestFSTransport_ListHeld_ExcludesMeta(t *testing.T) {
	t.Parallel()
	stagingDir := t.TempDir()
	outputDir := t.TempDir()

	holdDir := filepath.Join(stagingDir, ".chaos-hold")
	if err := os.MkdirAll(holdDir, 0o755); err != nil {
		t.Fatalf("setup: %v", err)
	}

	// Create a data file and its .meta sidecar.
	if err := os.WriteFile(filepath.Join(holdDir, "data.csv"), []byte("data"), 0o644); err != nil {
		t.Fatalf("setup: write data.csv: %v", err)
	}
	if err := os.WriteFile(filepath.Join(holdDir, "data.csv.meta"), []byte(`{"release_at":"2025-07-01T00:00:00Z"}`), 0o644); err != nil {
		t.Fatalf("setup: write data.csv.meta: %v", err)
	}

	tr := local.NewFSTransport(stagingDir, outputDir)
	objs, err := tr.ListHeld(context.Background())
	if err != nil {
		t.Fatalf("ListHeld() error = %v", err)
	}
	if len(objs) != 1 {
		t.Fatalf("ListHeld() returned %d objects, want 1", len(objs))
	}
	if objs[0].Key != "data.csv" {
		t.Errorf("ListHeld() key = %q, want %q", objs[0].Key, "data.csv")
	}
}

func TestFSTransport_ListHeld_HoldDirAbsent(t *testing.T) {
	t.Parallel()
	stagingDir := t.TempDir()
	outputDir := t.TempDir()

	// Do NOT create the .chaos-hold directory — it should not exist.
	tr := local.NewFSTransport(stagingDir, outputDir)
	objs, err := tr.ListHeld(context.Background())
	if err != nil {
		t.Fatalf("ListHeld() error = %v, want nil for absent hold dir", err)
	}
	if len(objs) != 0 {
		t.Errorf("ListHeld() returned %d objects, want 0", len(objs))
	}
}

func TestFSTransport_PathTraversal(t *testing.T) {
	t.Parallel()

	maliciousKey := "../../etc/passwd"

	t.Run("Read rejects traversal", func(t *testing.T) {
		t.Parallel()
		stagingDir := t.TempDir()
		outputDir := t.TempDir()
		tr := local.NewFSTransport(stagingDir, outputDir)

		_, err := tr.Read(context.Background(), maliciousKey)
		if err == nil {
			t.Fatal("Read() error = nil, want path traversal error")
		}
		if !strings.Contains(err.Error(), "escapes root") {
			t.Errorf("Read() error = %v, want 'escapes root' message", err)
		}
	})

	t.Run("Write rejects traversal", func(t *testing.T) {
		t.Parallel()
		stagingDir := t.TempDir()
		outputDir := t.TempDir()
		tr := local.NewFSTransport(stagingDir, outputDir)

		err := tr.Write(context.Background(), maliciousKey, strings.NewReader("bad"))
		if err == nil {
			t.Fatal("Write() error = nil, want path traversal error")
		}
		if !strings.Contains(err.Error(), "escapes root") {
			t.Errorf("Write() error = %v, want 'escapes root' message", err)
		}
	})

	t.Run("Delete rejects traversal", func(t *testing.T) {
		t.Parallel()
		stagingDir := t.TempDir()
		outputDir := t.TempDir()
		tr := local.NewFSTransport(stagingDir, outputDir)

		err := tr.Delete(context.Background(), maliciousKey)
		if err == nil {
			t.Fatal("Delete() error = nil, want path traversal error")
		}
		if !strings.Contains(err.Error(), "escapes root") {
			t.Errorf("Delete() error = %v, want 'escapes root' message", err)
		}
	})

	t.Run("Hold rejects traversal", func(t *testing.T) {
		t.Parallel()
		stagingDir := t.TempDir()
		outputDir := t.TempDir()
		tr := local.NewFSTransport(stagingDir, outputDir)

		err := tr.Hold(context.Background(), maliciousKey, time.Now().Add(time.Hour))
		if err == nil {
			t.Fatal("Hold() error = nil, want path traversal error")
		}
		if !strings.Contains(err.Error(), "escapes root") {
			t.Errorf("Hold() error = %v, want 'escapes root' message", err)
		}
	})

	t.Run("Release rejects traversal", func(t *testing.T) {
		t.Parallel()
		stagingDir := t.TempDir()
		outputDir := t.TempDir()
		tr := local.NewFSTransport(stagingDir, outputDir)

		err := tr.Release(context.Background(), maliciousKey)
		if err == nil {
			t.Fatal("Release() error = nil, want path traversal error")
		}
		if !strings.Contains(err.Error(), "escapes root") {
			t.Errorf("Release() error = %v, want 'escapes root' message", err)
		}
	})
}
