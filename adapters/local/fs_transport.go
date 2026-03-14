package local

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// Compile-time interface assertion.
var _ adapter.DataTransport = (*FSTransport)(nil)

// FSTransport implements adapter.DataTransport using the local filesystem.
type FSTransport struct {
	stagingDir string
	outputDir  string
	holdDir    string
}

// NewFSTransport creates an FSTransport with the given staging and output
// directories. The hold directory defaults to stagingDir/.chaos-hold/.
func NewFSTransport(stagingDir, outputDir string) *FSTransport {
	return &FSTransport{
		stagingDir: stagingDir,
		outputDir:  outputDir,
		holdDir:    filepath.Join(stagingDir, ".chaos-hold"),
	}
}

// holdMeta stores metadata about a held object.
type holdMeta struct {
	ReleaseAt time.Time `json:"release_at"`
}

// safeJoin joins root and key, returning an error if the resulting path
// escapes outside root. This prevents path-traversal attacks via keys
// like "../../etc/passwd".
func safeJoin(root, key string) (string, error) {
	p := filepath.Join(root, filepath.Clean(key))
	if !strings.HasPrefix(p, root+string(os.PathSeparator)) && p != root {
		return "", fmt.Errorf("key %q escapes root %q", key, root)
	}
	return p, nil
}

// List returns DataObjects for files in stagingDir whose names start with
// prefix. An empty prefix matches all files. Held files (stored in the
// .chaos-hold/ subdirectory) are excluded from results.
func (t *FSTransport) List(_ context.Context, prefix string) ([]types.DataObject, error) {
	entries, err := os.ReadDir(t.stagingDir)
	if err != nil {
		return nil, fmt.Errorf("list staging dir: %w", err)
	}

	var objects []types.DataObject
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if prefix != "" && !strings.HasPrefix(name, prefix) {
			continue
		}

		info, err := entry.Info()
		if err != nil {
			return nil, fmt.Errorf("stat %s: %w", name, err)
		}

		objects = append(objects, types.DataObject{
			Key:          name,
			Size:         info.Size(),
			LastModified: info.ModTime(),
		})
	}

	return objects, nil
}

// Read opens the file at stagingDir/key and returns a ReadCloser.
// Returns an error if key would escape the staging directory.
func (t *FSTransport) Read(_ context.Context, key string) (io.ReadCloser, error) {
	p, err := safeJoin(t.stagingDir, key)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", key, err)
	}
	f, err := os.Open(p)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", key, err)
	}
	return f, nil
}

// Write writes data to outputDir/key, creating subdirectories as needed.
// Returns an error if key would escape the output directory.
func (t *FSTransport) Write(_ context.Context, key string, data io.Reader) error {
	dest, err := safeJoin(t.outputDir, key)
	if err != nil {
		return fmt.Errorf("write %s: %w", key, err)
	}
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		return fmt.Errorf("create directories for %s: %w", key, err)
	}

	f, err := os.Create(dest)
	if err != nil {
		return fmt.Errorf("create %s: %w", key, err)
	}

	if _, err := io.Copy(f, data); err != nil {
		f.Close()
		return fmt.Errorf("write %s: %w", key, err)
	}
	return f.Close()
}

// Delete removes the file at stagingDir/key.
// Returns an error if key would escape the staging directory.
func (t *FSTransport) Delete(_ context.Context, key string) error {
	p, err := safeJoin(t.stagingDir, key)
	if err != nil {
		return fmt.Errorf("delete %s: %w", key, err)
	}
	if err := os.Remove(p); err != nil {
		return fmt.Errorf("delete %s: %w", key, err)
	}
	return nil
}

// Hold moves a file from stagingDir to holdDir and writes a .meta sidecar
// JSON file recording the release time.
// Returns an error if key would escape the staging or hold directories.
//
// The .meta sidecar is written before the file is renamed so that if the
// rename fails, no orphaned data file exists without metadata.
func (t *FSTransport) Hold(_ context.Context, key string, until time.Time) error {
	src, err := safeJoin(t.stagingDir, key)
	if err != nil {
		return fmt.Errorf("hold %s: %w", key, err)
	}
	dst, err := safeJoin(t.holdDir, key)
	if err != nil {
		return fmt.Errorf("hold %s: %w", key, err)
	}

	if err := os.MkdirAll(t.holdDir, 0o755); err != nil {
		return fmt.Errorf("create hold dir: %w", err)
	}

	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return fmt.Errorf("create hold subdirectories for %s: %w", key, err)
	}

	// Write .meta sidecar before rename for atomicity: if the rename
	// fails we clean up the .meta, preventing an orphaned data file
	// without metadata.
	meta := holdMeta{ReleaseAt: until}
	metaBytes, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("marshal hold metadata for %s: %w", key, err)
	}

	metaPath := filepath.Join(t.holdDir, key+".meta")
	if err := os.WriteFile(metaPath, metaBytes, 0o644); err != nil {
		return fmt.Errorf("write hold metadata for %s: %w", key, err)
	}

	if err := os.Rename(src, dst); err != nil {
		// Clean up the .meta sidecar on rename failure.
		os.Remove(metaPath)
		return fmt.Errorf("hold %s: %w", key, err)
	}

	return nil
}

// Release moves a file from holdDir to outputDir and removes the .meta
// sidecar file. A missing .meta file is tolerated (not treated as an error).
// Returns an error if key would escape the hold or output directories.
func (t *FSTransport) Release(_ context.Context, key string) error {
	src, err := safeJoin(t.holdDir, key)
	if err != nil {
		return fmt.Errorf("release %s: %w", key, err)
	}
	dst, err := safeJoin(t.outputDir, key)
	if err != nil {
		return fmt.Errorf("release %s: %w", key, err)
	}

	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return fmt.Errorf("create output subdirectories for %s: %w", key, err)
	}

	if err := os.Rename(src, dst); err != nil {
		return fmt.Errorf("release %s: %w", key, err)
	}

	metaPath := filepath.Join(t.holdDir, key+".meta")
	if err := os.Remove(metaPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove hold metadata for %s: %w", key, err)
	}

	return nil
}
