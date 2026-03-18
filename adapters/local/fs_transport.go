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

// ListHeld returns HeldObjects currently in the hold directory. The walk is
// recursive so nested keys (e.g. "ingest/file.jsonl") are included. .meta
// sidecars are excluded from results. HeldUntil is populated from the
// accompanying .meta sidecar; a missing or corrupt sidecar leaves HeldUntil
// as the zero value. If the hold directory does not exist, nil is returned
// without error.
func (t *FSTransport) ListHeld(_ context.Context) ([]types.HeldObject, error) {
	if _, err := os.Stat(t.holdDir); errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}

	var objects []types.HeldObject
	err := filepath.WalkDir(t.holdDir, func(p string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			return nil
		}
		if strings.HasSuffix(d.Name(), ".meta") {
			return nil
		}

		info, err := d.Info()
		if err != nil {
			return fmt.Errorf("stat held %s: %w", d.Name(), err)
		}

		rel, err := filepath.Rel(t.holdDir, p)
		if err != nil {
			return fmt.Errorf("rel path %s: %w", p, err)
		}

		obj := types.HeldObject{
			DataObject: types.DataObject{
				Key:          rel,
				Size:         info.Size(),
				LastModified: info.ModTime(),
			},
		}

		metaPath := p + ".meta"
		if metaBytes, readErr := os.ReadFile(metaPath); readErr == nil {
			var meta holdMeta
			if json.Unmarshal(metaBytes, &meta) == nil {
				obj.HeldUntil = meta.ReleaseAt
			}
		}

		objects = append(objects, obj)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walk hold dir: %w", err)
	}

	return objects, nil
}

// ReleaseAll immediately releases all currently held objects. It calls
// ListHeld to enumerate held objects, then calls Release on each. Failures
// from individual Release calls are collected and returned via errors.Join
// so that a single failure does not prevent the remaining releases.
// If the hold directory does not exist, ReleaseAll returns nil without error.
func (t *FSTransport) ReleaseAll(ctx context.Context) error {
	held, err := t.ListHeld(ctx)
	if err != nil {
		return fmt.Errorf("release all: list held: %w", err)
	}

	var errs []error
	for _, obj := range held {
		if releaseErr := t.Release(ctx, obj.Key); releaseErr != nil {
			errs = append(errs, releaseErr)
		}
	}

	return errors.Join(errs...)
}

// HoldData writes data directly to holdDir/key and writes a .meta sidecar
// recording the release time. Unlike Hold, it does not require the data to
// exist in stagingDir first.
// Returns an error if key would escape the hold directory.
func (t *FSTransport) HoldData(_ context.Context, key string, data io.Reader, until time.Time) error {
	dst, err := safeJoin(t.holdDir, key)
	if err != nil {
		return fmt.Errorf("hold data %s: %w", key, err)
	}

	if err := os.MkdirAll(t.holdDir, 0o755); err != nil {
		return fmt.Errorf("create hold dir: %w", err)
	}

	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return fmt.Errorf("create hold subdirectories for %s: %w", key, err)
	}

	meta := holdMeta{ReleaseAt: until}
	metaBytes, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("marshal hold metadata for %s: %w", key, err)
	}

	metaPath := filepath.Join(t.holdDir, key+".meta")
	if err := os.WriteFile(metaPath, metaBytes, 0o644); err != nil {
		return fmt.Errorf("write hold metadata for %s: %w", key, err)
	}

	f, err := os.Create(dst)
	if err != nil {
		os.Remove(metaPath)
		return fmt.Errorf("create held file %s: %w", key, err)
	}

	if _, err := io.Copy(f, data); err != nil {
		f.Close()
		os.Remove(dst)
		os.Remove(metaPath)
		return fmt.Errorf("write held data %s: %w", key, err)
	}

	return f.Close()
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
