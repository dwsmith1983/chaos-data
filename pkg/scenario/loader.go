package scenario

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

// LoadFile reads a YAML file at path, unmarshals it into a Scenario, validates
// it, and returns the result. The returned Scenario is a value type; the
// original file is never modified.
func LoadFile(path string) (Scenario, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Scenario{}, fmt.Errorf("load scenario file %q: %w", path, err)
	}

	var s Scenario
	if err := yaml.Unmarshal(data, &s); err != nil {
		return Scenario{}, fmt.Errorf("parse scenario file %q: %w", path, err)
	}

	if err := s.Validate(); err != nil {
		return Scenario{}, fmt.Errorf("validate scenario file %q: %w", path, err)
	}

	return s, nil
}

// LoadDir reads all .yaml and .yml files in dir (non-recursive), loading each
// with LoadFile. It returns all successfully loaded scenarios or an empty
// (non-nil) slice when no files match. If any file fails to load, it returns
// an error.
func LoadDir(dir string) ([]Scenario, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read scenario directory %q: %w", dir, err)
	}

	scenarios := make([]Scenario, 0)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		ext := strings.ToLower(filepath.Ext(entry.Name()))
		if ext != ".yaml" && ext != ".yml" {
			continue
		}

		path := filepath.Join(dir, entry.Name())
		s, err := LoadFile(path)
		if err != nil {
			return nil, err
		}

		scenarios = append(scenarios, s)
	}

	return scenarios, nil
}
