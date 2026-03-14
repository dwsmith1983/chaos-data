package scenario

import (
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"

	catalogFS "github.com/dwsmith1983/chaos-data/catalog"
)

// ErrNotFound is returned when a scenario name does not exist in the catalog.
var ErrNotFound = errors.New("scenario not found")

// BuiltinCatalog returns all scenarios from the embedded catalog.
// Each returned scenario has passed Validate(). The returned slice is a
// fresh copy; callers may modify it without affecting the catalog.
func BuiltinCatalog() ([]Scenario, error) {
	var scenarios []Scenario

	err := fs.WalkDir(catalogFS.FS, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}

		ext := strings.ToLower(filepath.Ext(d.Name()))
		if ext != ".yaml" && ext != ".yml" {
			return nil
		}

		data, readErr := fs.ReadFile(catalogFS.FS, path)
		if readErr != nil {
			return fmt.Errorf("read catalog file %q: %w", path, readErr)
		}

		var s Scenario
		if unmarshalErr := yaml.Unmarshal(data, &s); unmarshalErr != nil {
			return fmt.Errorf("parse catalog file %q: %w", path, unmarshalErr)
		}

		if validateErr := s.Validate(); validateErr != nil {
			return fmt.Errorf("validate catalog file %q: %w", path, validateErr)
		}

		scenarios = append(scenarios, s)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walk catalog: %w", err)
	}

	return scenarios, nil
}

// Get returns a single scenario by name from the built-in catalog.
// Returns ErrNotFound if the scenario does not exist.
func Get(name string) (Scenario, error) {
	scenarios, err := BuiltinCatalog()
	if err != nil {
		return Scenario{}, fmt.Errorf("load catalog: %w", err)
	}

	for _, s := range scenarios {
		if s.Name == name {
			return s, nil
		}
	}

	return Scenario{}, fmt.Errorf("%w: %q", ErrNotFound, name)
}
