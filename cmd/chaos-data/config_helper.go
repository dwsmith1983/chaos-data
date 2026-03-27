package main

import (
	"os"
	"path/filepath"
)

// resolveConfigPath returns the config file path to use. If explicit is
// non-empty (from --config flag), it is returned as-is. Otherwise, auto-
// discovery checks ./chaos.yaml then ~/.config/chaos-data/config.yaml.
// Returns empty string if no config file is found.
func resolveConfigPath(explicit string) string {
	if explicit != "" {
		return explicit
	}

	// Check current directory first.
	if info, err := os.Stat("chaos.yaml"); err == nil && !info.IsDir() {
		return "chaos.yaml"
	}

	// Check XDG user config.
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	xdgPath := filepath.Join(home, ".config", "chaos-data", "config.yaml")
	if info, err := os.Stat(xdgPath); err == nil && !info.IsDir() {
		return xdgPath
	}

	return ""
}
