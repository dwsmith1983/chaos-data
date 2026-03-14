// Package catalog provides the embedded built-in scenario catalog.
package catalog

import "embed"

// FS contains the embedded built-in scenario catalog.
//
//go:embed all:data-arrival
var FS embed.FS
