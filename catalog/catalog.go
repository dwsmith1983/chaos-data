// Package catalog provides the embedded built-in scenario catalog.
package catalog

import "embed"

// FS contains the embedded built-in scenario catalog.
//
//go:embed all:data-arrival all:data-quality all:state-consistency all:infrastructure all:orchestrator all:compound
var FS embed.FS
