package piacp

import (
	"fmt"
	"os"
	"path/filepath"
)

// scratchDir is the sole scratch accessor. It returns the adapter directory
// for one purpose under the configured scratch parent, creating the parent
// 0700 when missing. Names carry the acp-go-pi-<purpose>- prefix so a host
// can sweep orphans.
func (a *Agent) scratchDir(purpose string, name string) (string, error) {
	parent := a.options.ScratchDir
	if parent == "" {
		parent = os.TempDir()
	}

	if err := os.MkdirAll(parent, 0o700); err != nil {
		return "", fmt.Errorf("create scratch parent: %w", err)
	}

	return filepath.Join(parent, "acp-go-pi-"+purpose+"-"+name), nil
}
