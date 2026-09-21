package piacp

import "github.com/savid/acp-go-core/process"

// scratchDir names the adapter directory for one purpose and one name under
// the configured scratch parent.
func (a *Agent) scratchDir(purpose string, name string) (string, error) {
	return process.ScratchPath(a.options.ScratchDir, vendor, purpose, name)
}
