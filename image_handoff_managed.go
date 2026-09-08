package piacp

import (
	"os"
	"path/filepath"
	"sync"
)

// managedHandoffRoot reserves the complete scratch allocation domain and pins
// one disjoint read root before any managed prepare or launch. The host keeps
// those directory identities and their placement stable, including mount aliases.
// Discovery is never retried after native work or a failed preparation.
type managedHandoffRoot struct {
	mu          sync.RWMutex
	scratch     string
	path        string
	initialized bool
	closed      bool
	root        *os.Root
}

func (r *managedHandoffRoot) freeze() {
	if r == nil {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.initialized || r.closed {
		return
	}

	r.initialized = true

	scratch, err := ensureScratchParent(r.scratch)
	if err != nil {
		return
	}

	_, domain, err := handoffDirectoryLineage(scratch)
	if err != nil {
		return
	}

	resolved, lineage, err := handoffDirectoryLineage(r.path)
	if err != nil || handoffLineageContains(lineage, domain[0]) || handoffLineageContains(domain, lineage[0]) {
		return
	}

	root, err := os.OpenRoot(resolved)
	if err != nil {
		return
	}

	info, err := root.Stat(".")
	if err != nil || !os.SameFile(info, lineage[0]) {
		_ = root.Close()

		return
	}

	r.root = root
}

func handoffDirectoryLineage(path string) (string, []os.FileInfo, error) {
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return "", nil, err
	}

	absolute, err := filepath.Abs(resolved)
	if err != nil {
		return "", nil, err
	}

	var lineage []os.FileInfo

	for path := absolute; ; path = filepath.Dir(path) {
		info, err := os.Stat(path)
		if err != nil {
			return "", nil, err
		}

		lineage = append(lineage, info)
		if filepath.Dir(path) == path {
			return absolute, lineage, nil
		}
	}
}

func handoffLineageContains(lineage []os.FileInfo, directory os.FileInfo) bool {
	for _, ancestor := range lineage {
		if os.SameFile(ancestor, directory) {
			return true
		}
	}

	return false
}

// acquire holds the pinned root through a whole prompt mapping. Close waits
// for those descriptor reads; reclaim never releases or repins this read root.
func (r *managedHandoffRoot) acquire() (*os.Root, func(), *handoffError) {
	r.freeze()
	r.mu.RLock()

	if r.closed || r.root == nil {
		r.mu.RUnlock()

		return nil, nil, &handoffError{value: imageErrorPathNotAllowed, message: handoffRootUnresolvedMessage}
	}

	return r.root, r.mu.RUnlock, nil
}

func (r *managedHandoffRoot) close() {
	if r == nil {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	r.closed = true
	if r.root != nil {
		_ = r.root.Close()
		r.root = nil
	}
}
