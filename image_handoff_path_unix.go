//go:build !windows

package piacp

import "path/filepath"

// handoffLocalPath maps the path component of a file uri onto a local path.
// Here the two are the same thing once the separators are converted: a rooted
// uri path is already a rooted filesystem path.
func handoffLocalPath(uriPath string) string {
	return filepath.FromSlash(uriPath)
}
