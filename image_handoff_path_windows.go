//go:build windows

package piacp

import (
	"path/filepath"
	"strings"
)

// handoffLocalPath maps the path component of a file uri onto a local path.
// A file uri names a drive-letter path as "/C:/dir/file", because the path
// component of a uri always begins at the root, so the leading slash belongs to
// the uri rather than to the filesystem. Converting separators without dropping
// it produced "\C:\dir\file", a path no drive holds, and every handoff a
// Windows host offered was refused as missing. Only a slash that introduces a
// drive is dropped, so a uri naming a rooted path on the current drive still
// keeps the root it stated.
func handoffLocalPath(uriPath string) string {
	trimmed := strings.TrimPrefix(uriPath, "/")
	if filepath.VolumeName(filepath.FromSlash(trimmed)) != "" {
		return filepath.FromSlash(trimmed)
	}

	return filepath.FromSlash(uriPath)
}
