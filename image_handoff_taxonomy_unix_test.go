//go:build !windows

package piacp

// A path whose parent component is a regular file is reported by this platform
// as a path that may not be taken: open(2) answers ENOTDIR, which is not a
// missing file, so the mapper refuses the route rather than the name.
const (
	handoffThroughFileError   = imageErrorPathNotAllowed
	handoffThroughFileMessage = handoffUnopenableMessage
)
