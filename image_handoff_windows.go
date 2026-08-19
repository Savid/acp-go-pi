//go:build windows

package piacp

// handoffOpenFlags adds no flags on Windows, which exposes no non-blocking open
// mode here and needs none: O_NONBLOCK is a Unix flag, and the platform open
// flags are the one part of the read this contract lets a platform differ on.
// Everything load-bearing binds here exactly as it does on Unix — the read opens
// the block's path through the handle held on the configured root, so
// containment is the kernel's and atomic with the open, and the descriptor is
// stat-ed and must be a regular file.
const handoffOpenFlags = 0
