//go:build windows

package piacp

// syncAuthLedgerDirectory has no directory flush to perform on Windows.
// FlushFileBuffers refuses a directory handle opened through os.Open, so the
// call returned "Access is denied" on every ledger write and made the whole
// provider-auth surface inoperable here. What stays durable is the entry
// itself: its bytes are written, restricted, and flushed through
// FlushFileBuffers on the file handle before MoveFileEx replaces the name, and
// that replacement is atomic. Only the directory update ordering is left to
// the filesystem, which is the guarantee NTFS metadata journaling already
// provides.
func syncAuthLedgerDirectory(string) error { return nil }
