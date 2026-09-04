//go:build !windows

package piacp

import (
	"errors"
	"fmt"
)

// syncAuthLedgerDirectory flushes the ledger root itself, so the rename that
// committed an entry is durable rather than merely visible: without it a crash
// can leave the entry's bytes on disk under a name the directory never
// recorded.
func syncAuthLedgerDirectory(dir string) error {
	handle, err := ledgerOpen(dir)
	if err != nil {
		return fmt.Errorf("open provider auth ledger root: %w", err)
	}

	return errors.Join(handle.Sync(), handle.Close())
}
