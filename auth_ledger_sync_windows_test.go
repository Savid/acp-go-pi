//go:build windows

package piacp

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestAuthLedgerWriteCommitsWithoutADirectoryFlush is the Windows half of the
// commit rule. FlushFileBuffers refuses a directory handle, so the flush the
// POSIX side performs is not a weaker step here — it is one that always failed
// and took every authorize down with it. The entry's own bytes are still
// flushed before the rename, so the record a commit promises is readable back.
func TestAuthLedgerWriteCommitsWithoutADirectoryFlush(t *testing.T) {
	ledger := testLedger(t)
	record := sampleLedgerRecord("anthropic")

	require.NoError(t, syncAuthLedgerDirectory(ledger.dir))
	require.NoError(t, ledger.write(record))

	stored, found, err := ledger.read("anthropic")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, record, stored)
}
