//go:build !windows

package piacp

import (
	"errors"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestAuthLedgerWriteReportsDirectoryFlushFailure pins the durability step a
// commit ends with where the platform has one: the ledger root is opened and
// flushed, and a write whose flush cannot even begin is reported rather than
// answered as committed.
func TestAuthLedgerWriteReportsDirectoryFlushFailure(t *testing.T) {
	ledger := testLedger(t)

	original := ledgerOpen
	t.Cleanup(func() { ledgerOpen = original })

	ledgerOpen = func(string) (*os.File, error) { return nil, errors.New("open") }
	require.ErrorContains(t, ledger.write(sampleLedgerRecord("anthropic")), "open provider auth ledger root")
}
