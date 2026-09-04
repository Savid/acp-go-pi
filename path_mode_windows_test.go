//go:build windows

package piacp

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// requireRestrictedMode has no permission bits to read on Windows. Confinement
// here is the ACL the path inherits from the user profile that holds it, and
// Go reports 0777 for every writable directory and 0666 for every writable
// file whatever that ACL says, so the wanted mode names bits this platform
// cannot carry. What is checked here is that the path the agent promised to
// create is there; the mode rule itself is pinned where modes exist.
func requireRestrictedMode(t *testing.T, path string, _ os.FileMode) {
	t.Helper()

	_, err := os.Stat(path)
	require.NoError(t, err)
}
