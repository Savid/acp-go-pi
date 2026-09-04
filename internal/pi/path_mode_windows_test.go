//go:build windows

package pi

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// requireRestrictedMode has no permission bits to read on Windows. Confinement
// here is the ACL the path inherits from the profile that holds it, and Go
// reports 0666 for every writable file whatever that ACL says, so the wanted
// mode names bits this platform cannot carry. What is checked is that the file
// the writer promised is there; the mode rule is pinned where modes exist.
func requireRestrictedMode(t *testing.T, path string, _ os.FileMode) {
	t.Helper()

	_, err := os.Stat(path)
	require.NoError(t, err)
}
