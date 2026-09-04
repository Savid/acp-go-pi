//go:build !windows

package piacp

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// requireRestrictedMode asserts the permission bits a path this agent created
// carries. Confinement on this platform is the POSIX mode, so the mode is what
// the rule is stated in.
func requireRestrictedMode(t *testing.T, path string, want os.FileMode) {
	t.Helper()

	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, want, info.Mode().Perm())
}
