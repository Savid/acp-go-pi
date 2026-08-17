//go:build darwin

package piacp

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOwnedBrowserShimRemovesItselfWhenOwnershipHandoffFails(t *testing.T) {
	scratch := t.TempDir()
	agent := NewAgent(
		WithScratchDir(scratch),
		WithProcessIsolation(ProcessIsolation{UID: 11, GID: 22, BaseEnvironment: map[string]string{}}),
	)

	shim, err := agent.newOwnedSessionBrowserShim()
	require.ErrorContains(t, err, "unsupported")
	require.Nil(t, shim)
	require.Empty(t, browserShimDirs(t, scratch))
}
