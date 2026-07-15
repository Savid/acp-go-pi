//go:build linux || darwin || freebsd || openbsd

package pi

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProviderDescendantInventoryUnavailableOnUnix(t *testing.T) {
	var nilProcess *Process
	count, available := nilProcess.ProviderDescendantCount()
	require.Zero(t, count)
	require.False(t, available)

	process := &Process{}
	count, available = process.ProviderDescendantCount()
	require.Zero(t, count)
	require.False(t, available)

	process.tree = &processTree{pgid: 123}
	count, available = process.ProviderDescendantCount()
	require.Zero(t, count)
	require.False(t, available)
}
