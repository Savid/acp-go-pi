//go:build darwin || freebsd || openbsd

package pi

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBrowserShimOwnershipHandoffWithoutLinuxAuthority(t *testing.T) {
	shim, err := NewBrowserShim(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, shim.Remove()) })

	require.NoError(t, shim.Handoff(nil))
	require.NoError(t, shim.Handoff(&ProcessIsolation{TestOnlyNoCredential: true}))
	require.ErrorContains(t, shim.Handoff(&ProcessIsolation{UID: 11, GID: 22}), "unsupported")
}
