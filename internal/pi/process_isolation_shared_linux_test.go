//go:build linux

package pi

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSharedIdentityIsolationCarriesNoStandaloneOwnerFields(t *testing.T) {
	restoreSharedIdentitySeams(t)

	processIsolationGeteuid = func() int { return 1000 }

	canonical := &ProcessIsolation{UID: 1000, GID: 1000, BaseEnvironment: map[string]string{}}
	require.NoError(t, validateStandaloneIdentityDisposition(canonical))

	withOwner := *canonical
	withOwner.StandaloneOwnerID = "shared-owner-test"
	err := validateStandaloneIdentityDisposition(&withOwner)
	require.ErrorContains(t, err, "standalone owner fields describe an identity the supervisor already holds")
	require.ErrorContains(t, err, sharedIdentitySupervisorRemedy)

	withStateRoot := *canonical
	withStateRoot.StandaloneStateRoot = "/var/lib/acp-go-pi-test"
	err = validateStandaloneIdentityDisposition(&withStateRoot)
	require.ErrorContains(t, err, "standalone owner fields describe an identity the supervisor already holds")

	processIsolationGeteuid = func() int { return 0 }
	require.EqualError(
		t,
		validateStandaloneIdentityDisposition(canonical),
		"standalone owner id must be 1..256 valid UTF-8 bytes without whitespace or control characters",
	)

	borrowed := *canonical
	placeholder := &agentIdentityLock{}
	borrowed.IdentityLock = placeholder
	borrowed.AuthorityDomain = placeholder

	processIsolationGeteuid = func() int { return 1000 }
	require.NoError(t, validateStandaloneIdentityDisposition(&borrowed))
}
