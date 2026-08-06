//go:build darwin

package pi

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestInheritedProcessIsolationDecodesOnlyAVerifiedPolicy proves the Darwin
// supervisor bootstrap accepts an inherited identity only when it parses and
// the running process actually holds it. The environment is attacker-adjacent
// input to a process that is about to drop privilege, so an unparsable pair is
// refused, and a parseable pair that the process cannot verify it holds is
// refused too. Only the explicit credential-free test policy bypasses the
// verification, and that is the one shape that grants nothing.
func TestInheritedProcessIsolationDecodesOnlyAVerifiedPolicy(t *testing.T) {
	originalUID, originalGID, originalGroups := processIsolationGeteuid, processIsolationGetegid, processIsolationGetgroups
	t.Cleanup(func() {
		processIsolationGeteuid, processIsolationGetegid, processIsolationGetgroups = originalUID, originalGID, originalGroups
	})

	t.Setenv(envIsolationUID, "invalid")
	t.Setenv(envIsolationGID, "22")

	_, err := inheritedProcessIsolation()
	require.ErrorContains(t, err, "process isolation bootstrap identity is invalid")

	t.Setenv(envIsolationUID, "11")
	t.Setenv(envIsolationTest, "true")

	isolation, err := inheritedProcessIsolation()
	require.NoError(t, err)
	require.True(t, isolation.TestOnlyNoCredential)
	require.Equal(t, uint32(11), isolation.UID)
	require.Equal(t, uint32(22), isolation.GID)

	t.Setenv(envIsolationTest, "false")
	processIsolationGeteuid = func() int { return 12 }
	processIsolationGetegid = func() int { return 22 }
	processIsolationGetgroups = func() ([]int, error) { return nil, nil }

	_, err = inheritedProcessIsolation()
	require.Error(t, err, "an identity the process does not hold was accepted")

	processIsolationGeteuid = func() int { return 11 }

	isolation, err = inheritedProcessIsolation()
	require.NoError(t, err)
	require.False(t, isolation.TestOnlyNoCredential)
	require.Equal(t, uint32(11), isolation.UID)
	require.Equal(t, uint32(22), isolation.GID)
}
