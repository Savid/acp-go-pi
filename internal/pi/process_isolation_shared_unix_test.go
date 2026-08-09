//go:build unix

package pi

import (
	"os/exec"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

func restoreSharedIdentitySeams(t *testing.T) {
	t.Helper()

	goos, geteuid, getegid, getgroups :=
		processIsolationGOOS, processIsolationGeteuid, processIsolationGetegid, processIsolationGetgroups
	t.Cleanup(func() {
		processIsolationGOOS, processIsolationGeteuid = goos, geteuid
		processIsolationGetegid, processIsolationGetgroups = getegid, getgroups
	})

	processIsolationGOOS = processIsolationLinux
}

func TestSharedProcessIdentityNamesOnlyTheSupervisorsOwnLinuxIdentity(t *testing.T) {
	restoreSharedIdentitySeams(t)

	processIsolationGeteuid = func() int { return 1000 }
	require.False(t, sharedProcessIdentity(nil))
	require.True(t, sharedProcessIdentity(&ProcessIsolation{UID: 1000, GID: 1000}))
	require.True(t, sharedProcessIdentity(&ProcessIsolation{UID: 1000, GID: 1001}))
	require.False(t, sharedProcessIdentity(&ProcessIsolation{UID: 1001, GID: 1000}))

	processIsolationGeteuid = func() int { return 0 }
	require.False(t, sharedProcessIdentity(&ProcessIsolation{UID: 0, GID: 0}))
	require.False(t, sharedProcessIdentity(&ProcessIsolation{UID: 1000, GID: 1000}))

	processIsolationGeteuid = func() int { return 1000 }
	processIsolationGOOS = "darwin"
	require.False(t, sharedProcessIdentity(&ProcessIsolation{UID: 1000, GID: 1000}))
}

func TestSharedIdentityCredentialRequestsNoIdentityChange(t *testing.T) {
	restoreSharedIdentitySeams(t)

	processIsolationGeteuid = func() int { return 1000 }
	processIsolationGetegid = func() int { return 1000 }
	processIsolationGetgroups = func() ([]int, error) { return []int{1000, 27}, nil }

	shared := &ProcessIsolation{UID: 1000, GID: 1000, BaseEnvironment: map[string]string{}}
	cmd := exec.Command("/usr/bin/true")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	require.NoError(t, applyProcessIsolation(cmd, shared))
	require.True(t, cmd.SysProcAttr.Setpgid)
	require.Nil(t, cmd.SysProcAttr.Credential)

	wrongGroup := &ProcessIsolation{UID: 1000, GID: 1001, BaseEnvironment: map[string]string{}}
	err := applyProcessIsolation(exec.Command("/usr/bin/true"), wrongGroup)
	require.ErrorContains(t, err, "native group 1001 cannot be entered from group 1000")
	require.ErrorContains(t, err, sharedIdentitySupervisorRemedy)

	processIsolationGeteuid = func() int { return 0 }
	processIsolationGetegid = func() int { return 0 }

	isolated := &ProcessIsolation{
		UID: 1000, GID: 1000, BaseEnvironment: map[string]string{},
		StandaloneOwnerID: "shared-credential-test", StandaloneStateRoot: "/var/lib/acp-go-pi-test",
	}
	root := exec.Command("/usr/bin/true")
	require.NoError(t, applyProcessIsolation(root, isolated))
	require.NotNil(t, root.SysProcAttr.Credential)
	require.Equal(t, isolated.UID, root.SysProcAttr.Credential.Uid)
	require.Equal(t, isolated.GID, root.SysProcAttr.Credential.Gid)
	require.Empty(t, root.SysProcAttr.Credential.Groups)
}
