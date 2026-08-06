//go:build unix

package pi

import (
	"errors"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProcessIsolationUnixVerificationBranches(t *testing.T) {
	originalUID, originalGID, originalGroups := processIsolationGeteuid, processIsolationGetegid, processIsolationGetgroups
	t.Cleanup(func() {
		processIsolationGeteuid, processIsolationGetegid, processIsolationGetgroups = originalUID, originalGID, originalGroups
	})
	processIsolationGeteuid = func() int { return 11 }
	processIsolationGetegid = func() int { return 22 }
	processIsolationGetgroups = func() ([]int, error) { return nil, nil }
	policy := &ProcessIsolation{
		UID: 11, GID: 22, BaseEnvironment: map[string]string{},
		StandaloneOwnerID: "unix-verification-test", StandaloneStateRoot: "/var/lib/acp-go-pi-test",
	}
	require.NoError(t, verifyProcessIsolation(policy))
	require.NoError(t, applyProcessIsolation(exec.Command("/usr/bin/true"), policy))
	processIsolationGetgroups = func() ([]int, error) { return nil, errors.New("groups") }
	require.Error(t, verifyProcessIsolation(policy))
	processIsolationGetgroups = func() ([]int, error) { return []int{22}, nil }
	require.Error(t, verifyProcessIsolation(policy))
	processIsolationGeteuid = func() int { return 12 }
	require.Error(t, verifyProcessIsolation(policy))
	require.Error(t, verifyProcessIsolation(nil))
	require.Error(t, applyProcessIsolation(nil, policy))
	require.Error(t, applyProcessIsolation(exec.Command("/usr/bin/true"), nil))
	require.NoError(t, applyProcessIsolation(exec.Command("/usr/bin/true"), &ProcessIsolation{UID: 11, GID: 22, BaseEnvironment: map[string]string{}, TestOnlyNoCredential: true}))
	cmd := exec.Command("/usr/bin/true")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	require.NoError(t, applyProcessIsolation(cmd, policy))
	require.True(t, cmd.SysProcAttr.Setpgid)
	require.NotNil(t, cmd.SysProcAttr.Credential)
	require.Equal(t, policy.UID, cmd.SysProcAttr.Credential.Uid)
	require.Equal(t, policy.GID, cmd.SysProcAttr.Credential.Gid)
	require.Empty(t, cmd.SysProcAttr.Credential.Groups)
	plain := exec.Command("/usr/bin/true")
	require.NoError(t, applyProcessIsolation(plain, policy))
	require.NotNil(t, plain.SysProcAttr.Credential)
}

func TestProcessIsolationLaunchFailures(t *testing.T) {
	wantErr := errors.New("injected isolation failure")
	originalUID, originalGID, originalGroups := processIsolationGeteuid, processIsolationGetegid, processIsolationGetgroups
	t.Cleanup(func() {
		processIsolationGeteuid, processIsolationGetegid, processIsolationGetgroups = originalUID, originalGID, originalGroups
	})
	processIsolationGeteuid = func() int { return 11 }
	processIsolationGetegid = func() int { return 22 }
	processIsolationGetgroups = func() ([]int, error) { return nil, wantErr }
	containment := ContainmentSpec{Isolation: &ProcessIsolation{
		UID: 11, GID: 22, BaseEnvironment: map[string]string{"PATH": "/usr/bin"},
		StandaloneOwnerID: "launch-failure-test", StandaloneStateRoot: "/var/lib/acp-go-pi-test",
	}}
	containment.GenerationRoot = t.TempDir()

	t.Run("process", func(t *testing.T) {
		restoreProcessSeams(t)
		processPrepareTreeCommand = func(cmd *exec.Cmd, _ ContainmentSpec) (*processTreeCommand, error) {
			return &processTreeCommand{cmd: cmd}, nil
		}
		_, err := StartProcess(t.Context(), LaunchSpec{ExecutablePath: "/usr/bin/true", Containment: containment})
		require.ErrorContains(t, err, "apply pi process isolation")
	})

	t.Run("version", func(t *testing.T) {
		restoreVersionSeams(t)
		versionPrepareTreeCommand = func(cmd *exec.Cmd, _ ContainmentSpec) (*processTreeCommand, error) {
			return &processTreeCommand{cmd: cmd}, nil
		}
		agentDir := filepath.Join(containment.GenerationRoot, "probe-agent")
		require.NoError(t, (AgentDir{Root: agentDir}).Write())
		_, err := ProbeVersion(t.Context(), "/usr/bin/true", agentDir, containment)
		require.ErrorContains(t, err, "apply pi version process isolation")
	})
}
