//go:build darwin

package pi

import (
	"os/exec"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOrdinaryDarwinProcessControlSignalsOnlyTheOriginalGroup(t *testing.T) {
	original := darwinProcessGroupSignal
	t.Cleanup(func() { darwinProcessGroupSignal = original })

	var signals []syscall.Signal
	darwinProcessGroupSignal = func(pid int, signal syscall.Signal) error {
		require.Equal(t, -4321, pid)
		signals = append(signals, signal)

		return nil
	}

	tree := &processTree{pgid: 4321, ordinary: true}
	require.NoError(t, terminateProcessTree(tree))
	require.NoError(t, killProcessTree(tree))
	require.Equal(t, []syscall.Signal{syscall.SIGTERM, syscall.SIGKILL}, signals)
}

func TestDarwinRefusesExplicitProcessIsolationBeforeBootstrap(t *testing.T) {
	_, err := prepareProcessTreeCommand(exec.Command("/usr/bin/true"), ContainmentSpec{
		Isolation: &ProcessIsolation{UID: 11, GID: 22, BaseEnvironment: map[string]string{}},
	})
	require.ErrorContains(t, err, "supported only on linux")
}
