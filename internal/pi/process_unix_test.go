//go:build unix

package pi

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func restoreSignalSeams(t *testing.T) {
	t.Helper()

	osProcess, getpgid, kill := signalOSProcess, syscallGetpgid, syscallKill

	t.Cleanup(func() {
		signalOSProcess, syscallGetpgid, syscallKill = osProcess, getpgid, kill
	})
}

func startedThrowawayCommand(t *testing.T) *exec.Cmd {
	t.Helper()

	cmd := exec.Command("/bin/sh", "-c", "sleep 5")
	require.NoError(t, cmd.Start())

	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	})

	return cmd
}

func TestSignalProcessNilTargets(t *testing.T) {
	t.Parallel()

	signalled, err := signalProcess(nil, syscall.SIGTERM)
	require.NoError(t, err)
	require.False(t, signalled)

	signalled, err = signalProcess(&exec.Cmd{}, syscall.SIGTERM)
	require.NoError(t, err)
	require.False(t, signalled)
}

func TestSignalProcessWithoutProcessGroup(t *testing.T) {
	restoreSignalSeams(t)

	cmd := startedThrowawayCommand(t)
	require.False(t, usesProcessGroup(cmd))

	t.Run("signal delivered", func(t *testing.T) {
		signalled, err := signalProcess(cmd, syscall.SIGCONT)
		require.NoError(t, err)
		require.True(t, signalled)
	})

	t.Run("done process is not an error", func(t *testing.T) {
		signalOSProcess = func(*os.Process, os.Signal) error { return os.ErrProcessDone }

		signalled, err := signalProcess(cmd, syscall.SIGTERM)
		require.NoError(t, err)
		require.False(t, signalled)
	})

	t.Run("other signal errors propagate", func(t *testing.T) {
		signalOSProcess = func(*os.Process, os.Signal) error { return fmt.Errorf("boom") }

		_, err := signalProcess(cmd, syscall.SIGTERM)
		require.ErrorContains(t, err, "boom")
	})
}

func TestSignalProcessGroupSeams(t *testing.T) {
	restoreSignalSeams(t)

	cmd := startedThrowawayCommand(t)
	cmd.SysProcAttr = processSysProcAttr()
	require.True(t, usesProcessGroup(cmd))

	t.Run("vanished group on getpgid", func(t *testing.T) {
		syscallGetpgid = func(int) (int, error) { return 0, syscall.ESRCH }

		signalled, err := signalProcess(cmd, syscall.SIGTERM)
		require.NoError(t, err)
		require.False(t, signalled)
	})

	t.Run("getpgid errors propagate", func(t *testing.T) {
		syscallGetpgid = func(int) (int, error) { return 0, syscall.EPERM }

		_, err := signalProcess(cmd, syscall.SIGTERM)
		require.Error(t, err)
	})

	t.Run("vanished group on kill", func(t *testing.T) {
		syscallGetpgid = func(int) (int, error) { return 12345, nil }
		syscallKill = func(int, syscall.Signal) error { return syscall.ESRCH }

		signalled, err := signalProcess(cmd, syscall.SIGTERM)
		require.NoError(t, err)
		require.False(t, signalled)
	})

	t.Run("kill errors propagate", func(t *testing.T) {
		syscallGetpgid = func(int) (int, error) { return 12345, nil }
		syscallKill = func(int, syscall.Signal) error { return syscall.EPERM }

		_, err := signalProcess(cmd, syscall.SIGTERM)
		require.Error(t, err)
	})

	t.Run("group signal delivered", func(t *testing.T) {
		syscallGetpgid = func(int) (int, error) { return 12345, nil }
		syscallKill = func(int, syscall.Signal) error { return nil }

		signalled, err := signalProcess(cmd, syscall.SIGTERM)
		require.NoError(t, err)
		require.True(t, signalled)
	})
}

// startStubbornProcessWithSeams launches a TERM-ignoring child for signal-seam
// tests. It uses a background context (never cancelled) so exec's context
// watcher cannot invoke the platform Cancel concurrently with the seam
// restore, and its cleanup restores the real syscalls before reaping.
func startStubbornProcessWithSeams(t *testing.T) *Process {
	t.Helper()

	osProcess, getpgid, kill := signalOSProcess, syscallGetpgid, syscallKill

	script := writeScript(t, `trap '' TERM; while :; do sleep 0.1; done`)

	process, err := StartProcess(context.Background(), LaunchSpec{
		ExecutablePath:      script,
		AgentDir:            t.TempDir(),
		ShutdownStepTimeout: 50 * time.Millisecond,
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		signalOSProcess, syscallGetpgid, syscallKill = osProcess, getpgid, kill

		_ = process.Kill()
		<-process.Exited()
		_ = process.Close()
	})

	return process
}

func TestProcessSignalFailuresSurface(t *testing.T) {
	process := startStubbornProcessWithSeams(t)

	syscallGetpgid = func(int) (int, error) { return 0, syscall.EPERM }

	require.ErrorContains(t, process.Shutdown(t.Context()), "terminate pi process")
	require.ErrorContains(t, process.Kill(), "kill pi process")
}

func TestProcessShutdownKillFailureSurfaces(t *testing.T) {
	process := startStubbornProcessWithSeams(t)

	syscallKill = func(pgid int, signal syscall.Signal) error {
		if signal == syscall.SIGKILL {
			return syscall.EPERM
		}

		return syscall.Kill(pgid, signal)
	}

	require.ErrorContains(t, process.Shutdown(t.Context()), "kill pi process")
}

func TestConfigureProcessCommandPlatformCancel(t *testing.T) {
	restoreSignalSeams(t)

	var delivered []os.Signal

	signalOSProcess = func(_ *os.Process, signal os.Signal) error {
		delivered = append(delivered, signal)

		return nil
	}

	cmd := startedThrowawayCommand(t)
	configureProcessCommandPlatform(cmd)
	require.NotNil(t, cmd.SysProcAttr)
	require.NotNil(t, cmd.Cancel)

	// Started before configure, so no process group: Cancel signals the
	// process directly through the seam.
	cmd.SysProcAttr = nil
	require.NoError(t, cmd.Cancel())
	require.Equal(t, []os.Signal{syscall.SIGTERM}, delivered)
}
