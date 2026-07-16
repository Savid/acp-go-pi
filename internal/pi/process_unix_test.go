//go:build linux

package pi

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const adapterDeathHelperEnv = "ACP_GO_PI_ADAPTER_DEATH_TEST_HELPER"

const (
	detachedNativePathEnv = "ACP_GO_PI_DETACHED_NATIVE_PATH"
	detachedPIDFileEnv    = "ACP_GO_PI_DETACHED_PID_FILE"
	detachedSentinelEnv   = "ACP_GO_PI_DETACHED_SENTINEL"
	detachedAgentDirEnv   = "ACP_GO_PI_DETACHED_AGENT_DIR"
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
	process.tree.mu.Lock()
	process.tree.supervised = false
	process.tree.mu.Unlock()
	t.Cleanup(func() {
		process.tree.mu.Lock()
		process.tree.supervised = true
		process.tree.mu.Unlock()
	})

	syscallKill = func(int, syscall.Signal) error { return syscall.EPERM }

	require.ErrorContains(t, process.Shutdown(t.Context()), "terminate pi process")
	require.ErrorContains(t, process.Kill(), "kill pi process")
}

func TestProcessKillReportsUnreapedRoot(t *testing.T) {
	process := &Process{tree: &processTree{}, exited: make(chan struct{})}

	require.ErrorIs(t, process.Kill(), ErrProcessTreeNotQuiescent)
}

func TestShutdownWaitsForDescendantTreeQuiescence(t *testing.T) {
	dir := t.TempDir()
	pidFile := filepath.Join(dir, "child.pid")
	script := writeScript(t, `(trap '' TERM; while :; do sleep 1; done) &
echo $! > "$PI_CHILD_PID_FILE"
cat >/dev/null
exit 0`)

	process := startScriptProcess(t, LaunchSpec{
		ExecutablePath: script,
		AgentDir:       dir,
		Env:            map[string]string{"PI_CHILD_PID_FILE": pidFile},
	})
	t.Cleanup(func() { _ = process.Close() })

	require.NoError(t, process.Shutdown(t.Context()))

	rawPID, err := os.ReadFile(pidFile)
	require.NoError(t, err)

	pid, err := strconv.Atoi(strings.TrimSpace(string(rawPID)))
	require.NoError(t, err)
	require.False(t, processPIDAlive(pid), "descendant %d still live after shutdown", pid)
}

func TestProbeVersionWaitsForDescendantTreeQuiescence(t *testing.T) {
	dir := t.TempDir()
	pidFile := filepath.Join(dir, "child.pid")
	t.Setenv("PI_VERSION_CHILD_PID_FILE", pidFile)

	script := filepath.Join(dir, "fake-pi")
	require.NoError(t, os.WriteFile(script, []byte(`#!/bin/sh
(trap '' TERM; while :; do sleep 1; done) &
echo $! > "$PI_VERSION_CHILD_PID_FILE"
echo 0.80.6
`), 0o700))

	version, err := ProbeVersion(t.Context(), script)
	require.NoError(t, err)
	require.Equal(t, "0.80.6", version)

	rawPID, err := os.ReadFile(pidFile)
	require.NoError(t, err)

	pid, err := strconv.Atoi(strings.TrimSpace(string(rawPID)))
	require.NoError(t, err)
	require.False(t, processPIDAlive(pid), "version-probe descendant %d still live", pid)
}

func TestProcessTreeQuiescenceFailureBranches(t *testing.T) {
	restoreSignalSeams(t)

	require.NoError(t, (*processTree)(nil).terminateAndWait(time.Millisecond))
	require.NoError(t, signalProcessGroupID(0, syscall.SIGKILL))
	require.True(t, ProcessTreeQuiescent(nil))
	require.False(t, ProcessTreeQuiescent(ErrProcessTreeNotQuiescent))

	tree := &processTree{pgid: 12345}
	syscallKill = func(int, syscall.Signal) error { return syscall.EPERM }
	require.ErrorIs(t, tree.terminateAndWait(time.Millisecond), ErrProcessTreeNotQuiescent)

	calls := 0
	syscallKill = func(_ int, signal syscall.Signal) error {
		calls++
		if signal == 0 {
			return syscall.ESRCH
		}

		return nil
	}
	require.NoError(t, tree.terminateAndWait(time.Millisecond))
	require.Equal(t, 2, calls)

	syscallKill = func(_ int, signal syscall.Signal) error {
		if signal == 0 {
			return syscall.EINVAL
		}

		return nil
	}
	require.ErrorIs(t, tree.terminateAndWait(time.Millisecond), ErrProcessTreeNotQuiescent)

	syscallKill = func(_ int, signal syscall.Signal) error {
		if signal == 0 {
			return syscall.EPERM
		}

		return nil
	}
	require.ErrorIs(t, tree.terminateAndWait(time.Millisecond), ErrProcessTreeNotQuiescent)
}

func TestShutdownReturnsFromGracefulSignalRung(t *testing.T) {
	stdinRead, stdinWrite, err := os.Pipe()
	require.NoError(t, err)
	t.Cleanup(func() { _ = stdinRead.Close() })

	process := &Process{
		tree:                &processTree{},
		stdin:               stdinWrite,
		shutdownStepTimeout: 50 * time.Millisecond,
		exited:              make(chan struct{}),
	}
	go func() {
		time.Sleep(75 * time.Millisecond)
		close(process.exited)
	}()

	require.NoError(t, process.Shutdown(t.Context()))
}

func TestLinuxSupervisorProofBranches(t *testing.T) {
	t.Run("missing channel", func(t *testing.T) {
		tree := &processTree{supervised: true}
		require.ErrorIs(t, tree.proveQuiescence(), ErrProcessTreeNotQuiescent)
		require.ErrorIs(t, tree.proveQuiescence(), ErrProcessTreeNotQuiescent)
	})

	t.Run("deadline failure", func(t *testing.T) {
		proof, err := os.CreateTemp(t.TempDir(), "proof")
		require.NoError(t, err)
		tree := &processTree{supervised: true, proof: proof, status: bufio.NewReader(proof)}
		require.ErrorContains(t, tree.proveQuiescence(), "arm pi turn supervisor proof")
	})

	for _, test := range []struct {
		name  string
		value string
		ok    bool
	}{
		{name: "eof"},
		{name: "invalid", value: "not-proof\n"},
		{name: "proven", value: turnSupervisorProven, ok: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			proof, writer, err := os.Pipe()
			require.NoError(t, err)
			if test.value != "" {
				_, err = io.WriteString(writer, test.value)
				require.NoError(t, err)
			}
			require.NoError(t, writer.Close())

			tree := &processTree{supervised: true, proof: proof, status: bufio.NewReader(proof)}
			err = tree.proveQuiescence()
			if test.ok {
				require.NoError(t, err)
				require.NoError(t, tree.proveQuiescence())
			} else {
				require.ErrorIs(t, err, ErrProcessTreeNotQuiescent)
			}
		})
	}
}

func TestLinuxSupervisorPreparationFailuresSurfaceAtCallers(t *testing.T) {
	restoreTurnSupervisorSeams(t)
	want := errors.New("memfd unavailable")
	turnSupervisorMemfd = func(string, int) (int, error) { return 0, want }

	_, err := StartProcess(t.Context(), LaunchSpec{
		ExecutablePath: "/bin/true",
		AgentDir:       t.TempDir(),
	})
	require.ErrorIs(t, err, want)
	require.ErrorContains(t, err, "prepare pi process")

	_, err = ProbeVersion(t.Context(), "/bin/true")
	require.ErrorIs(t, err, want)
	require.ErrorContains(t, err, "prepare pi version probe")
}

func TestSignalProcessGroupIDVanishedIsSuccess(t *testing.T) {
	restoreSignalSeams(t)
	syscallKill = func(int, syscall.Signal) error { return syscall.ESRCH }
	require.NoError(t, signalProcessGroupID(1234, syscall.SIGKILL))
}

func TestLinuxSupervisorAdapterDeathContainsDetachedDescendant(t *testing.T) {
	if os.Getenv(adapterDeathHelperEnv) == "1" {
		process, err := StartProcess(context.Background(), LaunchSpec{
			ExecutablePath: os.Getenv(detachedNativePathEnv),
			AgentDir:       os.Getenv(detachedAgentDirEnv),
			Env: map[string]string{
				"PI_DETACHED_PID_FILE": os.Getenv(detachedPIDFileEnv),
				"PI_DETACHED_SENTINEL": os.Getenv(detachedSentinelEnv),
			},
		})
		if err != nil {
			os.Exit(21)
		}

		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			if _, err := os.Stat(os.Getenv(detachedPIDFileEnv)); err == nil {
				// Deliberately bypass every cleanup path. Adapter process death
				// must close the private control descriptor by kernel action.
				_ = process
				os.Exit(0)
			}
			time.Sleep(10 * time.Millisecond)
		}

		os.Exit(22)
	}

	dir := t.TempDir()
	pidFile := filepath.Join(dir, "detached.pid")
	sentinel := filepath.Join(dir, "leaked")
	script := detachedNativeScript(t, 2*time.Second)

	helper := exec.Command(os.Args[0], "-test.run", "^TestLinuxSupervisorAdapterDeathContainsDetachedDescendant$")
	helper.Env = append(os.Environ(),
		adapterDeathHelperEnv+"=1",
		detachedNativePathEnv+"="+script,
		detachedPIDFileEnv+"="+pidFile,
		detachedSentinelEnv+"="+sentinel,
		detachedAgentDirEnv+"="+dir,
	)
	require.NoError(t, helper.Run())

	pid := readProcessPID(t, pidFile)
	require.Eventually(t, func() bool { return !processPIDAlive(pid) }, 5*time.Second, 10*time.Millisecond)
	require.Never(t, func() bool {
		_, err := os.Stat(sentinel)

		return err == nil
	}, 2200*time.Millisecond, 20*time.Millisecond, "adapter-death descendant reached delayed side effect")
}

func TestLinuxSupervisorDeathCannotForgeContainmentProof(t *testing.T) {
	dir := t.TempDir()
	pidFile := filepath.Join(dir, "detached.pid")
	sentinel := filepath.Join(dir, "leaked")
	script := detachedNativeScript(t, 20*time.Second)
	process, err := StartProcess(context.Background(), LaunchSpec{
		ExecutablePath: script,
		AgentDir:       dir,
		Env: map[string]string{
			"PI_DETACHED_PID_FILE": pidFile,
			"PI_DETACHED_SENTINEL": sentinel,
		},
	})
	require.NoError(t, err)

	pid := readProcessPID(t, pidFile)
	require.True(t, processPIDAlive(pid))
	require.NoError(t, process.cmd.Process.Kill())
	<-process.Exited()

	err = process.Close()
	require.ErrorIs(t, err, ErrProcessTreeNotQuiescent)
	require.False(t, ProcessTreeQuiescent(err))
	require.True(t, processPIDAlive(pid), "escaped child unexpectedly served as proof after supervisor death")

	require.NoError(t, syscall.Kill(pid, syscall.SIGKILL))
	require.Eventually(t, func() bool { return !processPIDAlive(pid) }, 5*time.Second, 10*time.Millisecond)
}

func detachedNativeScript(t *testing.T, delay time.Duration) string {
	t.Helper()

	return writeScript(t, fmt.Sprintf(`setsid /bin/sh -c 'trap "" INT TERM; echo $$ > "$1"; sleep %d; echo LEAK > "$2"' ignored "$PI_DETACHED_PID_FILE" "$PI_DETACHED_SENTINEL" &
trap '' INT TERM
while :; do sleep 1; done`, int(delay/time.Second)))
}

func readProcessPID(t *testing.T, path string) int {
	t.Helper()

	var pid int
	require.Eventually(t, func() bool {
		raw, err := os.ReadFile(path) // #nosec G304 -- private test path.
		if err != nil {
			return false
		}

		pid, err = strconv.Atoi(strings.TrimSpace(string(raw)))

		return err == nil && pid > 0
	}, 5*time.Second, 10*time.Millisecond)

	return pid
}

func processPIDAlive(pid int) bool {
	err := syscall.Kill(pid, 0)

	return err == nil || errors.Is(err, syscall.EPERM)
}

func TestProcessShutdownKillFailureSurfaces(t *testing.T) {
	process := startStubbornProcessWithSeams(t)
	process.tree.mu.Lock()
	process.tree.supervised = false
	process.tree.mu.Unlock()
	t.Cleanup(func() {
		process.tree.mu.Lock()
		process.tree.supervised = true
		process.tree.mu.Unlock()
	})

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
