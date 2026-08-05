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

	getpgid, kill := syscallGetpgid, syscallKill

	t.Cleanup(func() {
		syscallGetpgid, syscallKill = getpgid, kill
	})
}

// startStubbornProcessWithSeams launches a TERM-ignoring child for signal-seam
// tests. It uses a background context (never cancelled) so exec's context
// watcher cannot invoke the platform Cancel concurrently with the seam
// restore, and its cleanup restores the real syscalls before reaping.
func startStubbornProcessWithSeams(t *testing.T) *Process {
	t.Helper()

	getpgid, kill := syscallGetpgid, syscallKill
	startTree := processStartTree

	script := writeScript(t, `trap '' TERM; while :; do sleep 0.1; done`)

	processStartTree = func(launch *processTreeCommand) (*processTree, error) {
		tree, err := startTree(launch)
		if tree != nil {
			tree.supervised = false
		}

		return tree, err
	}
	process, err := func() (*Process, error) {
		defer func() { processStartTree = startTree }()

		return StartProcess(context.Background(), LaunchSpec{
			ExecutablePath:      script,
			AgentDir:            t.TempDir(),
			ShutdownStepTimeout: 50 * time.Millisecond,
			Containment:         testContainmentSpec(t),
		})
	}()
	require.NoError(t, err)

	t.Cleanup(func() {
		syscallGetpgid, syscallKill = getpgid, kill

		_ = process.Kill()
		<-process.Exited()
		process.tree.mu.Lock()
		process.tree.supervised = true
		process.tree.mu.Unlock()
		_ = process.Close()
	})

	return process
}

func TestProcessSignalFailuresSurface(t *testing.T) {
	process := startStubbornProcessWithSeams(t)

	syscallKill = func(int, syscall.Signal) error { return syscall.EPERM }

	require.ErrorContains(t, process.Shutdown(t.Context()), "terminate pi process")
	require.ErrorContains(t, process.Kill(), "kill pi process")
}

func TestProcessKillReportsUnreapedRoot(t *testing.T) {
	process := &Process{tree: &processTree{}, exited: make(chan struct{})}

	require.ErrorIs(t, process.Kill(), ErrProcessContainmentIncomplete)
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

	script := filepath.Join(dir, "fake-pi")
	require.NoError(t, os.WriteFile(script, []byte(fmt.Sprintf(`#!/bin/sh
(trap '' TERM; while :; do sleep 1; done) &
echo $! > %s
echo 0.80.6
`, strconv.Quote(pidFile))), 0o700))

	version, err := probeVersionWithTestSpec(t, t.Context(), script)
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
	require.True(t, ProcessContainmentComplete(nil))
	require.False(t, ProcessContainmentComplete(ErrProcessContainmentIncomplete))

	tree := &processTree{pgid: 12345}
	syscallKill = func(int, syscall.Signal) error { return syscall.EPERM }
	require.ErrorIs(t, tree.terminateAndWait(time.Millisecond), ErrProcessContainmentIncomplete)

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
	require.ErrorIs(t, tree.terminateAndWait(time.Millisecond), ErrProcessContainmentIncomplete)

	syscallKill = func(_ int, signal syscall.Signal) error {
		if signal == 0 {
			return syscall.EPERM
		}

		return nil
	}
	require.ErrorIs(t, tree.terminateAndWait(time.Millisecond), ErrProcessContainmentIncomplete)
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
		require.ErrorIs(t, tree.completeBoundary(), ErrProcessContainmentIncomplete)
		require.ErrorIs(t, tree.completeBoundary(), ErrProcessContainmentIncomplete)
	})

	t.Run("deadline failure", func(t *testing.T) {
		boundary, err := os.CreateTemp(t.TempDir(), "boundary")
		require.NoError(t, err)
		tree := &processTree{supervised: true, boundary: boundary, status: bufio.NewReader(boundary)}
		require.ErrorContains(t, tree.completeBoundary(), "arm pi turn supervisor boundary")
	})

	for _, test := range []struct {
		name  string
		value string
		ok    bool
	}{
		{name: "eof"},
		{name: "invalid", value: "not-proof\n"},
		{name: "complete", value: turnSupervisorComplete, ok: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			boundary, writer, err := os.Pipe()
			require.NoError(t, err)
			if test.value != "" {
				_, err = io.WriteString(writer, test.value)
				require.NoError(t, err)
			}
			require.NoError(t, writer.Close())

			tree := &processTree{supervised: true, boundary: boundary, status: bufio.NewReader(boundary)}
			err = tree.completeBoundary()
			if test.ok {
				require.NoError(t, err)
				require.NoError(t, tree.completeBoundary())
			} else {
				require.ErrorIs(t, err, ErrProcessContainmentIncomplete)
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
		Containment:    testContainmentSpec(t),
	})
	require.ErrorIs(t, err, want)
	require.ErrorContains(t, err, "prepare pi process")

	_, err = probeVersionWithTestSpec(t, t.Context(), "/bin/true")
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
			Containment:    testContainmentSpec(t),
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

func TestLinuxSupervisorPeerDeathRetainsContainmentProof(t *testing.T) {
	dir := t.TempDir()
	pidFile := filepath.Join(dir, "detached.pid")
	sentinel := filepath.Join(dir, "leaked")
	script := detachedNativeScript(t, 20*time.Second)
	process, err := StartProcess(context.Background(), LaunchSpec{
		ExecutablePath: script,
		AgentDir:       dir,
		Containment:    testContainmentSpec(t),
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

	require.True(t, ProcessContainmentComplete(process.WaitErr()))
	require.NoError(t, process.Close())
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

	syscallKill = func(pgid int, signal syscall.Signal) error {
		if signal == syscall.SIGKILL {
			return syscall.EPERM
		}

		return syscall.Kill(pgid, signal)
	}

	require.ErrorContains(t, process.Shutdown(t.Context()), "kill pi process")
}

func TestConfigureProcessCommandPlatformAttributes(t *testing.T) {
	cmd := &exec.Cmd{}
	configureProcessCommandPlatform(cmd)
	require.NotNil(t, cmd.SysProcAttr)
	require.Nil(t, cmd.Cancel)
}
