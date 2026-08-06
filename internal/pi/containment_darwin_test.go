//go:build darwin

package pi

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

type darwinTestReadCloser struct {
	io.Reader
	closeErr error
	closed   bool
}

type darwinTestWriteCloser struct {
	bytes.Buffer
	closeErr error
	closed   bool
}

func restoreDarwinLaunchSeams(t *testing.T) {
	t.Helper()

	executable := darwinLaunchExecutable
	command := darwinLaunchCommand
	exit := darwinLaunchExit
	execNative := darwinLaunchExec
	input := darwinLaunchInput
	openFile := darwinLaunchOpenFile
	fcntl := darwinLaunchFcntl
	closeOnExec := darwinLaunchCloseOnExec
	createTemp := darwinLaunchCreateTemp
	chmod := darwinLaunchFileChmod
	encode := darwinLaunchEncodeConfig
	seek := darwinLaunchFileSeek
	remove := darwinLaunchRemove
	pipe := darwinLaunchPipe
	statusWait := darwinLaunchStatusWait
	t.Cleanup(func() {
		darwinLaunchExecutable = executable
		darwinLaunchCommand = command
		darwinLaunchExit = exit
		darwinLaunchExec = execNative
		darwinLaunchInput = input
		darwinLaunchOpenFile = openFile
		darwinLaunchFcntl = fcntl
		darwinLaunchCloseOnExec = closeOnExec
		darwinLaunchCreateTemp = createTemp
		darwinLaunchFileChmod = chmod
		darwinLaunchEncodeConfig = encode
		darwinLaunchFileSeek = seek
		darwinLaunchRemove = remove
		darwinLaunchPipe = pipe
		darwinLaunchStatusWait = statusWait
	})
}

func restoreDarwinProcessTreeSeams(t *testing.T) {
	t.Helper()
	getpgid := syscallGetpgid
	kill := syscallKill
	activate := activateProcessContainmentRecord
	groupSignal := darwinProcessGroupSignal
	directKill := darwinDirectProcessKill
	t.Cleanup(func() {
		syscallGetpgid = getpgid
		syscallKill = kill
		activateProcessContainmentRecord = activate
		darwinProcessGroupSignal = groupSignal
		darwinDirectProcessKill = directKill
	})
}

func (writer *darwinTestWriteCloser) Close() error {
	writer.closed = true

	return writer.closeErr
}

func (reader *darwinTestReadCloser) Close() error {
	reader.closed = true

	return reader.closeErr
}

func TestDarwinLaunchBootstrapProtocol(t *testing.T) {
	originalExec := darwinLaunchExec
	t.Cleanup(func() { darwinLaunchExec = originalExec })

	configBytes, err := json.Marshal(darwinLaunchConfig{
		Path: "/native/pi",
		Args: []string{"pi", "--version"},
		Env:  []string{"A=B"},
	})
	require.NoError(t, err)

	config := &darwinTestReadCloser{Reader: bytes.NewReader(configBytes)}
	gate := &darwinTestReadCloser{Reader: bytes.NewReader([]byte{1})}
	var got darwinLaunchConfig
	darwinLaunchExec = func(path string, args []string, environment []string) error {
		got = darwinLaunchConfig{Path: path, Args: args, Env: environment}

		return nil
	}
	require.NoError(t, runDarwinLaunchBootstrap(config, gate))
	require.True(t, config.closed)
	require.True(t, gate.closed)
	require.Equal(t, darwinLaunchConfig{Path: "/native/pi", Args: []string{"pi", "--version"}, Env: []string{"A=B"}}, got)

	for _, test := range []struct {
		name   string
		config *darwinTestReadCloser
		gate   *darwinTestReadCloser
		exec   func(string, []string, []string) error
	}{
		{name: "decode", config: &darwinTestReadCloser{Reader: strings.NewReader("{")}, gate: &darwinTestReadCloser{Reader: strings.NewReader("\x01")}},
		{name: "incomplete", config: &darwinTestReadCloser{Reader: strings.NewReader(`{}`)}, gate: &darwinTestReadCloser{Reader: strings.NewReader("\x01")}},
		{name: "gate eof", config: &darwinTestReadCloser{Reader: bytes.NewReader(configBytes)}, gate: &darwinTestReadCloser{Reader: strings.NewReader("")}},
		{name: "gate byte", config: &darwinTestReadCloser{Reader: bytes.NewReader(configBytes)}, gate: &darwinTestReadCloser{Reader: strings.NewReader("x")}},
		{name: "close", config: &darwinTestReadCloser{Reader: bytes.NewReader(configBytes), closeErr: errors.New("close config")}, gate: &darwinTestReadCloser{Reader: strings.NewReader("\x01")}},
		{name: "exec", config: &darwinTestReadCloser{Reader: bytes.NewReader(configBytes)}, gate: &darwinTestReadCloser{Reader: strings.NewReader("\x01")}, exec: func(string, []string, []string) error { return errors.New("exec") }},
	} {
		t.Run(test.name, func(t *testing.T) {
			darwinLaunchExec = test.exec
			if darwinLaunchExec == nil {
				darwinLaunchExec = func(string, []string, []string) error { return nil }
			}

			require.Error(t, runDarwinLaunchBootstrap(test.config, test.gate))
		})
	}
}

func TestDarwinLaunchBootstrapDispatch(t *testing.T) {
	originalInput, originalExit, originalExec := darwinLaunchInput, darwinLaunchExit, darwinLaunchExec
	t.Cleanup(func() {
		darwinLaunchInput, darwinLaunchExit, darwinLaunchExec = originalInput, originalExit, originalExec
	})
	t.Setenv(darwinLaunchBootstrapEnv, darwinLaunchBootstrapMode)
	setTestIsolationBootstrapEnv(t)
	darwinLaunchExec = func(string, []string, []string) error { return nil }

	var exits []int
	darwinLaunchExit = func(code int) { exits = append(exits, code) }
	failedStatus := &darwinTestWriteCloser{}
	darwinLaunchInput = func() (io.ReadCloser, io.ReadCloser, io.WriteCloser, error) {
		return nil, nil, failedStatus, errors.New("input")
	}
	darwinLaunchBootstrap()
	require.True(t, failedStatus.closed)
	require.Contains(t, failedStatus.String(), "input")

	config, err := json.Marshal(darwinLaunchConfig{Path: "/native/pi", Args: []string{"pi"}})
	require.NoError(t, err)
	successStatus := &darwinTestWriteCloser{}
	darwinLaunchInput = func() (io.ReadCloser, io.ReadCloser, io.WriteCloser, error) {
		return io.NopCloser(bytes.NewReader(config)), io.NopCloser(strings.NewReader("\x01")), successStatus, nil
	}
	darwinLaunchBootstrap()
	require.True(t, successStatus.closed)
	require.Empty(t, successStatus.String())
	require.Equal(t, []int{1, 0}, exits)
}

func TestDarwinLaunchBootstrapCommandAndGateLifecycle(t *testing.T) {
	require.NoError(t, (*processTreeCommand)(nil).releaseStartGate())
	(*processTreeCommand)(nil).abortStartGate()
	(*processTreeCommand)(nil).close()

	readGate, writeGate, err := os.Pipe()
	require.NoError(t, err)
	completion, completionWrite, err := os.Pipe()
	require.NoError(t, err)
	command := &processTreeCommand{startGate: writeGate, completion: completion}
	require.NoError(t, command.releaseStartGate())
	var release [1]byte
	_, err = io.ReadFull(readGate, release[:])
	require.NoError(t, err)
	require.Equal(t, byte(1), release[0])
	require.NoError(t, readGate.Close())
	command.abortStartGate()
	command.close()
	require.ErrorIs(t, completion.Close(), os.ErrClosed)
	require.NoError(t, completionWrite.Close())

	_, closedWrite, err := os.Pipe()
	require.NoError(t, err)
	require.NoError(t, closedWrite.Close())
	require.Error(t, (&processTreeCommand{startGate: closedWrite}).releaseStartGate())

	spec := testContainmentSpec(t)
	native := exec.Command("/usr/bin/true")
	native.Env = []string{"A=B", "acp_go_pi_internal_darwin_launch=1"}
	launch, err := prepareProcessTreeCommand(native, spec)
	require.NoError(t, err)
	require.True(t, launch.cmd.SysProcAttr.Setpgid)
	require.Equal(t, darwinPipeWait, launch.cmd.WaitDelay)
	require.Len(t, launch.inherited, 3)
	require.NotNil(t, launch.startGate)
	require.NotNil(t, launch.ready)
	require.Contains(t, launch.cmd.Env, darwinLaunchBootstrapEnv+"="+darwinLaunchBootstrapMode)

	var config darwinLaunchConfig
	require.NoError(t, json.NewDecoder(launch.inherited[0]).Decode(&config))
	require.Equal(t, darwinLaunchConfig{Path: "/usr/bin/true", Args: []string{"/usr/bin/true"}, Env: []string{"A=B"}}, config)
	_, statErr := os.Stat(launch.inherited[0].Name())
	require.ErrorIs(t, statErr, os.ErrNotExist)
	launch.close()
}

func TestDarwinLaunchPreparationFailures(t *testing.T) {
	wantErr := errors.New("injected Darwin launch failure")

	_, err := prepareProcessTreeCommand(exec.Command("/usr/bin/true"), ContainmentSpec{})
	require.ErrorIs(t, err, ErrProcessContainmentIncomplete)

	invalid := testContainmentSpec(t)
	invalid.RuntimeID = "bad"
	_, err = prepareProcessTreeCommand(exec.Command("/usr/bin/true"), invalid)
	require.ErrorIs(t, err, ErrProcessContainmentIncomplete)

	_, err = prepareProcessTreeCommand(&exec.Cmd{}, testContainmentSpec(t))
	require.ErrorContains(t, err, "command is incomplete")

	for _, test := range []struct {
		name  string
		setup func()
		want  string
	}{
		{name: "create config", setup: func() {
			darwinLaunchCreateTemp = func(string, string) (*os.File, error) { return nil, wantErr }
		}, want: "create Darwin native launch config"},
		{name: "chmod config", setup: func() {
			darwinLaunchFileChmod = func(*os.File, os.FileMode) error { return wantErr }
		}, want: "secure Darwin native launch config"},
		{name: "encode config", setup: func() {
			darwinLaunchEncodeConfig = func(*os.File, darwinLaunchConfig) error { return wantErr }
		}, want: "encode Darwin native launch config"},
		{name: "seek config", setup: func() {
			darwinLaunchFileSeek = func(*os.File, int64, int) (int64, error) { return 0, wantErr }
		}, want: "rewind Darwin native launch config"},
		{name: "unlink config", setup: func() {
			darwinLaunchRemove = func(string) error { return wantErr }
		}, want: "unlink Darwin native launch config"},
		{name: "gate pipe", setup: func() {
			darwinLaunchPipe = func() (*os.File, *os.File, error) { return nil, nil, wantErr }
		}, want: "create Darwin native launch gate"},
		{name: "status pipe", setup: func() {
			calls := 0
			darwinLaunchPipe = func() (*os.File, *os.File, error) {
				calls++
				if calls == 2 {
					return nil, nil, wantErr
				}

				return os.Pipe()
			}
		}, want: "create Darwin native launch status"},
		{name: "bootstrap executable", setup: func() {
			darwinLaunchExecutable = func() (string, error) { return "", wantErr }
		}, want: "resolve Darwin native launch bootstrap"},
	} {
		t.Run(test.name, func(t *testing.T) {
			restoreDarwinLaunchSeams(t)
			test.setup()
			_, prepareErr := prepareProcessTreeCommand(exec.Command("/usr/bin/true"), testContainmentSpec(t))
			require.ErrorContains(t, prepareErr, test.want)
		})
	}

	invalidIsolation := testContainmentSpec(t)
	invalidIsolation.Isolation = nil
	_, err = prepareProcessTreeCommand(exec.Command("/usr/bin/true"), invalidIsolation)
	require.ErrorContains(t, err, "prepare Darwin native launch isolation")
}

func TestDarwinLaunchInputAndStatusBranches(t *testing.T) {
	t.Run("missing inherited descriptor", func(t *testing.T) {
		restoreDarwinLaunchSeams(t)
		darwinLaunchOpenFile = func(uintptr, string) *os.File { return nil }
		config, gate, status, err := inheritedDarwinLaunchInput()
		require.Error(t, err)
		require.Nil(t, config)
		require.Nil(t, gate)
		require.Nil(t, status)
	})

	t.Run("inherited descriptors", func(t *testing.T) {
		restoreDarwinLaunchSeams(t)
		files := make([]*os.File, 3)
		for index := range files {
			file, err := os.CreateTemp(t.TempDir(), "inherited")
			require.NoError(t, err)
			files[index] = file
		}
		calls := 0
		darwinLaunchOpenFile = func(uintptr, string) *os.File {
			file := files[calls]
			calls++

			return file
		}
		closedOnExec := -1
		darwinLaunchCloseOnExec = func(fd int) error {
			closedOnExec = fd

			return nil
		}
		config, gate, status, err := inheritedDarwinLaunchInput()
		require.NoError(t, err)
		require.Equal(t, int(files[2].Fd()), closedOnExec)
		require.NoError(t, config.Close())
		require.NoError(t, gate.Close())
		require.NoError(t, status.Close())
	})

	t.Run("close-on-exec failure", func(t *testing.T) {
		restoreDarwinLaunchSeams(t)
		files := make([]*os.File, 3)
		for index := range files {
			file, err := os.CreateTemp(t.TempDir(), "inherited")
			require.NoError(t, err)
			files[index] = file
			t.Cleanup(func() { _ = file.Close() })
		}
		calls := 0
		darwinLaunchOpenFile = func(uintptr, string) *os.File {
			file := files[calls]
			calls++

			return file
		}
		want := errors.New("close-on-exec")
		darwinLaunchCloseOnExec = func(int) error { return want }
		_, _, _, err := inheritedDarwinLaunchInput()
		require.ErrorIs(t, err, want)
	})

	require.ErrorContains(t, awaitProcessTreeReady(&processTreeCommand{}), "status is unavailable")

	t.Run("deadline arm", func(t *testing.T) {
		file, err := os.CreateTemp(t.TempDir(), "status")
		require.NoError(t, err)
		require.ErrorContains(t, awaitProcessTreeReady(&processTreeCommand{ready: file}), "arm Darwin native launch status")
	})

	t.Run("read deadline", func(t *testing.T) {
		restoreDarwinLaunchSeams(t)
		darwinLaunchStatusWait = time.Millisecond
		read, write, err := os.Pipe()
		require.NoError(t, err)
		defer func() { _ = write.Close() }()
		require.ErrorContains(t, awaitProcessTreeReady(&processTreeCommand{ready: read}), "await Darwin native launch status")
	})

	t.Run("status limit", func(t *testing.T) {
		read, write, err := os.Pipe()
		require.NoError(t, err)
		go func() {
			_, _ = write.Write(bytes.Repeat([]byte{'x'}, 4097))
			_ = write.Close()
		}()
		require.ErrorContains(t, awaitProcessTreeReady(&processTreeCommand{ready: read}), "status limit")
	})

	t.Run("native failure", func(t *testing.T) {
		read, write, err := os.Pipe()
		require.NoError(t, err)
		_, err = write.WriteString("native failed\n")
		require.NoError(t, err)
		require.NoError(t, write.Close())
		require.ErrorContains(t, awaitProcessTreeReady(&processTreeCommand{ready: read}), "native failed")
	})

	t.Setenv("GORACE", "halt_on_error=1")
	require.NotContains(t, darwinLaunchBootstrapEnvironment(), "GORACE=halt_on_error=1")
}

func TestDarwinLaunchCloseOnExecChecked(t *testing.T) {
	restoreDarwinLaunchSeams(t)
	calls := 0
	darwinLaunchFcntl = func(_ uintptr, command int, argument int) (int, error) {
		calls++
		if calls == 1 {
			require.Equal(t, unix.F_GETFD, command)
			require.Zero(t, argument)

			return 0, nil
		}
		require.Equal(t, unix.F_SETFD, command)
		require.NotZero(t, argument&unix.FD_CLOEXEC)

		return 0, nil
	}
	require.NoError(t, setDarwinLaunchCloseOnExec(5))
	require.Equal(t, 2, calls)

	want := errors.New("fcntl")
	darwinLaunchFcntl = func(uintptr, int, int) (int, error) { return 0, want }
	require.ErrorIs(t, setDarwinLaunchCloseOnExec(5), want)
	calls = 0
	darwinLaunchFcntl = func(uintptr, int, int) (int, error) {
		calls++
		if calls == 1 {
			return 0, nil
		}

		return 0, want
	}
	require.ErrorIs(t, setDarwinLaunchCloseOnExec(5), want)
}

func TestDarwinSharedProcessTreeBoundaryBranches(t *testing.T) {
	require.True(t, ProcessContainmentComplete(nil))
	require.False(t, ProcessContainmentComplete(ErrProcessContainmentIncomplete))
	require.ErrorIs(t, (*directChildWait)(nil).await(time.Millisecond), ErrProcessContainmentIncomplete)
	require.ErrorIs(t, (&directChildWait{done: make(chan struct{})}).await(time.Millisecond), ErrProcessContainmentIncomplete)
	require.ErrorIs(t, (*directChildWait)(nil).awaitReaped(time.Millisecond), ErrProcessContainmentIncomplete)
	require.ErrorIs(t, (&directChildWait{done: make(chan struct{})}).awaitReaped(time.Millisecond), ErrProcessContainmentIncomplete)

	require.NoError(t, (&processTree{}).completeBoundary())
	require.ErrorIs(t, (&processTree{supervised: true}).completeBoundary(), ErrProcessContainmentIncomplete)

	regular, err := os.CreateTemp(t.TempDir(), "boundary")
	require.NoError(t, err)
	tree := &processTree{supervised: true, boundary: regular, status: bufio.NewReader(regular)}
	require.ErrorIs(t, tree.completeBoundary(), ErrProcessContainmentIncomplete)

	for _, test := range []struct {
		name    string
		message string
		wantErr bool
	}{
		{name: "eof", wantErr: true},
		{name: "invalid", message: "invalid\n", wantErr: true},
		{name: "complete", message: turnSupervisorComplete},
	} {
		t.Run(test.name, func(t *testing.T) {
			read, write, pipeErr := os.Pipe()
			require.NoError(t, pipeErr)
			if test.message != "" {
				_, pipeErr = io.WriteString(write, test.message)
				require.NoError(t, pipeErr)
			}
			require.NoError(t, write.Close())

			candidate := &processTree{supervised: true, boundary: read, status: bufio.NewReader(read)}
			boundaryErr := candidate.completeBoundary()
			if test.wantErr {
				require.ErrorIs(t, boundaryErr, ErrProcessContainmentIncomplete)
			} else {
				require.NoError(t, boundaryErr)
			}
		})
	}

	controlRead, controlWrite, err := os.Pipe()
	require.NoError(t, err)
	command := &processTreeCommand{control: controlWrite}
	command.close()
	require.NoError(t, controlRead.Close())

	parent := t.TempDir()
	registry := filepath.Join(parent, containmentRegistryName)
	require.NoError(t, os.Mkdir(registry, 0o700))
	require.ErrorIs(t, completeUnstartedContainment(containmentRecord{path: filepath.Join(registry, "missing.json")}), ErrProcessContainmentIncomplete)
}

func TestDarwinStartProcessTreeFailureBranches(t *testing.T) {
	t.Run("command start", func(t *testing.T) {
		launch := &processTreeCommand{cmd: exec.Command(filepath.Join(t.TempDir(), "missing"))}
		_, err := startProcessTree(launch)
		require.Error(t, err)
	})

	t.Run("invalid process group", func(t *testing.T) {
		restoreDarwinProcessTreeSeams(t)
		command := exec.Command("/bin/sleep", "60")
		configureProcessCommandPlatform(command)
		syscallGetpgid = func(int) (int, error) { return 0, syscall.EIO }
		_, err := startProcessTree(&processTreeCommand{cmd: command})
		require.ErrorIs(t, err, ErrProcessContainmentIncomplete)
		require.ErrorIs(t, err, syscall.EIO)
	})

	t.Run("release gate", func(t *testing.T) {
		restoreDarwinProcessTreeSeams(t)
		command := exec.Command("/bin/sleep", "60")
		configureProcessCommandPlatform(command)
		_, gate, err := os.Pipe()
		require.NoError(t, err)
		require.NoError(t, gate.Close())
		_, err = startProcessTree(&processTreeCommand{cmd: command, startGate: gate})
		require.ErrorContains(t, err, "release validated native launch")
	})

	t.Run("ready status", func(t *testing.T) {
		restoreDarwinProcessTreeSeams(t)
		command := exec.Command("/bin/sleep", "60")
		configureProcessCommandPlatform(command)
		_, err := startProcessTree(&processTreeCommand{cmd: command})
		require.ErrorContains(t, err, "status is unavailable")
	})
}

func TestDarwinVanishedLeaderFailureBranches(t *testing.T) {
	done := make(chan struct{})
	close(done)
	direct := &directChildWait{done: done, start: make(chan struct{})}

	t.Run("absent group record failure", func(t *testing.T) {
		restoreDarwinProcessTreeSeams(t)
		syscallKill = func(int, syscall.Signal) error { return syscall.ESRCH }
		launch := &processTreeCommand{
			cmd:         &exec.Cmd{Process: &os.Process{Pid: 8123}},
			containment: containmentRecord{path: filepath.Join(t.TempDir(), "missing", "record.json")},
		}
		_, handled, err := handleVanishedProcessGroupLeader(launch, direct)
		require.True(t, handled)
		require.ErrorIs(t, err, ErrProcessContainmentIncomplete)
	})

	t.Run("probe error is unhandled", func(t *testing.T) {
		restoreDarwinProcessTreeSeams(t)
		syscallKill = func(int, syscall.Signal) error { return syscall.EINVAL }
		launch := &processTreeCommand{cmd: &exec.Cmd{Process: &os.Process{Pid: 8124}}}
		tree, handled, err := handleVanishedProcessGroupLeader(launch, direct)
		require.Nil(t, tree)
		require.False(t, handled)
		require.NoError(t, err)
	})
}

func TestDarwinLaunchBootstrapScrubsPrivateModeBeforeLinkedExec(t *testing.T) {
	if os.Getenv("PI_TEST_DARWIN_LINKED_EXEC") == "1" {
		require.Empty(t, os.Getenv("ACP_GO_PI_INTERNAL_DARWIN_LAUNCH"))
		require.Empty(t, os.Getenv("acp_go_pi_internal_turn_supervisor"))

		return
	}

	spec := testContainmentSpec(t)
	native := exec.Command(os.Args[0], "-test.run=^TestDarwinLaunchBootstrapScrubsPrivateModeBeforeLinkedExec$")
	native.Env = append(os.Environ(),
		"PI_TEST_DARWIN_LINKED_EXEC=1",
		"ACP_GO_PI_INTERNAL_DARWIN_LAUNCH=1",
		"acp_go_pi_internal_turn_supervisor=1",
	)
	launch, err := prepareProcessTreeCommand(native, spec)
	require.NoError(t, err)
	launch.containment, err = prepareContainmentRecord(spec)
	require.NoError(t, err)
	completion, completionWrite, err := os.Pipe()
	require.NoError(t, err)
	launch.completion = completion

	tree, err := startProcessTree(launch)
	require.NoError(t, err)
	require.NotNil(t, tree.status)
	require.NoError(t, tree.direct.await(defaultProcessTreeWait))
	require.NoError(t, tree.terminateAndWait(defaultProcessTreeWait))
	require.NoError(t, tree.boundary.Close())
	require.NoError(t, completionWrite.Close())
}

func TestDarwinOriginalGroupESRCHStopsAllFurtherSignals(t *testing.T) {
	realSignal := darwinProcessGroupSignal
	t.Cleanup(func() { darwinProcessGroupSignal = realSignal })
	calls := 0
	darwinProcessGroupSignal = func(pid int, signal syscall.Signal) error {
		calls++
		require.Equal(t, -1234, pid)
		require.Equal(t, syscall.SIGTERM, signal)

		return syscall.ESRCH
	}
	done := make(chan struct{})
	close(done)
	tree := &processTree{pgid: 1234, direct: &directChildWait{done: done, start: make(chan struct{})}}
	require.NoError(t, runDarwinProcessGroupCleanup(tree))
	require.Equal(t, 1, calls)
}

func TestDarwinProcessGroupCleanupBranches(t *testing.T) {
	realSignal := darwinProcessGroupSignal
	realDirectKill := darwinDirectProcessKill
	t.Cleanup(func() {
		darwinProcessGroupSignal = realSignal
		darwinDirectProcessKill = realDirectKill
	})

	done := make(chan struct{})
	close(done)
	newTree := func() *processTree {
		return &processTree{pgid: 1234, direct: &directChildWait{done: done, start: make(chan struct{})}}
	}

	t.Run("default direct process kill", func(t *testing.T) {
		command := exec.Command("sleep", "30")
		require.NoError(t, command.Start())
		require.NoError(t, darwinDirectProcessKill(command.Process))
		require.Error(t, command.Wait())
	})

	t.Run("initial signal error", func(t *testing.T) {
		darwinProcessGroupSignal = func(int, syscall.Signal) error { return syscall.EINVAL }
		require.ErrorIs(t, runDarwinProcessGroupCleanup(newTree()), ErrProcessContainmentIncomplete)
	})

	t.Run("term poll error", func(t *testing.T) {
		calls := 0
		darwinProcessGroupSignal = func(_ int, signal syscall.Signal) error {
			calls++
			if signal == 0 {
				return syscall.EINVAL
			}

			return nil
		}
		require.ErrorIs(t, finishDarwinProcessGroupCleanup(newTree(), time.Now().Add(time.Second), false, nil), ErrProcessContainmentIncomplete)
		require.Equal(t, 1, calls)
	})

	t.Run("term poll absent", func(t *testing.T) {
		darwinProcessGroupSignal = func(_ int, signal syscall.Signal) error {
			if signal == 0 {
				return syscall.ESRCH
			}

			return nil
		}
		require.NoError(t, finishDarwinProcessGroupCleanup(newTree(), time.Now().Add(time.Second), false, nil))
	})

	t.Run("kill signal error", func(t *testing.T) {
		darwinProcessGroupSignal = func(_ int, signal syscall.Signal) error {
			if signal == syscall.SIGKILL {
				return syscall.EINVAL
			}

			return nil
		}
		require.ErrorIs(t, finishDarwinProcessGroupCleanup(newTree(), time.Now().Add(600*time.Millisecond), false, nil), ErrProcessContainmentIncomplete)
	})

	t.Run("kill observes absence", func(t *testing.T) {
		darwinProcessGroupSignal = func(_ int, signal syscall.Signal) error {
			if signal == syscall.SIGKILL {
				return syscall.ESRCH
			}

			return nil
		}
		require.NoError(t, finishDarwinProcessGroupCleanup(newTree(), time.Now().Add(2*time.Second), false, nil))
	})

	t.Run("final poll absent", func(t *testing.T) {
		probes := 0
		darwinProcessGroupSignal = func(_ int, signal syscall.Signal) error {
			if signal == 0 {
				probes++
				if probes > 50 {
					return syscall.ESRCH
				}
			}

			return nil
		}
		require.NoError(t, finishDarwinProcessGroupCleanup(newTree(), time.Now().Add(2*time.Second), false, nil))
	})

	t.Run("final poll remains", func(t *testing.T) {
		darwinProcessGroupSignal = func(int, syscall.Signal) error { return nil }
		require.ErrorIs(t, finishDarwinProcessGroupCleanup(newTree(), time.Now().Add(520*time.Millisecond), false, nil), ErrProcessContainmentIncomplete)
	})

	t.Run("final poll error", func(t *testing.T) {
		probes := 0
		darwinProcessGroupSignal = func(_ int, signal syscall.Signal) error {
			if signal == 0 {
				probes++
				if probes > 50 {
					return syscall.EINVAL
				}
			}

			return nil
		}
		require.ErrorIs(t, finishDarwinProcessGroupCleanup(newTree(), time.Now().Add(2*time.Second), false, nil), ErrProcessContainmentIncomplete)
	})

	t.Run("eperm remains observable", func(t *testing.T) {
		darwinProcessGroupSignal = func(int, syscall.Signal) error { return syscall.EPERM }
		absent, err := pollOriginalProcessGroup(1234, time.Now())
		require.NoError(t, err)
		require.False(t, absent)
	})

	t.Run("deadline before reap", func(t *testing.T) {
		tree := newTree()
		require.ErrorIs(t, tree.failCleanup(time.Now().Add(-time.Second), errors.New("cause")), ErrProcessContainmentIncomplete)
		require.ErrorIs(t, tree.finishGroupAbsent(time.Now().Add(-time.Second)), ErrProcessContainmentIncomplete)
	})

	t.Run("record completion failure", func(t *testing.T) {
		tree := newTree()
		tree.containment.path = filepath.Join(t.TempDir(), "missing", "record.json")
		require.ErrorIs(t, tree.finishGroupAbsent(time.Now().Add(time.Second)), ErrProcessContainmentIncomplete)
	})

	t.Run("direct reap timeout", func(t *testing.T) {
		tree := &processTree{direct: &directChildWait{done: make(chan struct{})}}
		require.ErrorIs(t, tree.finishGroupAbsent(time.Now().Add(time.Millisecond)), ErrProcessContainmentIncomplete)
	})

	t.Run("cleanup error force kills direct child and waits for reap", func(t *testing.T) {
		direct := &directChildWait{done: make(chan struct{}), start: make(chan struct{})}
		direct.begin()
		darwinProcessGroupSignal = func(int, syscall.Signal) error { return syscall.EINVAL }
		darwinDirectProcessKill = func(process *os.Process) error {
			require.Equal(t, 1234, process.Pid)
			close(direct.done)

			return nil
		}
		tree := &processTree{pgid: 1234, process: &os.Process{Pid: 1234}, direct: direct}
		require.ErrorIs(t, finishDarwinProcessGroupCleanup(tree, time.Now().Add(time.Second), false, errors.New("probe failed")), ErrProcessContainmentIncomplete)
		select {
		case <-direct.done:
		default:
			t.Fatal("direct child waiter did not join after forced kill")
		}
	})

	t.Run("completed waiter prevents direct pid reuse signal", func(t *testing.T) {
		done := make(chan struct{})
		close(done)
		darwinDirectProcessKill = func(*os.Process) error {
			t.Fatal("completed direct child PID was signalled after reap")

			return nil
		}
		tree := &processTree{pgid: 1234, process: &os.Process{Pid: 1234}, direct: &directChildWait{done: done}}
		require.NoError(t, forceKillDarwinDirectChild(tree))
	})

	t.Run("direct process fallback branches", func(t *testing.T) {
		require.ErrorIs(t, forceKillDarwinDirectChild(&processTree{}), ErrProcessContainmentIncomplete)
		pending := &directChildWait{done: make(chan struct{})}
		require.ErrorIs(t, forceKillDarwinDirectChild(&processTree{direct: pending}), ErrProcessContainmentIncomplete)

		process := &os.Process{Pid: 1234}
		for _, killErr := range []error{nil, os.ErrProcessDone, syscall.ESRCH} {
			darwinDirectProcessKill = func(*os.Process) error { return killErr }
			require.NoError(t, forceKillDarwinDirectChild(&processTree{process: process, direct: pending}))
		}

		darwinDirectProcessKill = func(*os.Process) error { return syscall.EINVAL }
		require.ErrorContains(t, forceKillDarwinDirectChild(&processTree{process: process, direct: pending}), "signal direct child 1234")
	})

	require.ErrorIs(t, forceKillDarwinDirectChild(nil), ErrProcessContainmentIncomplete)

	require.Equal(t, time.Unix(1, 0), minTime(time.Unix(1, 0), time.Unix(2, 0)))
	require.Equal(t, time.Unix(1, 0), minTime(time.Unix(2, 0), time.Unix(1, 0)))
}

func TestSignalOriginalProcessGroupPermissionDenied(t *testing.T) {
	realSignal := darwinProcessGroupSignal
	t.Cleanup(func() { darwinProcessGroupSignal = realSignal })
	darwinProcessGroupSignal = func(int, syscall.Signal) error { return syscall.EPERM }

	absent, err := signalOriginalProcessGroup(1234, syscall.SIGTERM)
	require.NoError(t, err)
	require.False(t, absent)
}

func TestDarwinActivationFailureCleansCapturedGroup(t *testing.T) {
	realActivate := activateProcessContainmentRecord
	t.Cleanup(func() { activateProcessContainmentRecord = realActivate })
	parent := t.TempDir()
	root, err := os.MkdirTemp(parent, "acp-go-pi-runtime-")
	require.NoError(t, err)
	pidFile := filepath.Join(parent, "descendant.pid")
	script := filepath.Join(parent, "pi-helper")
	require.NoError(t, os.WriteFile(script, []byte("#!/bin/sh\necho $$ > \"$PI_TEST_PID_FILE\"\nsleep 60\n"), 0o700))
	var helperPID, helperPGID int
	activateProcessContainmentRecord = func(_ containmentRecord, pid int, pgid int) error {
		helperPID, helperPGID = pid, pgid

		return errors.New("injected activation failure")
	}

	_, err = StartProcess(t.Context(), LaunchSpec{
		ExecutablePath: script,
		AgentDir:       filepath.Join(root, "agent"),
		SessionDir:     filepath.Join(root, "sessions"),
		Env:            map[string]string{"PI_TEST_PID_FILE": pidFile},
		Containment: ContainmentSpec{
			DarwinBestEffort: true, ScratchParent: parent, GenerationRoot: root,
			RuntimeID: strings.Repeat("d", 32), LifecycleKind: "session",
			Isolation: testContainmentSpec(t).Isolation,
		},
	})
	require.ErrorIs(t, err, ErrProcessContainmentIncomplete)
	require.ErrorContains(t, err, "injected activation failure")
	require.Positive(t, helperPID)
	require.Equal(t, helperPID, helperPGID)
	require.ErrorIs(t, unix.Kill(helperPID, 0), syscall.ESRCH)
	require.ErrorIs(t, unix.Kill(-helperPGID, 0), syscall.ESRCH)
	_, statErr := os.Stat(pidFile)
	require.ErrorIs(t, statErr, os.ErrNotExist, "native script must remain behind the launch gate")
}

func TestDarwinFastExitBeforeGetpgid(t *testing.T) {
	for _, test := range []struct {
		name           string
		probe          error
		wantIncomplete bool
	}{
		{name: "group absent", probe: syscall.ESRCH},
		{name: "group still observable"},
	} {
		t.Run(test.name, func(t *testing.T) {
			realGetpgid, realKill, realGroupSignal := syscallGetpgid, syscallKill, darwinProcessGroupSignal
			t.Cleanup(func() {
				syscallGetpgid, syscallKill, darwinProcessGroupSignal = realGetpgid, realKill, realGroupSignal
			})
			syscallGetpgid = func(int) (int, error) { return 0, syscall.ESRCH }
			probeCalls := 0
			syscallKill = func(pid int, signal syscall.Signal) error {
				probeCalls++
				require.Less(t, pid, 0)
				require.Equal(t, syscall.Signal(0), signal)

				return test.probe
			}
			darwinProcessGroupSignal = func(int, syscall.Signal) error { return syscall.ESRCH }
			spec := testContainmentSpec(t)
			process, err := StartProcess(t.Context(), LaunchSpec{
				ExecutablePath: "/usr/bin/true", AgentDir: filepath.Join(spec.GenerationRoot, "agent"),
				SessionDir: filepath.Join(spec.GenerationRoot, "sessions"), Containment: spec,
			})
			require.Equal(t, 1, probeCalls)
			require.Error(t, err)
			require.Equal(t, test.wantIncomplete, errors.Is(err, ErrProcessContainmentIncomplete))
			require.Nil(t, process)
			record, readErr := readContainmentRecord(filepath.Join(spec.ScratchParent, containmentRegistryName, spec.RuntimeID+".json"))
			require.NoError(t, readErr)
			require.Equal(t, containmentStateAbsent, record.State)
			require.Nil(t, record.DirectChildPID)
		})
	}
}

func TestDarwinCleanupSignalsBeforeReleasingDirectWaiter(t *testing.T) {
	restoreDarwinProcessTreeSeams(t)
	direct := &directChildWait{done: make(chan struct{}), start: make(chan struct{})}
	go func() {
		<-direct.start
		close(direct.done)
	}()
	darwinProcessGroupSignal = func(_ int, signal syscall.Signal) error {
		require.Equal(t, syscall.SIGTERM, signal)
		select {
		case <-direct.start:
			t.Fatal("direct waiter was released before the captured group was signalled")
		default:
		}

		return syscall.ESRCH
	}
	tree := &processTree{pgid: 123, direct: direct}
	require.NoError(t, runDarwinProcessGroupCleanup(tree))
}

func TestDarwinFastExitRaceStress(t *testing.T) {
	const (
		workers    = 8
		iterations = 16
	)

	parent := t.TempDir()
	start := make(chan struct{})
	errs := make(chan error, workers*iterations)
	var group sync.WaitGroup

	isolation := testContainmentSpec(t).Isolation
	for worker := range workers {
		group.Add(1)

		go func() {
			defer group.Done()
			<-start

			for iteration := range iterations {
				root, err := os.MkdirTemp(parent, "acp-go-pi-runtime-")
				if err != nil {
					errs <- fmt.Errorf("create generation root: %w", err)

					continue
				}

				runtimeID := fmt.Sprintf("%032x", worker*iterations+iteration+1)
				process, err := StartProcess(t.Context(), LaunchSpec{
					ExecutablePath: "/usr/bin/true",
					AgentDir:       filepath.Join(root, "agent"),
					SessionDir:     filepath.Join(root, "sessions"),
					Containment: ContainmentSpec{
						DarwinBestEffort: true,
						ScratchParent:    parent,
						GenerationRoot:   root,
						RuntimeID:        runtimeID,
						LifecycleKind:    "session",
						Isolation:        isolation,
					},
				})
				if err != nil {
					errs <- fmt.Errorf("start fast-exit process %s: %w", runtimeID, err)

					continue
				}

				select {
				case <-process.Exited():
					if waitErr := process.WaitErr(); waitErr != nil {
						errs <- fmt.Errorf("wait fast-exit process %s: %w", runtimeID, waitErr)
					}
				case <-time.After(10 * time.Second):
					errs <- fmt.Errorf("wait fast-exit process %s: timeout", runtimeID)
				}
				if closeErr := process.Close(); closeErr != nil {
					errs <- fmt.Errorf("close fast-exit process %s: %w", runtimeID, closeErr)
				}
			}
		}()
	}

	close(start)
	group.Wait()
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}
}

func setTestIsolationBootstrapEnv(t *testing.T) {
	t.Helper()
	t.Setenv(envIsolationUID, strconv.Itoa(os.Geteuid()))
	t.Setenv(envIsolationGID, strconv.Itoa(os.Getegid()))
	t.Setenv(envIsolationTest, "true")
}
