package piacp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	internalpi "github.com/savid/acp-go-pi/internal/pi"
)

type edgeHostAuthority struct {
	environment func() map[string]string
	prepare     func(context.Context, string) error
	reclaim     func(context.Context, string) error
	start       func(context.Context, NativeRequest) (NativeProcess, error)
}

func (a *edgeHostAuthority) NativeEnvironment() map[string]string {
	if a.environment != nil {
		return a.environment()
	}

	return map[string]string{}
}

func (a *edgeHostAuthority) PrepareNativeTree(ctx context.Context, root string) error {
	if a.prepare != nil {
		return a.prepare(ctx, root)
	}

	return nil
}

func (a *edgeHostAuthority) ReclaimNativeTree(ctx context.Context, root string) error {
	if a.reclaim != nil {
		return a.reclaim(ctx, root)
	}

	return nil
}

func (a *edgeHostAuthority) StartNative(ctx context.Context, request NativeRequest) (NativeProcess, error) {
	if a.start != nil {
		return a.start(ctx, request)
	}

	return &edgeNativeProcess{}, nil
}

type edgeNativeProcess struct {
	stdin        io.WriteCloser
	stdout       io.ReadCloser
	stderr       io.ReadCloser
	wait         func(context.Context) (NativeResult, error)
	revoke       func(context.Context) error
	panicStreams bool
	nilStreams   bool
}

type valueAuthority struct{}

func (valueAuthority) NativeEnvironment() map[string]string            { return map[string]string{} }
func (valueAuthority) PrepareNativeTree(context.Context, string) error { return nil }
func (valueAuthority) ReclaimNativeTree(context.Context, string) error { return nil }
func (valueAuthority) StartNative(context.Context, NativeRequest) (NativeProcess, error) {
	return valueNativeProcess{}, nil
}

type valueNativeProcess struct{}

func (valueNativeProcess) Stdin() io.WriteCloser                      { return discardWriteCloser{Writer: io.Discard} }
func (valueNativeProcess) Stdout() io.ReadCloser                      { return io.NopCloser(&emptyReader{}) }
func (valueNativeProcess) Stderr() io.ReadCloser                      { return io.NopCloser(&emptyReader{}) }
func (valueNativeProcess) Wait(context.Context) (NativeResult, error) { return NativeResult{}, nil }
func (valueNativeProcess) Revoke(context.Context) error               { return nil }

func (p *edgeNativeProcess) Stdin() io.WriteCloser {
	if p.panicStreams {
		panic("stdin")
	}
	if p.nilStreams {
		return nil
	}
	if p.stdin != nil {
		return p.stdin
	}

	return discardWriteCloser{Writer: io.Discard}
}

func (p *edgeNativeProcess) Stdout() io.ReadCloser {
	if p.panicStreams {
		panic("stdout")
	}
	if p.nilStreams {
		return nil
	}
	if p.stdout != nil {
		return p.stdout
	}

	return io.NopCloser(&emptyReader{})
}

func (p *edgeNativeProcess) Stderr() io.ReadCloser {
	if p.panicStreams {
		panic("stderr")
	}
	if p.nilStreams {
		return nil
	}
	if p.stderr != nil {
		return p.stderr
	}

	return io.NopCloser(&emptyReader{})
}

func (p *edgeNativeProcess) Wait(ctx context.Context) (NativeResult, error) {
	if p.wait != nil {
		return p.wait(ctx)
	}

	return NativeResult{}, nil
}

func (p *edgeNativeProcess) Revoke(ctx context.Context) error {
	if p.revoke != nil {
		return p.revoke(ctx)
	}

	return nil
}

type emptyReader struct{}

func (*emptyReader) Read([]byte) (int, error) { return 0, io.EOF }

type errorWriteCloser struct{ err error }

func (c *errorWriteCloser) Write([]byte) (int, error) { return 0, c.err }
func (c *errorWriteCloser) Close() error              { return c.err }

type errorReadCloser struct{ err error }

func (c *errorReadCloser) Read([]byte) (int, error) { return 0, io.EOF }
func (c *errorReadCloser) Close() error             { return c.err }

type blockingEdgeReadCloser struct {
	startedOnce sync.Once
	closeOnce   sync.Once
	started     chan struct{}
	closed      chan struct{}
	exited      chan struct{}
}

func newBlockingEdgeReadCloser() *blockingEdgeReadCloser {
	return &blockingEdgeReadCloser{
		started: make(chan struct{}), closed: make(chan struct{}), exited: make(chan struct{}),
	}
}

func (r *blockingEdgeReadCloser) Read([]byte) (int, error) {
	r.startedOnce.Do(func() { close(r.started) })
	<-r.closed
	close(r.exited)

	return 0, io.EOF
}

func (r *blockingEdgeReadCloser) Close() error {
	r.closeOnce.Do(func() { close(r.closed) })

	return nil
}

type signalingWriteCloser struct {
	sync.Once
	closed chan struct{}
}

type emptyWaitMultiError struct{}

func (emptyWaitMultiError) Error() string   { return "empty wait error" }
func (emptyWaitMultiError) Unwrap() []error { return nil }

func (w *signalingWriteCloser) Write(data []byte) (int, error) { return len(data), nil }
func (w *signalingWriteCloser) Close() error {
	w.Do(func() { close(w.closed) })

	return nil
}

func TestHostAuthorityRuntimeEdges(t *testing.T) {
	var nilAuthority *edgeHostAuthority
	require.True(t, hostAuthorityNil(nil))
	require.True(t, hostAuthorityNil(nilAuthority))
	require.False(t, hostAuthorityNil(&edgeHostAuthority{}))
	require.False(t, hostAuthorityNil(valueAuthority{}))

	_, err := readHostEnvironment(nil)
	require.ErrorIs(t, err, ErrHostAuthorityUnavailable)
	_, err = readHostEnvironment(&edgeHostAuthority{environment: func() map[string]string { panic("environment") }})
	require.ErrorIs(t, err, ErrHostAuthorityUnavailable)
	_, err = readHostEnvironment(&edgeHostAuthority{environment: func() map[string]string { return nil }})
	require.ErrorIs(t, err, ErrHostAuthorityUnavailable)
	environment, err := readHostEnvironment(&edgeHostAuthority{environment: func() map[string]string {
		return map[string]string{"PATH": "/native"}
	}})
	require.NoError(t, err)
	require.Equal(t, "/native", environment["PATH"])

	ordinary := NewAgent()
	require.NoError(t, ordinary.prepareNativeTree(t.Context(), "/unused"))
	require.NoError(t, ordinary.reclaimNativeTree(t.Context(), "/unused"))
	require.NoError(t, ordinary.disposeNativeTree(t.Context(), ""))
	require.NoError(t, ordinary.removeNativeTree(""))

	originalRemoveAll := materializeRemoveAll
	t.Cleanup(func() { materializeRemoveAll = originalRemoveAll })
	removed := ""
	materializeRemoveAll = func(root string) error {
		removed = root

		return nil
	}
	require.NoError(t, ordinary.disposeNativeTree(t.Context(), "/ordinary"))
	require.Equal(t, "/ordinary", removed)
	require.NoError(t, ordinary.removeNativeTree("/removed"))
	require.Equal(t, "/removed", removed)

	wantErr := errors.New("authority fault")
	managed := NewAgent()
	managed.options.hostAuthoritySupplied = true
	managed.options.HostAuthority = &edgeHostAuthority{
		prepare: func(context.Context, string) error { return wantErr },
		reclaim: func(context.Context, string) error { return wantErr },
	}
	require.ErrorIs(t, managed.prepareNativeTree(t.Context(), "/tree"), wantErr)
	require.ErrorIs(t, managed.reclaimNativeTree(t.Context(), "/tree"), wantErr)
	require.ErrorIs(t, managed.disposeNativeTree(t.Context(), "/tree"), wantErr)

	managed.options.HostAuthority = &edgeHostAuthority{
		prepare: func(context.Context, string) error { panic("prepare") },
		reclaim: func(context.Context, string) error { panic("reclaim") },
	}
	require.ErrorIs(t, managed.prepareNativeTree(t.Context(), "/panic"), ErrHostAuthorityUnavailable)
	require.ErrorIs(t, managed.reclaimNativeTree(t.Context(), "/panic"), ErrHostAuthorityUnavailable)

	managed.options.HostAuthority = &edgeHostAuthority{reclaim: func(context.Context, string) error {
		return ErrNativeTreeBusy
	}}
	require.ErrorIs(t, managed.reclaimNativeTree(t.Context(), "/busy"), ErrNativeTreeBusy)
	managed.options.HostAuthority = &edgeHostAuthority{}
	require.NoError(t, managed.reclaimNativeTree(t.Context(), "/busy"))

	var nilProcess *edgeNativeProcess
	require.True(t, nativeProcessNil(nil))
	require.True(t, nativeProcessNil(nilProcess))
	require.False(t, nativeProcessNil(&edgeNativeProcess{}))
	require.False(t, nativeProcessNil(valueNativeProcess{}))
	require.True(t, nativeContainmentComplete(nil))
	require.False(t, nativeContainmentComplete(ErrContainmentIncomplete))
}

func TestRuntimeGenerationFailureEdges(t *testing.T) {
	originalParent := runtimeGenerationEnsureScratchParent
	originalAbs := runtimeGenerationAbs
	originalMkdirTemp := runtimeGenerationMkdirTemp
	originalChmod := runtimeGenerationChmod
	originalRemoveAll := runtimeGenerationRemoveAll
	originalWrite := runtimeGenerationWriteProbeAgentDir
	t.Cleanup(func() {
		runtimeGenerationEnsureScratchParent = originalParent
		runtimeGenerationAbs = originalAbs
		runtimeGenerationMkdirTemp = originalMkdirTemp
		runtimeGenerationChmod = originalChmod
		runtimeGenerationRemoveAll = originalRemoveAll
		runtimeGenerationWriteProbeAgentDir = originalWrite
	})

	_, err := (*runtimeGeneration)(nil).prepareVersionProbeAgentDir(t.Context())
	require.Error(t, err)
	_, err = (&runtimeGeneration{root: "relative"}).prepareVersionProbeAgentDir(t.Context())
	require.Error(t, err)

	wantErr := errors.New("generation fault")
	root := filepath.Join(t.TempDir(), "generation")
	generation := &runtimeGeneration{agent: NewAgent(), root: root}
	runtimeGenerationWriteProbeAgentDir = func(string) error { return wantErr }
	_, err = generation.prepareVersionProbeAgentDir(t.Context())
	require.ErrorIs(t, err, wantErr)
	runtimeGenerationWriteProbeAgentDir = originalWrite
	require.NoError(t, os.MkdirAll(root, 0o700))
	agentDir, err := generation.prepareVersionProbeAgentDir(t.Context())
	require.NoError(t, err)
	require.Equal(t, filepath.Join(root, "probe-agent"), agentDir)
	prepareAgent := NewAgent()
	prepareAgent.options.hostAuthoritySupplied = true
	prepareAgent.options.HostAuthority = &edgeHostAuthority{prepare: func(context.Context, string) error {
		return wantErr
	}}
	prepareRoot := filepath.Join(t.TempDir(), "prepared")
	require.NoError(t, os.MkdirAll(prepareRoot, 0o700))
	_, err = (&runtimeGeneration{agent: prepareAgent, root: prepareRoot}).prepareVersionProbeAgentDir(t.Context())
	require.ErrorIs(t, err, wantErr)

	closed := NewAgent()
	closed.closed = true
	_, err = closed.createRuntimeGeneration(t.Context())
	require.Error(t, err)

	agent := NewAgent()
	runtimeGenerationEnsureScratchParent = func(string) (string, error) { return "", wantErr }
	_, err = agent.createRuntimeGeneration(t.Context())
	require.ErrorIs(t, err, wantErr)
	runtimeGenerationEnsureScratchParent = func(string) (string, error) { return "/parent", nil }
	runtimeGenerationAbs = func(string) (string, error) { return "", wantErr }
	_, err = agent.createRuntimeGeneration(t.Context())
	require.ErrorIs(t, err, wantErr)
	runtimeGenerationAbs = func(path string) (string, error) { return path, nil }
	runtimeGenerationMkdirTemp = func(string, string) (string, error) { return "", wantErr }
	_, err = agent.createRuntimeGeneration(t.Context())
	require.ErrorIs(t, err, wantErr)

	removed := ""
	runtimeGenerationMkdirTemp = func(string, string) (string, error) { return "/root", nil }
	runtimeGenerationChmod = func(string, os.FileMode) error { return wantErr }
	runtimeGenerationRemoveAll = func(path string) error {
		removed = path

		return nil
	}
	_, err = agent.createRuntimeGeneration(t.Context())
	require.ErrorIs(t, err, wantErr)
	require.Equal(t, "/root", removed)

	require.ErrorIs(t, (*runtimeGeneration)(nil).finalize(t.Context(), wantErr), wantErr)
	incomplete := &runtimeGeneration{agent: agent, root: "/incomplete"}
	require.ErrorIs(t, incomplete.finalize(t.Context(), ErrContainmentIncomplete), ErrContainmentIncomplete)

	removeErr := errors.New("remove fault")
	runtimeGenerationRemoveAll = func(string) error { return removeErr }
	complete := &runtimeGeneration{agent: agent, root: "/complete"}
	require.ErrorIs(t, complete.finalize(t.Context(), nil), removeErr)
	require.ErrorIs(t, complete.finalize(t.Context(), nil), removeErr)

	busyAgent := NewAgent()
	busyAgent.options.hostAuthoritySupplied = true
	busyAgent.options.HostAuthority = &edgeHostAuthority{reclaim: func(context.Context, string) error {
		return ErrNativeTreeBusy
	}}
	busy := &runtimeGeneration{agent: busyAgent, root: "/busy", prepared: true}
	require.ErrorIs(t, busy.finalize(t.Context(), nil), ErrNativeTreeBusy)

	successAgent := NewAgent()
	successAgent.options.hostAuthoritySupplied = true
	successAgent.options.HostAuthority = &edgeHostAuthority{}
	runtimeGenerationRemoveAll = func(string) error { return nil }
	success := &runtimeGeneration{agent: successAgent, root: "/success", prepared: true}
	require.NoError(t, success.finalize(t.Context(), nil))
	require.False(t, success.prepared)

	runtimeGenerationEnsureScratchParent = originalParent
	runtimeGenerationAbs = originalAbs
	runtimeGenerationMkdirTemp = originalMkdirTemp
	runtimeGenerationChmod = originalChmod
	runtimeGenerationRemoveAll = originalRemoveAll
	created, err := (&Agent{scratchParent: t.TempDir()}).createRuntimeGeneration(t.Context())
	require.NoError(t, err)
	require.NoError(t, created.finalize(t.Context(), nil))
}

func edgeManagedAgent(authority HostAuthority) *Agent {
	agent := NewAgent()
	agent.options.hostAuthoritySupplied = true
	agent.options.HostAuthority = authority
	agent.nativeEnvironment = map[string]string{"PATH": "/native"}

	return agent
}

func TestAuthorityProcessStartFailureEdges(t *testing.T) {
	spec := internalpi.LaunchSpec{ExecutablePath: "pi", AgentDir: "/agent"}

	blocked := edgeManagedAgent(&edgeHostAuthority{})
	blocked.nativeContainmentErr = ErrContainmentIncomplete
	_, _, err := blocked.startAuthorityPiProcess(t.Context(), spec)
	require.ErrorIs(t, err, ErrContainmentIncomplete)

	wantErr := errors.New("start fault")
	for _, authority := range []*edgeHostAuthority{
		{start: func(context.Context, NativeRequest) (NativeProcess, error) { panic("start") }},
		{start: func(context.Context, NativeRequest) (NativeProcess, error) { return nil, wantErr }},
		{start: func(context.Context, NativeRequest) (NativeProcess, error) {
			return nil, nil //nolint:nilnil // Deliberately exercise an invalid authority response.
		}},
		{start: func(context.Context, NativeRequest) (NativeProcess, error) {
			return &edgeNativeProcess{panicStreams: true}, nil
		}},
		{start: func(context.Context, NativeRequest) (NativeProcess, error) {
			return &edgeNativeProcess{nilStreams: true}, nil
		}},
	} {
		agent := edgeManagedAgent(authority)
		_, _, startErr := agent.startAuthorityPiProcess(t.Context(), spec)
		require.Error(t, startErr)
	}

	done := make(chan struct{})
	process := &edgeNativeProcess{
		stdout: io.NopCloser(strings.NewReader("")),
		stderr: io.NopCloser(strings.NewReader("diagnostic")),
		wait: func(context.Context) (NativeResult, error) {
			<-done

			return NativeResult{}, nil
		},
		revoke: func(context.Context) error {
			close(done)

			return nil
		},
	}
	agent := edgeManagedAgent(&edgeHostAuthority{start: func(context.Context, NativeRequest) (NativeProcess, error) {
		return process, nil
	}})
	wrapper, _, err := agent.startAuthorityPiProcess(t.Context(), spec)
	require.NoError(t, err)
	require.ErrorContains(t, wrapper.WaitErr(), "still running")
	require.NoError(t, wrapper.Kill())
	require.NoError(t, wrapper.Close())
	require.Contains(t, wrapper.StderrTail(), "diagnostic")
}

func TestAuthorityProcessControlAndPrimitiveEdges(t *testing.T) {
	wantErr := errors.New("native fault")
	waitFailureCalls := 0

	waitFailure := &authorityPiProcess{
		agent: NewAgent(), process: &edgeNativeProcess{wait: func(context.Context) (NativeResult, error) {
			waitFailureCalls++
			if waitFailureCalls == 1 {
				return NativeResult{}, wantErr
			}

			return NativeResult{Revoked: true}, nil
		}}, exited: make(chan struct{}),
	}
	waitFailure.startWait()
	<-waitFailure.exited
	require.ErrorIs(t, waitFailure.WaitErr(), wantErr)
	require.ErrorIs(t, waitFailure.WaitErr(), ErrContainmentIncomplete)
	require.NoError(t, waitFailure.Kill())
	require.NoError(t, waitFailure.WaitErr())
	require.Equal(t, 2, waitFailureCalls)

	exitFailure := &authorityPiProcess{
		agent: NewAgent(), process: &edgeNativeProcess{wait: func(context.Context) (NativeResult, error) {
			return NativeResult{ExitCode: 7}, nil
		}}, exited: make(chan struct{}),
	}
	exitFailure.startWait()
	<-exitFailure.exited
	require.ErrorContains(t, exitFailure.WaitErr(), "exit status 7")

	immediateExited := make(chan struct{})
	close(immediateExited)
	immediate := &authorityPiProcess{
		agent: NewAgent(), process: &edgeNativeProcess{},
		stdin: &errorWriteCloser{}, tail: &nativeStderrTail{limit: 4}, exited: immediateExited, terminal: true,
	}
	immediate.managedByHostAuthority()
	require.True(t, immediate.Exited() == immediateExited)
	_, _ = immediate.tail.Write([]byte("012345"))
	require.Equal(t, "2345", immediate.StderrTail())
	require.NoError(t, immediate.Shutdown(t.Context()))
	require.NoError(t, immediate.Kill())

	delayedDone := make(chan struct{})
	delayed := &authorityPiProcess{
		agent: NewAgent(),
		process: &edgeNativeProcess{
			wait: func(ctx context.Context) (NativeResult, error) {
				select {
				case <-delayedDone:
					return NativeResult{Revoked: true}, nil
				case <-ctx.Done():
					return NativeResult{}, ctx.Err()
				}
			},
			revoke: func(context.Context) error {
				close(delayedDone)

				return nil
			},
		},
		stdin: &errorWriteCloser{}, exited: make(chan struct{}),
	}
	delayed.startWait()
	require.NoError(t, delayed.Kill())

	shutdownDone := make(chan struct{})
	shutdown := &authorityPiProcess{
		agent: NewAgent(),
		process: &edgeNativeProcess{
			wait: func(ctx context.Context) (NativeResult, error) {
				select {
				case <-shutdownDone:
					return NativeResult{Revoked: true}, nil
				case <-ctx.Done():
					return NativeResult{}, ctx.Err()
				}
			},
			revoke: func(context.Context) error {
				close(shutdownDone)

				return nil
			},
		},
		stdin: &errorWriteCloser{}, exited: make(chan struct{}),
	}
	shutdown.startWait()
	require.NoError(t, shutdown.Shutdown(t.Context()))

	cancelDone := make(chan struct{})
	cancelled := &authorityPiProcess{
		agent: NewAgent(), process: &edgeNativeProcess{wait: func(ctx context.Context) (NativeResult, error) {
			select {
			case <-cancelDone:
				return NativeResult{}, nil
			case <-ctx.Done():
				return NativeResult{}, ctx.Err()
			}
		}},
		stdin: &errorWriteCloser{}, exited: make(chan struct{}),
	}
	cancelled.startWait()
	cancelCtx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, cancelled.Shutdown(cancelCtx), context.Canceled)
	require.ErrorIs(t, cancelled.Shutdown(cancelCtx), ErrContainmentIncomplete)
	close(cancelDone)
	require.NoError(t, cancelled.Shutdown(t.Context()))
	<-cancelled.exited

	originalKillTimeout := authorityProcessKillTimeout
	t.Cleanup(func() { authorityProcessKillTimeout = originalKillTimeout })
	authorityProcessKillTimeout = time.Nanosecond
	stuckDone := make(chan struct{})
	stuck := &authorityPiProcess{
		agent: NewAgent(),
		process: &edgeNativeProcess{wait: func(ctx context.Context) (NativeResult, error) {
			select {
			case <-stuckDone:
				return NativeResult{}, nil
			default:
			}

			select {
			case <-stuckDone:
				return NativeResult{}, nil
			case <-ctx.Done():
				return NativeResult{}, ctx.Err()
			}
		}},
		stdin: &errorWriteCloser{}, exited: make(chan struct{}),
	}
	stuck.startWait()
	require.ErrorIs(t, stuck.Kill(), ErrContainmentIncomplete)
	require.ErrorIs(t, stuck.Close(), ErrContainmentIncomplete)
	close(stuckDone)
	require.NoError(t, stuck.Close())
	<-stuck.exited

	streamDone := make(chan struct{})
	close(streamDone)
	streamErr := &authorityPiProcess{
		agent: NewAgent(), process: &edgeNativeProcess{}, stdin: &errorWriteCloser{},
		stdout: &errorReadCloser{err: wantErr}, stderr: &errorReadCloser{err: wantErr},
		exited: immediateExited, stderrDone: streamDone, terminal: true,
	}
	require.ErrorIs(t, streamErr.Close(), wantErr)
	require.ErrorIs(t, streamErr.Close(), wantErr)

	panicProcess := &edgeNativeProcess{
		wait:   func(context.Context) (NativeResult, error) { panic("wait") },
		revoke: func(context.Context) error { panic("revoke") },
	}
	_, err := waitNativeProcess(t.Context(), panicProcess)
	require.ErrorIs(t, err, ErrHostAuthorityUnavailable)
	require.ErrorIs(t, revokeNativeProcess(t.Context(), panicProcess), ErrHostAuthorityUnavailable)
	panicWrapper := &authorityPiProcess{agent: NewAgent(), process: panicProcess}
	require.ErrorIs(t, panicWrapper.revoke(t.Context()), ErrHostAuthorityUnavailable)

	require.NoError(t, terminalNativeClose(wantErr, nil))
	require.ErrorIs(t, terminalNativeClose(wantErr, ErrContainmentIncomplete), wantErr)
	require.ErrorIs(t, authorityTerminalRevokeError(ErrHostAuthorityUnavailable), ErrHostAuthorityUnavailable)
	require.NoError(t, authorityTerminalRevokeError(wantErr))
	require.ErrorIs(t, settleStartedNativeProcess(&edgeNativeProcess{wait: func(context.Context) (NativeResult, error) {
		return NativeResult{}, wantErr
	}}), ErrContainmentIncomplete)
}

func TestAuthorityWaitCallerObservesExactFlightAcrossTerminalSuccessor(t *testing.T) {
	wantErr := errors.New("first authority wait failed")
	secondStarted := make(chan struct{})
	releaseSecond := make(chan struct{})
	var callsMu sync.Mutex
	waitCalls := 0
	process := &edgeNativeProcess{wait: func(context.Context) (NativeResult, error) {
		callsMu.Lock()
		waitCalls++
		call := waitCalls
		callsMu.Unlock()
		if call == 1 {
			return NativeResult{}, wantErr
		}

		close(secondStarted)
		<-releaseSecond

		return NativeResult{Revoked: true}, nil
	}}
	wrapper := &authorityPiProcess{agent: NewAgent(), process: process, exited: make(chan struct{})}
	first := wrapper.startWait()
	<-first.done
	second := wrapper.startWait()
	<-secondStarted
	close(releaseSecond)
	<-second.done

	terminal, waitErr := wrapper.awaitWait(t.Context(), first)
	require.False(t, terminal)
	require.ErrorIs(t, waitErr, wantErr)
	require.ErrorIs(t, waitErr, ErrContainmentIncomplete)

	terminal, waitErr = wrapper.awaitWait(t.Context(), second)
	require.True(t, terminal)
	require.NoError(t, waitErr)
	require.NoError(t, wrapper.WaitErr())
}

func TestAuthorityProcessIncompleteCloseJoinsWaitAndStderrWorkers(t *testing.T) {
	originalKillTimeout := authorityProcessKillTimeout
	authorityProcessKillTimeout = time.Nanosecond
	t.Cleanup(func() { authorityProcessKillTimeout = originalKillTimeout })

	waitStarted := make(chan struct{})
	waitExited := make(chan struct{})
	process := &edgeNativeProcess{wait: func(ctx context.Context) (NativeResult, error) {
		close(waitStarted)
		<-ctx.Done()
		close(waitExited)

		return NativeResult{}, ctx.Err()
	}}
	stdin := &signalingWriteCloser{closed: make(chan struct{})}
	stdout := newBlockingEdgeReadCloser()
	stderr := newBlockingEdgeReadCloser()
	wrapper := &authorityPiProcess{
		agent: NewAgent(), process: process, stdin: stdin, stdout: stdout, stderr: stderr,
		tail: &nativeStderrTail{limit: 8 << 10}, exited: make(chan struct{}), stderrDone: make(chan struct{}),
	}
	go func() {
		defer close(wrapper.stderrDone)
		_, _ = io.Copy(wrapper.tail, stderr)
	}()
	wrapper.startWait()
	<-waitStarted
	<-stderr.started

	require.ErrorIs(t, wrapper.Close(), ErrContainmentIncomplete)
	<-waitExited
	<-stdin.closed
	<-stdout.closed
	<-stderr.exited
	select {
	case <-wrapper.Exited():
		t.Fatal("a detached Wait was published as terminal")
	default:
	}
}

func TestAuthorityProcessPublishesTerminalResultRacingWaitCancellation(t *testing.T) {
	waitStarted := make(chan struct{})
	waitExited := make(chan struct{})
	wantResult := NativeResult{ExitCode: -1, Signal: 9, Revoked: true}
	process := &edgeNativeProcess{wait: func(ctx context.Context) (NativeResult, error) {
		close(waitStarted)
		<-ctx.Done()
		close(waitExited)

		return wantResult, nil
	}}
	wrapper := &authorityPiProcess{
		agent: NewAgent(), process: process, stdin: &errorWriteCloser{}, exited: make(chan struct{}),
	}
	wrapper.startWait()
	<-waitStarted

	shutdownCtx, cancelShutdown := context.WithCancel(t.Context())
	cancelShutdown()
	require.NoError(t, wrapper.Shutdown(shutdownCtx))
	<-waitExited
	<-wrapper.Exited()
	require.Equal(t, wantResult, wrapper.result)
	require.NoError(t, wrapper.WaitErr())
}

func TestDetachedWaitErrorRequiresOnlyContextFailures(t *testing.T) {
	require.False(t, detachedWaitError(nil))
	require.False(t, detachedWaitError(ErrHostAuthorityUnavailable))
	require.False(t, detachedWaitError(ErrContainmentIncomplete))
	require.False(t, detachedWaitError(emptyWaitMultiError{}))
	require.True(t, detachedWaitError(errors.Join(context.Canceled, context.DeadlineExceeded)))
	require.False(t, detachedWaitError(errors.Join(context.Canceled, errors.New("independent failure"))))
	require.True(t, detachedWaitError(fmt.Errorf("wrapped: %w", context.Canceled)))
	require.False(t, detachedWaitError(errors.New("ordinary failure")))
}

func TestNativeVersionProbeEdges(t *testing.T) {
	ordinary := NewAgent()
	ordinary.ordinaryEnvironment = map[string]string{"PATH": t.TempDir()}
	_, err := ordinary.probeNativeVersion(t.Context(), "missing-pi", t.TempDir())
	require.Error(t, err)
	require.Equal(t, ordinary.ordinaryEnvironment, ordinary.nativeBaseEnvironment())

	wantErr := errors.New("probe fault")
	for _, authority := range []*edgeHostAuthority{
		{start: func(context.Context, NativeRequest) (NativeProcess, error) { panic("start") }},
		{start: func(context.Context, NativeRequest) (NativeProcess, error) { return nil, wantErr }},
		{start: func(context.Context, NativeRequest) (NativeProcess, error) {
			return nil, nil //nolint:nilnil // Deliberately exercise an invalid authority response.
		}},
		{start: func(context.Context, NativeRequest) (NativeProcess, error) {
			return &edgeNativeProcess{panicStreams: true}, nil
		}},
		{start: func(context.Context, NativeRequest) (NativeProcess, error) {
			return &edgeNativeProcess{nilStreams: true}, nil
		}},
	} {
		agent := edgeManagedAgent(authority)
		_, probeErr := agent.probeNativeVersion(t.Context(), "pi", "/agent")
		require.Error(t, probeErr)
	}

	probe := func(t *testing.T, process *edgeNativeProcess) (string, error) {
		t.Helper()

		agent := edgeManagedAgent(&edgeHostAuthority{start: func(_ context.Context, request NativeRequest) (NativeProcess, error) {
			require.Equal(t, "pi", request.Executable)
			require.Equal(t, []string{"--version"}, request.Arguments)

			return process, nil
		}})
		require.Equal(t, agent.nativeEnvironment, agent.nativeBaseEnvironment())

		return agent.probeNativeVersion(t.Context(), "pi", "/agent")
	}

	version, err := probe(t, &edgeNativeProcess{stdout: io.NopCloser(strings.NewReader(" 1.2.3\n"))})
	require.NoError(t, err)
	require.Equal(t, "1.2.3", version)

	version, err = probe(t, &edgeNativeProcess{
		stdout: io.NopCloser(strings.NewReader("ignored")), stderr: io.NopCloser(strings.NewReader(" bad version\n")),
		wait: func(context.Context) (NativeResult, error) { return NativeResult{ExitCode: 9}, nil },
	})
	require.Empty(t, version)
	require.ErrorContains(t, err, "exit status 9: bad version")

	version, err = probe(t, &edgeNativeProcess{})
	require.Empty(t, version)
	require.ErrorContains(t, err, "empty output")

	waitCalls := 0
	revokeCalls := 0
	revoked := false
	version, err = probe(t, &edgeNativeProcess{
		wait: func(context.Context) (NativeResult, error) {
			waitCalls++
			if waitCalls == 1 {
				return NativeResult{}, wantErr
			}

			require.True(t, revoked)

			return NativeResult{Revoked: true}, nil
		},
		revoke: func(context.Context) error {
			revokeCalls++
			revoked = true

			return nil
		},
	})
	require.Empty(t, version)
	require.ErrorIs(t, err, wantErr)
	require.NotErrorIs(t, err, ErrContainmentIncomplete)
	require.Equal(t, 2, waitCalls)
	require.Equal(t, 1, revokeCalls)

	cancelled := func(waitErr error) error {
		done := make(chan struct{})
		waitCalls := 0
		process := &edgeNativeProcess{
			wait: func(ctx context.Context) (NativeResult, error) {
				waitCalls++
				select {
				case <-done:
					return NativeResult{Revoked: true}, waitErr
				case <-ctx.Done():
					return NativeResult{}, ctx.Err()
				}
			},
			revoke: func(context.Context) error {
				close(done)

				return nil
			},
		}
		agent := edgeManagedAgent(&edgeHostAuthority{start: func(context.Context, NativeRequest) (NativeProcess, error) {
			return process, nil
		}})
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		_, probeErr := agent.probeNativeVersion(ctx, "pi", "/agent")
		require.Equal(t, 2, waitCalls)

		return probeErr
	}
	require.ErrorIs(t, cancelled(nil), context.Canceled)
	require.ErrorIs(t, cancelled(wantErr), ErrContainmentIncomplete)

	originalShutdownTimeout := sessionShutdownTimeout
	t.Cleanup(func() { sessionShutdownTimeout = originalShutdownTimeout })
	sessionShutdownTimeout = time.Nanosecond
	waitExited := make(chan struct{}, 2)
	stuck := &edgeNativeProcess{wait: func(ctx context.Context) (NativeResult, error) {
		<-ctx.Done()
		waitExited <- struct{}{}

		return NativeResult{}, ctx.Err()
	}}
	agent := edgeManagedAgent(&edgeHostAuthority{start: func(context.Context, NativeRequest) (NativeProcess, error) {
		return stuck, nil
	}})
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = agent.probeNativeVersion(ctx, "pi", "/agent")
	require.ErrorIs(t, err, ErrContainmentIncomplete)
	<-waitExited
	<-waitExited
}

func TestNativeVersionProbeGivesTerminalRejoinAFreshBoundAndJoinsDrains(t *testing.T) {
	originalShutdownTimeout := sessionShutdownTimeout
	sessionShutdownTimeout = 10 * time.Millisecond
	t.Cleanup(func() { sessionShutdownTimeout = originalShutdownTimeout })

	stdout := newBlockingEdgeReadCloser()
	stderr := newBlockingEdgeReadCloser()
	waitCalls := 0
	process := &edgeNativeProcess{
		stdout: stdout,
		stderr: stderr,
		wait: func(ctx context.Context) (NativeResult, error) {
			waitCalls++
			if waitCalls == 1 {
				<-ctx.Done()

				return NativeResult{}, ctx.Err()
			}

			if err := ctx.Err(); err != nil {
				return NativeResult{}, err
			}
			_ = stdout.Close()
			_ = stderr.Close()

			return NativeResult{Revoked: true}, nil
		},
		revoke: func(ctx context.Context) error {
			<-ctx.Done()

			return ctx.Err()
		},
	}
	agent := edgeManagedAgent(&edgeHostAuthority{start: func(context.Context, NativeRequest) (NativeProcess, error) {
		return process, nil
	}})
	probeCtx, cancelProbe := context.WithCancel(t.Context())
	cancelProbe()

	_, err := agent.probeNativeVersion(probeCtx, "pi", "/agent")
	require.ErrorIs(t, err, context.Canceled)
	require.NotErrorIs(t, err, ErrContainmentIncomplete)
	require.Equal(t, 2, waitCalls)
	<-stdout.exited
	<-stderr.exited
}

func TestTerminalVersionProbeJoinsBufferedOutputBeforeClose(t *testing.T) {
	buffered := []byte("1.2.3")
	var output []byte
	order := make([]string, 0, 2)

	finishVersionProbeOutput(
		true,
		func() {
			order = append(order, "join")
			output = append([]byte(nil), buffered...)
		},
		func() {
			order = append(order, "close")
			buffered = nil
		},
	)

	require.Equal(t, []string{"join", "close"}, order)
	require.Equal(t, "1.2.3", string(output), "closing first would truncate buffered version output")
}
