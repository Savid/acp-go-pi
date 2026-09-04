package piacp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	internalpi "github.com/savid/acp-go-pi/internal/pi"
	"github.com/stretchr/testify/require"
)

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
