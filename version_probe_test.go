package piacp

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

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
