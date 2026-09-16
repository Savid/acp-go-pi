package piacp

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/savid/acp-go-core/process"
	"github.com/savid/acp-go-pi/internal/pi"
)

func TestStateModelRef(t *testing.T) {
	t.Parallel()

	require.Equal(t, "", stateModelRef(pi.SessionState{}))
	require.Equal(t, "", stateModelRef(pi.SessionState{Model: &pi.Model{Provider: "unknown", ID: "unknown"}}))
	require.Equal(t, "a/b", stateModelRef(pi.SessionState{Model: &pi.Model{Provider: "a", ID: "b"}}))
}

func TestPermissionModeDefault(t *testing.T) {
	t.Parallel()

	s := &session{}
	require.Equal(t, pi.PermissionModeAsk, s.permissionMode())

	s.options.Permission = pi.PermissionModeAllow
	require.Equal(t, pi.PermissionModeAllow, s.permissionMode())
}

func TestLateDialogAfterCancellationIsRefused(t *testing.T) {
	t.Parallel()
	for _, state := range []string{"cancelled", "timed out", "closed", "disconnected"} {
		t.Run(state, func(t *testing.T) {
			t.Parallel()
			s := &session{runtime: &runtime{}, turn: &turn{}}
			switch state {
			case "cancelled":
				s.turn.cancelled = true
			case "timed out":
				s.turn.timedOut = true
			case "closed":
				s.closing = true
			case "disconnected":
				s.runtime = nil
			}
			ctx, cancel := context.WithCancelCause(t.Context())
			defer cancel(nil)
			release := s.registerDialog("late-native-request", cancel)
			require.ErrorIs(t, context.Cause(ctx), errDialogCancelled)
			release()
			s.callbacks.Wait()
		})
	}
}

// The extension abort runs as session-owned work the shutdown ladder joins, so
// it is registered only while the session still routes the generation it
// interrupts. A refused registration adds nothing to the callbacks a closing
// session is already waiting on, and never reaches the runtime.
func TestExtensionAbortRegistrationIsRefused(t *testing.T) {
	t.Parallel()
	for _, state := range []string{"closed", "replaced", "disconnected"} {
		t.Run(state, func(t *testing.T) {
			t.Parallel()
			s := &session{agent: NewAgent(testOptions(t)...)}
			rt := &runtime{}
			s.runtime = rt
			switch state {
			case "closed":
				s.closing = true
			case "replaced":
				s.runtime = &runtime{}
			case "disconnected":
				s.runtime = nil
			}
			// The generation carries no client, so an abort that is registered
			// anyway fails on it.
			s.abortAsync(t.Context(), rt)
			s.callbacks.Wait()
		})
	}
}

// A generation the session no longer routes ends without settling the turn or
// ending the cycle the live generation owns. Its own child is still reaped.
func TestRuntimeEndedLeavesTheLiveGenerationAlone(t *testing.T) {
	t.Parallel()

	s := &session{agent: NewAgent(testOptions(t)...)}
	turnCtx, cancel := context.WithCancel(t.Context())
	defer cancel()

	live := &runtime{}
	running := &turn{rt: live, cancel: cancel, settled: make(chan struct{}), finished: make(chan struct{})}
	s.runtime = live
	s.turn = running
	s.cycle = &cycle{}

	s.runtimeEnded(t.Context(), &runtime{proc: startedProcess(t), done: make(chan struct{})})

	require.NoError(t, turnCtx.Err(), "the replaced generation cancelled a turn it never ran")
	require.Equal(t, turnRunning, running.ended)
	require.Same(t, live, s.runtime)
	require.NotNil(t, s.cycle)
}

// startedProcess is a real short-lived child, for a generation a test hands to
// code that reaps it.
func startedProcess(t *testing.T) *process.Process {
	t.Helper()

	proc, err := process.Start(t.Context(), process.Request{
		Executable: os.Args[0],
		Args:       []string{"--version"},
		Env:        []string{fakePiEnv + "=1"},
	})
	require.NoError(t, err)

	return proc
}
