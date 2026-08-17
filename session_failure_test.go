package piacp

import (
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/savid/acp-go-pi/internal/pi"
)

func TestProviderTurnFailureAndEmptyCloneError(t *testing.T) {
	err := providerTurnFailure(&promptTurnState{stopReason: stopReasonError, errorMessage: "message"})
	requirePiTurnFailure(t, err, failureCauseProvider)
	require.NoError(t, providerTurnFailure(&promptTurnState{}))
	require.False(t, emptyCloneError(errors.New("other")))
	require.True(t, emptyCloneError(&pi.CommandError{Message: "Entry abcd not found"}))
}

func TestNativeFailureClassification(t *testing.T) {
	previousGrace := processExitClassifyGrace
	processExitClassifyGrace = time.Millisecond
	t.Cleanup(func() { processExitClassifyGrace = previousGrace })

	agent := &Agent{log: slog.New(slog.DiscardHandler)}
	session := &agentSession{agent: agent}
	require.NoError(t, session.nativeTurnFailure(t.Context(), nil))
	requirePiTurnFailure(t, session.nativeTurnFailure(t.Context(), &pi.CommandError{Message: "provider"}), failureCauseProvider)
	requirePiTurnFailure(t, session.nativeTurnFailure(t.Context(), io.EOF), failureCauseTransport)

	process := newStubProcess(true)
	process.waitErr = errors.New("exit 2")
	process.stderr = " stderr "
	session.proc = process
	data := requirePiTurnFailure(t, session.nativeTurnFailure(t.Context(), io.EOF), failureCauseProcessExit)
	require.Equal(t, "pi process exited: exit 2: stderr", data[jsonFieldMessage])

	process = newStubProcess(false)
	session.proc = process
	_, exited := session.processExitCause(t.Context(), "pi process exited")
	require.False(t, exited)

	requirePiTurnFailure(t, providerTurnFailure(&promptTurnState{stopReason: stopReasonError}), failureCauseProvider)
}

// A native session start fails with the same uniform shape a turn does: there
// is no separate pi_session_start_failed surface.
func TestNativeStartFailureUsesTheUniformTurnFailureShape(t *testing.T) {
	agent := &Agent{log: slog.New(slog.DiscardHandler)}

	live := requirePiTurnFailure(t, agent.nativeStartFailure(t.Context(), failureCauseTransport, errors.New("start"), newStubProcess(false)), failureCauseTransport)
	require.Equal(t, "start", live[jsonFieldMessage])

	require.Equal(t,
		failureCauseProcessExit,
		requirePiTurnFailure(t, agent.nativeStartFailure(t.Context(), failureCauseProcessExit, errors.New("probe"), nil), failureCauseProcessExit)[failureFieldCause],
	)

	process := newStubProcess(true)
	process.waitErr = errors.New("exit 1")
	process.stderr = "connecting mcp server\nfatal: mcp server refused"
	dead := requirePiTurnFailure(t, agent.nativeStartFailure(t.Context(), failureCauseTransport, errors.New("start"), process), failureCauseProcessExit)
	require.Equal(t, "pi exited during session start: exit 1: fatal: mcp server refused", dead[jsonFieldMessage])
}

// The client is told the native cause, never the unbounded stderr transcript
// the adapter retains behind it.
func TestNativeCauseIsBounded(t *testing.T) {
	t.Parallel()

	data := requirePiTurnFailure(t, turnFailureError(failureCauseProvider, strings.Repeat("a", nativeCauseMaxBytes+64)), failureCauseProvider)
	require.Len(t, data[jsonFieldMessage], nativeCauseMaxBytes)
}
