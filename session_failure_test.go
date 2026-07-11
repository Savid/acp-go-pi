package piacp

import (
	"errors"
	"io"
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

	session := &agentSession{}
	require.NoError(t, session.nativeTurnFailure(nil))
	requirePiTurnFailure(t, session.nativeTurnFailure(&pi.CommandError{Message: "provider"}), failureCauseProvider)
	requirePiTurnFailure(t, session.nativeTurnFailure(io.EOF), failureCauseTransport)

	process := newStubProcess(true)
	process.waitErr = errors.New("exit 2")
	process.stderr = " stderr "
	session.proc = process
	data := requirePiTurnFailure(t, session.nativeTurnFailure(io.EOF), failureCauseProcessExit)
	require.Contains(t, data[jsonFieldMessage], "stderr")

	process = newStubProcess(false)
	session.proc = process
	_, exited := session.processExitMessage()
	require.False(t, exited)

	requirePiTurnFailure(t, providerTurnFailure(&promptTurnState{stopReason: stopReasonError}), failureCauseProvider)
	spawn := spawnFailureError(errors.New("start"), nil)
	require.Error(t, spawn)
	process = newStubProcess(true)
	process.waitErr = errors.New("exit")
	process.stderr = "tail"
	spawn = spawnFailureError(errors.New("start"), process)
	require.Contains(t, spawn.Error(), "Internal error")
}
