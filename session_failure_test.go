package piacp

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
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
	const secret = "stderr-and-transport-secret-sentinel"
	logs := &strings.Builder{}
	previousGrace := processExitClassifyGrace
	processExitClassifyGrace = time.Millisecond
	t.Cleanup(func() { processExitClassifyGrace = previousGrace })

	agent := &Agent{log: slog.New(slog.NewTextHandler(logs, nil))}
	session := &agentSession{agent: agent}
	require.NoError(t, session.nativeTurnFailure(t.Context(), nil))
	requirePiTurnFailure(t, session.nativeTurnFailure(t.Context(), &pi.CommandError{Message: "provider"}), failureCauseProvider)
	requirePiTurnFailure(t, session.nativeTurnFailure(t.Context(), errors.New(secret)), failureCauseTransport)

	process := newStubProcess(true)
	process.waitErr = errors.New("exit 2")
	process.stderr = " " + secret + " "
	session.proc = process
	data := requirePiTurnFailure(t, session.nativeTurnFailure(t.Context(), io.EOF), failureCauseProcessExit)
	require.Equal(t, "pi process exited: exit 2: "+secret, data[jsonFieldMessage])
	require.NotContains(t, logs.String(), secret)

	process = newStubProcess(false)
	session.proc = process
	_, exited := session.processExitCause(t.Context(), "pi process exited")
	require.False(t, exited)

	requirePiTurnFailure(t, providerTurnFailure(&promptTurnState{stopReason: stopReasonError}), failureCauseProvider)
}

// A native session start is off the prompt-turn path, so it carries the closed
// internal-failure token with its documented class and no cause text at all.
// The real native cause reaches the operator's log instead.
func TestNativeStartFailureUsesTheClosedInternalFailureShape(t *testing.T) {
	const secret = "native-start-cause-secret-sentinel"

	logs := &strings.Builder{}
	agent := &Agent{log: slog.New(slog.NewTextHandler(logs, nil))}

	requireInternalFailure(t,
		agent.nativeStartFailure(t.Context(), failureCauseTransport, errors.New("start"), newStubProcess(false)),
		internalClassNativeStart,
	)
	requireInternalFailure(t,
		agent.nativeStartFailure(t.Context(), failureCauseProcessExit, errors.New("probe"), nil),
		internalClassNativeStart,
	)

	process := newStubProcess(true)
	process.waitErr = errors.New("exit 1")
	process.stderr = "connecting mcp server\nfatal: " + secret
	startErr := agent.nativeStartFailure(t.Context(), failureCauseTransport, errors.New("start"), process)
	requireInternalFailure(t, startErr, internalClassNativeStart)

	// The driving error stays joined for adapter-internal callers, and the
	// native cause is in the log rather than on the wire.
	require.ErrorContains(t, startErr, "start")

	encoded, marshalErr := json.Marshal(startErr)
	require.NoError(t, marshalErr)
	require.NotContains(t, string(encoded), secret)
	require.Contains(t, logs.String(), secret)
}

// The client is told the native cause, never the unbounded stderr transcript
// the adapter retains behind it.
func TestNativeCauseIsBounded(t *testing.T) {
	t.Parallel()

	data := requirePiTurnFailure(t, turnFailureError(failureCauseProvider, strings.Repeat("a", nativeCauseMaxBytes+64)), failureCauseProvider)
	require.Len(t, data[jsonFieldMessage], nativeCauseMaxBytes)

	leading := strings.Repeat(" ", nativeCauseMaxBytes) + "secret suffix"
	data = requirePiTurnFailure(t, turnFailureError(failureCauseProvider, leading), failureCauseProvider)
	require.Empty(t, data[jsonFieldMessage])

	broken := strings.Repeat("a", nativeCauseMaxBytes-1) + "\xe2\x82\xacsecret"
	data = requirePiTurnFailure(t, turnFailureError(failureCauseProvider, broken), failureCauseProvider)
	require.Equal(t, strings.Repeat("a", nativeCauseMaxBytes-1), data[jsonFieldMessage])
}

// TestOffPromptInternalErrorVocabulary drives every -32603 this adapter can
// emit away from a prompt turn and pins the closed vocabulary. Each verdict
// carries exactly one vendor-prefixed token in `data.error`, the JSON-RPC
// message stays the protocol constant, `data` is always present, and no
// `data.message`, Go error text, or native text appears anywhere.
func TestOffPromptInternalErrorVocabulary(t *testing.T) {
	t.Parallel()

	const secret = "off-prompt-internal-error-secret-sentinel"

	t.Run("construction verdict", func(t *testing.T) {
		t.Parallel()

		agent := newStubClientAgent(t, newStubPiClient(),
			WithLogger(slog.New(slog.DiscardHandler)),
			WithImageLimits(ImageLimits{MaxOutputBytesPerImage: -1}),
		)

		// The verdict is delivered on initialize and on every
		// session-establishing entry point, because an embedded host can open a
		// session without ever calling initialize.
		_, err := agent.Initialize(t.Context(), acp.InitializeRequest{})
		requireClosedInternalError(t, err, invalidOptionsError)

		_, err = agent.NewSession(t.Context(), NewSessionRequest(t.TempDir()))
		requireClosedInternalError(t, err, invalidOptionsError)
	})

	t.Run("poisoned session", func(t *testing.T) {
		t.Parallel()

		logs := &strings.Builder{}
		agent := NewAgent(WithLogger(slog.New(slog.NewTextHandler(logs, nil))), testContainmentOption())
		connection := newDirectAgentClient()
		agent.setConnection(connection)

		session := &agentSession{agent: agent, id: "poisoned"}
		err := session.poison(t.Context(), poisoned(poisonCauseNativeInvariant, secret))

		data := requireClosedInternalError(t, err, sessionPoisonedError)
		require.Equal(t, poisonCauseNativeInvariant, data[failureFieldCause])
		require.NotContains(t, string(mustJSON(t, err)), secret,
			"the prose reason never reaches the wire")
		require.Contains(t, logs.String(), secret,
			"the prose reason reaches the operator's log instead")
	})

	t.Run("unreplayable store entry", func(t *testing.T) {
		t.Parallel()

		store := NewInMemorySessionStore()
		require.NoError(t, store.Append(t.Context(), SessionKey{SessionID: validSessionUUID},
			[]SessionStoreEntry{json.RawMessage(`{"row":1}`)}))

		agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)), WithSessionStore(store))
		_, _, err := agent.loadCurrentStoreEntries(t.Context(), validSessionUUID)
		requireClosedInternalError(t, err, restoreFailedError)

		// The entry the restore refused is neither deleted nor tombstoned.
		entries, loadErr := store.Load(t.Context(), SessionKey{SessionID: validSessionUUID})
		require.NoError(t, loadErr)
		require.Len(t, entries, 1)
	})

	t.Run("unclassified handler failure", func(t *testing.T) {
		t.Parallel()

		log := slog.New(slog.DiscardHandler)
		requireClosedInternalError(t, requestError(t.Context(), log, errSecret(secret)), internalFailureError)
		require.NotContains(t,
			string(mustJSON(t, requestError(t.Context(), log, errSecret(secret)))), secret)
	})
}
