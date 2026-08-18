package piacp

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"

	"github.com/savid/acp-go-pi/internal/pi"
)

func TestNativeMessageNotificationMetaPreservesTurnRoute(t *testing.T) {
	ctx := withTurnRoute(context.Background(), "turn-1")
	meta := nativeMessageNotificationMeta(ctx, "018f47ad-839d-7f70-b7f7-c01d6d97b675")

	require.Equal(t, "turn-1", anyMap(t, meta[routeMetaKey])[routeFieldTurn])
	require.Equal(t, "018f47ad-839d-7f70-b7f7-c01d6d97b675",
		anyMap(t, meta[piMetaKey])[jsonFieldMessageID])
	require.Nil(t, nativeMessageResponseMeta(""))
	require.NoError(t, (&agentSession{}).emitNativeMessageIdentity(t.Context(), ""))
}

func TestAvailableCommandsMapping(t *testing.T) {
	commands := availableCommandsFromNative([]pi.SlashCommand{
		{Name: "good", Description: "ok"},
		{Name: ""}, {Name: "bad/name"}, {Name: "bad name"}, {Name: "bad\x00"}, {Name: "bad\u200e"},
		{Name: string([]byte{0xff})},
	})
	require.Len(t, commands, 1)
	require.True(t, availableCommandsEqual(commands, cloneAvailableCommands(commands)))
	require.False(t, availableCommandsEqual(commands, nil))
	other := cloneAvailableCommands(commands)
	other[0].Description = "different"
	require.False(t, availableCommandsEqual(commands, other))
	require.Nil(t, cloneAvailableCommands(nil))

	unstructured := acp.UnstructuredCommandInput{Hint: "hint"}
	input := acp.AvailableCommandInput{Unstructured: &unstructured}
	withInput := []acp.AvailableCommand{{Name: "input", Input: &input}}
	clone := cloneAvailableCommands(withInput)
	clone[0].Input.Unstructured.Hint = "changed"
	require.Equal(t, "hint", withInput[0].Input.Unstructured.Hint)
}

func TestLiveSessionTitleNormalization(t *testing.T) {
	require.Equal(t, "short title", normalizeLiveSessionTitle("  short   title  "))
	require.Equal(t, "", normalizeLiveSessionTitle(" \n\t "))
	long := strings.Repeat("x", 300)
	require.LessOrEqual(t, len(normalizeLiveSessionTitle(long)), liveSessionTitleMaxRunes)
	prompt := []acp.ContentBlock{acp.TextBlock(" first "), acp.TextBlock("second")}
	require.Equal(t, "first", liveSessionTitleFromPrompt(prompt))
	require.Empty(t, liveSessionTitleFromPrompt(nil))
}

func TestRawEventSixCases(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	client := newDirectAgentClient()
	agent.setConnection(client)
	first := &agentSession{agent: agent, id: "first", rawMessages: rawMessageConfig{All: true}}
	second := &agentSession{agent: agent, id: "second", rawMessages: rawMessageConfig{All: true}}

	first.emitRawPiEvent(t.Context(), []byte(`{"type":"one"}`))
	first.emitRawPiEvent(t.Context(), []byte(`{"type":"two"}`))
	second.emitRawPiEvent(t.Context(), []byte(`{"type":"one"}`))
	require.Len(t, client.notified, 3)
	require.EqualValues(t, 1, client.notified[0][rawEventFieldSequence])
	require.EqualValues(t, 2, client.notified[1][rawEventFieldSequence])
	require.EqualValues(t, 1, client.notified[2][rawEventFieldSequence])

	first.emitRawPiEvent(t.Context(), []byte(`{"value":"`+strings.Repeat("x", rawEventMaxBytes)+`"}`))
	require.Equal(t, rawEventReasonOversize, anyMap(t, client.notified[3][rawEventFieldEvent])[rawEventFieldReason])
	first.emitRawPiEvent(t.Context(), []byte(`not-json`))
	require.Equal(t, rawEventReasonUnserializable, anyMap(t, client.notified[4][rawEventFieldEvent])[rawEventFieldReason])

	client.notifyErr = errors.New("emit")
	first.emitRawPiEvent(t.Context(), []byte(`{"type":"still-success"}`))
	first.emitRawPiEvent(
		withTurnRoute(t.Context(), strings.Repeat("n", rawEventMaxBytes)),
		[]byte(`{"type":"unbounded-internal-route"}`),
	)
	disabled := &agentSession{agent: agent, id: "disabled"}
	disabled.emitRawPiEvent(t.Context(), []byte(`{"type":"off"}`))
	first.emitRawPiEvent(t.Context(), nil)
	require.Len(t, client.notified, 6)

	agent.conn = nil
	first.emitRawPiEvent(t.Context(), []byte(`{"type":"no-client"}`))
	agent.conn = client
	agent.closed = true
	first.emitRawPiEvent(t.Context(), []byte(`{"type":"closed"}`))
}

func TestRawEventSequenceCommitsOnlyAfterSuccessfulDelivery(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	client := newDirectAgentClient()
	agent.setConnection(client)
	session := &agentSession{agent: agent, id: "session", rawMessages: rawMessageConfig{All: true}}

	session.emitRawPiEvent(t.Context(), nil)
	require.Empty(t, client.notified)
	require.Zero(t, session.rawEventSequence)

	session.emitRawPiEvent(
		withTurnRoute(t.Context(), strings.Repeat("n", rawEventMaxBytes)),
		[]byte(`{"type":"internal-cap-failure"}`),
	)
	require.Empty(t, client.notified)
	require.Zero(t, session.rawEventSequence)

	client.notifyErr = errors.New("delivery failed")
	session.emitRawPiEvent(t.Context(), []byte(`{"type":"failed"}`))
	require.Len(t, client.notified, 1)
	require.EqualValues(t, 1, client.notified[0][rawEventFieldSequence])
	require.Zero(t, session.rawEventSequence)

	client.notifyErr = nil
	session.emitRawPiEvent(t.Context(), []byte(`{"type":"recovered"}`))
	session.emitRawPiEvent(t.Context(), []byte(`{"type":"next"}`))
	require.Len(t, client.notified, 3)
	require.EqualValues(t, 1, client.notified[1][rawEventFieldSequence])
	require.EqualValues(t, 2, client.notified[2][rawEventFieldSequence])
	require.EqualValues(t, 2, session.rawEventSequence)
}

func TestSessionUpdateEmissionAndPoisoning(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	client := newDirectAgentClient()
	agent.setConnection(client)
	session := &agentSession{
		agent:             agent,
		id:                "id",
		availableCommands: []pi.SlashCommand{{Name: "one", Description: "first"}},
	}
	require.NoError(t, session.emitUpdates(t.Context(), nil))
	require.NoError(t, session.emitOptionalUpdates(t.Context(), nil))
	require.NoError(t, session.emitAvailableCommandsUpdate(t.Context(), false))
	require.Len(t, client.updates, 1)
	require.NoError(t, session.emitAvailableCommandsUpdate(t.Context(), false))
	require.Len(t, client.updates, 1)
	require.NoError(t, session.emitAvailableCommandsUpdate(t.Context(), true))
	require.Len(t, client.updates, 2)

	session.availableCommands = nil
	require.NoError(t, session.emitAvailableCommandsUpdate(t.Context(), false))
	require.Len(t, client.updates, 3)
	require.NoError(t, session.emitClearAvailableCommandsUpdate(t.Context()))
	require.Len(t, client.updates, 3)
	session.advertisedCommands = []acp.AvailableCommand{{Name: "one"}}
	require.NoError(t, session.emitClearAvailableCommandsUpdate(t.Context()))
	require.Len(t, client.updates, 4)
	require.Len(t, emptyAvailableCommandsUpdate(), 1)

	client.updateErr = errors.New("update")
	require.Error(t, session.emitUpdates(t.Context(), []acp.SessionUpdate{{}}))
	require.Error(t, session.emitOptionalUpdates(t.Context(), []acp.SessionUpdate{{}}))
	client.updateErr = nil
	agent.conn = nil
	require.ErrorIs(t, session.emitUpdates(t.Context(), []acp.SessionUpdate{{}}), errACPConnectionNotAttached)
	require.NoError(t, session.emitOptionalUpdates(t.Context(), []acp.SessionUpdate{{}}))
	agent.conn = client
	agent.closed = true
	require.ErrorIs(t, session.emitUpdates(t.Context(), []acp.SessionUpdate{{}}), errAgentClosed)
	require.NoError(t, session.emitOptionalUpdates(t.Context(), []acp.SessionUpdate{{}}))
	agent.closed = false

	cancelled := false
	session.cancel = func() { cancelled = true }
	session.advertisedCommands = []acp.AvailableCommand{{Name: "one"}}
	require.Error(t, session.poison(t.Context(), "broken"))
	require.True(t, cancelled)
	require.Error(t, session.poisonedError())
	require.Error(t, session.poison(t.Context(), "other"))
	require.Error(t, poisonedSessionError("broken"))
	nilAgent := &agentSession{}
	require.Error(t, nilAgent.poison(t.Context(), "broken"))
}

func TestClearCommandsAndPoisonEmitFailures(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	client := newDirectAgentClient()
	agent.setConnection(client)
	session := &agentSession{agent: agent, id: "id", advertisedCommands: []acp.AvailableCommand{{Name: "one"}}}
	client.updateErr = errors.New("clear")
	require.Error(t, session.emitClearAvailableCommandsUpdate(t.Context()))
	require.Error(t, session.poison(t.Context(), "broken"))
	require.Equal(t, "text", liveSessionTitleFromPrompt([]acp.ContentBlock{{}, acp.TextBlock("text")}))
}

// TestAvailableCommandsUpdateSilentBeforeAnyCatalog pins that without a force
// the adapter says nothing before pi has advertised a first catalog: silence
// there is not an answer a host may read as an empty catalog.
func TestAvailableCommandsUpdateSilentBeforeAnyCatalog(t *testing.T) {
	connection := newDirectAgentClient()
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	agent.setConnection(connection)
	session := &agentSession{agent: agent, id: "id"}

	require.NoError(t, session.emitAvailableCommandsUpdate(t.Context(), false))
	require.Empty(t, connection.notifications)
}
