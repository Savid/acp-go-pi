package piacp

import (
	"encoding/json"
	"errors"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"

	"github.com/savid/acp-go-pi/internal/pi"
)

func TestPromptContentMapping(t *testing.T) {
	mime := "image/png"
	textResource := acp.EmbeddedResourceResource{TextResourceContents: &acp.TextResourceContents{
		Uri: "file:///a<&\"'", Text: "body <&>",
	}}
	imageResource := acp.EmbeddedResourceResource{BlobResourceContents: &acp.BlobResourceContents{
		Uri: "file:///image", Blob: "aW1hZ2U=", MimeType: &mime,
	}}
	userAnnotations := &acp.Annotations{Audience: []acp.Role{acp.RoleUser}}
	ignored := acp.TextBlock("ignored")
	ignored.Text.Annotations = userAnnotations

	mapped, err := promptToPi([]acp.ContentBlock{
		acp.TextBlock("hello"), ignored,
		acp.ImageBlock("aW1hZ2U=", "image/png"),
		acp.ResourceLinkBlock("link", " https://example.test "),
		acp.ResourceBlock(textResource),
		acp.ResourceBlock(imageResource),
	})
	require.NoError(t, err)
	require.Contains(t, mapped.Message, "hello")
	require.NotContains(t, mapped.Message, "ignored")
	require.Contains(t, mapped.Message, "https://example.test")
	require.Contains(t, mapped.Message, "&lt;")
	require.Len(t, mapped.Images, 2)

	invalid := [][]acp.ContentBlock{
		nil,
		{{}},
		{acp.TextBlock("  ")},
		{acp.AudioBlock("data", "audio/wav")},
		{acp.ImageBlock("", "image/png")},
		{acp.ResourceBlock(acp.EmbeddedResourceResource{})},
		{acp.ResourceBlock(acp.EmbeddedResourceResource{BlobResourceContents: &acp.BlobResourceContents{Uri: "x", Blob: "data"}})},
	}
	for _, blocks := range invalid {
		_, invalidErr := promptToPi(blocks)
		requireInvalidParams(t, invalidErr)
	}

	text, contextText, image, err := resourceToPi(textResource)
	require.NoError(t, err)
	require.NotEmpty(t, text)
	require.NotEmpty(t, contextText)
	require.Nil(t, image)
	require.Equal(t, "&amp;&lt;&gt;&quot;&apos;", xmlEscape("&<>\"'"))
}

func TestAcpStopReasonMapping(t *testing.T) {
	session := &agentSession{agent: &Agent{log: slog.New(slog.DiscardHandler)}}
	for native, want := range map[string]acp.StopReason{
		stopReasonStop:      acp.StopReasonEndTurn,
		stopReasonLength:    acp.StopReasonMaxTokens,
		stopReasonMaxTokens: acp.StopReasonMaxTokens,
		stopReasonAborted:   acp.StopReasonCancelled,
		"unknown":           acp.StopReasonEndTurn,
	} {
		require.Equal(t, want, acpStopReason(session, &promptTurnState{stopReason: native}))
	}
}

func TestPromptSecondPoisonCheck(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	client := newStubPiClient()
	session := &agentSession{agent: agent, id: "id", client: client, proc: newStubProcess(false)}
	lateCtx := &poisonOnDoneContext{session: session}

	_, err := session.Prompt(lateCtx, TextPromptRequest("id", "test-turn", "hi"))
	require.Error(t, err)
}

func TestPromptClientPromptFailure(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	client := newStubPiClient()
	client.promptErr = errors.New("prompt")
	session := &agentSession{agent: agent, id: "id", client: client, proc: newStubProcess(false)}

	_, err := session.Prompt(t.Context(), TextPromptRequest("id", "test-turn", "hi"))
	requirePiTurnFailure(t, err, failureCauseTransport)
}

func TestPromptHandleTurnEventEmitFailure(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	connection := newDirectAgentClient()
	connection.updateErr = errors.New("emit")
	agent.setConnection(connection)
	client := newStubPiClient()
	session := &agentSession{agent: agent, id: "id", client: client, proc: newStubProcess(false)}
	session.startPump(client)
	t.Cleanup(session.stopPump)

	go func() {
		deadline := time.Now().Add(2 * time.Second)
		for session.activeTurnSink() == nil {
			if time.Now().After(deadline) {
				return
			}

			time.Sleep(time.Millisecond)
		}

		select {
		case client.events <- pi.ToolExecutionStartEvent{ToolCallID: "call", ToolName: "bash"}:
		case <-time.After(2 * time.Second):
		}
	}()

	_, err := session.Prompt(t.Context(), TextPromptRequest("id", "test-turn", "hi"))
	require.Error(t, err)
}

func TestMessageEndIdentityEmitFailure(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	connection := newDirectAgentClient()
	connection.updateErr = errors.New("emit identity")
	agent.setConnection(connection)
	session := &agentSession{agent: agent, id: "id"}

	_, err := session.handleTurnEvent(t.Context(), pi.MessageEndEvent{Message: pi.AgentMessage{
		Role: messageRoleAssistant, ACPMessageID: "018f47ad-839d-7f70-b7f7-c01d6d97b675",
	}}, &promptTurnState{})
	require.ErrorContains(t, err, "emit identity")
}

func TestFinishTurnCommitMirrorFailure(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)), WithTurnTimeout(time.Second))
	connection := newDirectAgentClient()
	agent.setConnection(connection)
	client := newStubPiClient()
	client.stats = pi.SessionStats{SessionID: "id"}
	session := &agentSession{agent: agent, id: "id", client: client, proc: newStubProcess(false), sessionFilePath: t.TempDir()}

	var timedOut atomic.Bool
	_, err := session.finishTurn(t.Context(), t.Context(), TextPromptRequest("id", "test-turn", "title"), &promptTurnState{}, &timedOut)
	require.Error(t, err)
}

func TestTurnEventAndUsageBranches(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	connection := newDirectAgentClient()
	agent.setConnection(connection)
	client := newStubPiClient()
	session := &agentSession{agent: agent, id: "id", client: client, proc: newStubProcess(false), contextWindowSize: 123}
	state := &promptTurnState{}

	settled, err := session.handleTurnEvent(t.Context(), pi.AgentSettledEvent{}, state)
	require.NoError(t, err)
	require.True(t, settled)
	_, err = session.handleTurnEvent(t.Context(), pi.MessageStartEvent{Message: pi.AgentMessage{Role: messageRoleAssistant, Model: "m", Provider: "p"}}, state)
	require.NoError(t, err)
	require.Equal(t, "m", state.model)

	for _, delta := range []pi.AssistantMessageEvent{
		{Type: assistantEventTextDelta, Delta: "text"},
		{Type: assistantEventTextDelta},
		{Type: assistantEventThinkingDelta, Delta: "thought"},
		{Type: assistantEventThinkingDelta},
		{Type: "unknown", Delta: "ignored"},
	} {
		_, err = session.handleTurnEvent(t.Context(), pi.MessageUpdateEvent{AssistantMessageEvent: delta}, state)
		require.NoError(t, err)
	}

	usage := &pi.Usage{Input: 1, Output: 2, CacheRead: 3, CacheWrite: 4, Cost: &pi.UsageCost{Total: 0.5}}
	_, err = session.handleTurnEvent(t.Context(), pi.MessageEndEvent{Message: pi.AgentMessage{
		Role: messageRoleAssistant, Model: "model", Provider: "provider", StopReason: stopReasonStop,
		ErrorMessage: "error", Usage: usage, ACPMessageID: "018f47ad-839d-7f70-b7f7-c01d6d97b675",
	}}, state)
	require.NoError(t, err)
	require.Equal(t, 10, state.usage.TotalTokens)
	require.Equal(t, "018f47ad-839d-7f70-b7f7-c01d6d97b675", state.nativeMessageID)
	require.Equal(t, "018f47ad-839d-7f70-b7f7-c01d6d97b675",
		anyMap(t, connection.notifications[len(connection.notifications)-1].Meta[piMetaKey])[jsonFieldMessageID])
	mergeTurnUsage(state.usage, nil)
	mergeTurnUsage(state.usage, &pi.Usage{Input: 2})
	require.Equal(t, 12, state.usage.TotalTokens)

	_, err = session.handleTurnEvent(t.Context(), pi.ToolExecutionStartEvent{ToolCallID: "call", ToolName: "bash", Args: json.RawMessage(`{"x":true}`)}, state)
	require.NoError(t, err)
	_, err = session.handleTurnEvent(t.Context(), pi.ToolExecutionUpdateEvent{ToolCallID: "call"}, state)
	require.NoError(t, err)
	result := &pi.ToolResult{Content: []pi.ContentBlock{{Type: contentBlockTypeText, Text: "output"}}}
	_, err = session.handleTurnEvent(t.Context(), pi.ToolExecutionUpdateEvent{ToolCallID: "call", PartialResult: result}, state)
	require.NoError(t, err)
	_, err = session.handleTurnEvent(t.Context(), pi.ToolExecutionEndEvent{ToolCallID: "call", IsError: true, Result: result}, state)
	require.NoError(t, err)
	_, err = session.handleTurnEvent(t.Context(), pi.ToolExecutionEndEvent{ToolCallID: "call"}, state)
	require.NoError(t, err)
	_, err = session.handleTurnEvent(t.Context(), pi.AgentStartEvent{}, state)
	require.NoError(t, err)

	require.EqualValues(t, 123, session.currentModelContextWindow())
	client.statsErr = errors.New("stats")
	require.Nil(t, session.settledSessionStats(t.Context()))
	client.statsErr = nil
	tokens := int64(7)
	client.stats = pi.SessionStats{ContextUsage: &pi.ContextUsage{Tokens: &tokens, ContextWindow: 456}}
	require.NotNil(t, session.settledSessionStats(t.Context()))
	session.emitTurnUsageUpdate(t.Context(), &promptTurnState{}, nil)
	session.emitTurnUsageUpdate(t.Context(), state, &client.stats)
	require.NotEmpty(t, connection.updates)
}

func TestTurnTerminationBranches(t *testing.T) {
	previousGrace := processExitClassifyGrace
	processExitClassifyGrace = time.Millisecond
	t.Cleanup(func() { processExitClassifyGrace = previousGrace })

	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)), WithTurnTimeout(time.Second))
	client := newStubPiClient()
	session := &agentSession{agent: agent, client: client, proc: newStubProcess(false)}
	var timedOut atomic.Bool
	messageID := "message"

	response, err := session.transportEndedTurn(&messageID, &timedOut)
	requirePiTurnFailure(t, err, failureCauseTransport)
	require.Empty(t, response.StopReason)
	client.err = errors.New("native stream")
	_, err = session.transportEndedTurn(&messageID, &timedOut)
	requirePiTurnFailure(t, err, failureCauseTransport)

	timedOut.Store(true)
	_, err = session.transportEndedTurn(&messageID, &timedOut)
	requirePiTurnFailure(t, err, failureCauseTimeout)
	_, err = session.contextEndedTurn(&messageID, &timedOut)
	requirePiTurnFailure(t, err, failureCauseTimeout)
	timedOut.Store(false)
	response, err = session.contextEndedTurn(&messageID, &timedOut)
	require.NoError(t, err)
	require.Equal(t, acp.StopReasonCancelled, response.StopReason)

	session.turnCancelled = true
	response, err = session.transportEndedTurn(&messageID, &timedOut)
	require.NoError(t, err)
	require.Equal(t, acp.StopReasonCancelled, response.StopReason)
	response, err = session.contextEndedTurn(&messageID, &timedOut)
	require.NoError(t, err)
	require.Equal(t, acp.StopReasonCancelled, response.StopReason)

	session.turnCancelled = false
	session.proc = newStubProcess(true)
	_, err = session.transportEndedTurn(&messageID, &timedOut)
	requirePiTurnFailure(t, err, failureCauseProcessExit)

	original := errors.New("emit")
	require.ErrorIs(t, session.abortAfterEmitError(t.Context(), original), original)
	session.client = nil
	require.ErrorIs(t, session.abortAfterEmitError(t.Context(), original), original)
}

func TestPromptAndFinishTurnErrorBranches(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)), WithTurnTimeout(time.Second))
	connection := newDirectAgentClient()
	agent.setConnection(connection)
	client := newStubPiClient()
	client.stats = pi.SessionStats{SessionID: "id"}
	session := &agentSession{agent: agent, id: "id", client: client, proc: newStubProcess(false)}

	session.poisonCause = "poisoned"
	_, err := session.Prompt(t.Context(), TextPromptRequest("id", "test-turn", "hello"))
	require.Error(t, err)
	session.poisonCause = ""
	release, err := session.acquireTurn(t.Context())
	require.NoError(t, err)
	_, err = session.Prompt(t.Context(), TextPromptRequest("id", "test-turn", "hello"))
	requireInvalidRequest(t, err)
	release()
	_, err = session.Prompt(t.Context(), PromptRequest("id", "test-turn"))
	requireInvalidParams(t, err)
	session.proc = nil
	_, err = session.Prompt(t.Context(), TextPromptRequest("id", "test-turn", "hello"))
	requirePiTurnFailure(t, err, failureCauseTransport)

	finish := func(state *promptTurnState, timedOut bool) (acp.PromptResponse, error) {
		var timeout atomic.Bool
		timeout.Store(timedOut)

		return session.finishTurn(t.Context(), t.Context(), TextPromptRequest("id", "test-turn", "title"), state, &timeout)
	}
	session.proc = newStubProcess(false)
	client.stats = pi.SessionStats{SessionID: "other"}
	_, err = finish(&promptTurnState{}, false)
	require.Error(t, err)
	session.poisonCause = ""
	client.stats = pi.SessionStats{SessionID: "id"}
	connection.updateErr = errors.New("update")
	_, err = finish(&promptTurnState{}, false)
	require.Error(t, err)
	connection.updateErr = nil
	session.turnCancelled = true
	response, err := finish(&promptTurnState{}, false)
	require.NoError(t, err)
	require.Equal(t, acp.StopReasonCancelled, response.StopReason)
	session.turnCancelled = false
	_, err = finish(&promptTurnState{}, true)
	requirePiTurnFailure(t, err, failureCauseTimeout)
	_, err = finish(&promptTurnState{stopReason: stopReasonError, errorMessage: "provider"}, false)
	requirePiTurnFailure(t, err, failureCauseProvider)
	response, err = finish(&promptTurnState{
		stopReason: stopReasonStop, nativeMessageID: "018f47ad-839d-7f70-b7f7-c01d6d97b675",
	}, false)
	require.NoError(t, err)
	require.Equal(t, acp.StopReasonEndTurn, response.StopReason)
	require.Equal(t, "018f47ad-839d-7f70-b7f7-c01d6d97b675",
		anyMap(t, response.Meta[piMetaKey])[jsonFieldMessageID])
}
