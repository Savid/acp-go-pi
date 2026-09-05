package piacp

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/savid/acp-go-pi/internal/pi"
)

func TestReplayMappingsAndStoreMetadata(t *testing.T) {
	png := fixtureBase64(t, "valid.png")
	rows := []SessionStoreEntry{
		json.RawMessage(`bad`),
		json.RawMessage(`{"type":"session","cwd":` + testCwdJSON + `}`),
		json.RawMessage(`{"type":"session_info","name":" Named "}`),
		messageRow(t, pi.AgentMessage{Role: messageRoleUser, Content: json.RawMessage(`[{"type":"text","text":"hello"},{"type":"image","data":"` + png + `","mimeType":"image/png"}]`)}),
		messageRow(t, pi.AgentMessage{Role: messageRoleAssistant, ACPMessageID: "018f47ad-839d-7f70-b7f7-c01d6d97b675", Content: json.RawMessage(`[{"type":"thinking","thinking":"thought"},{"type":"text","text":"answer"},{"type":"toolCall","id":"call","name":"bash","arguments":{"command":"true"}}]`)}),
		messageRow(t, pi.AgentMessage{Role: messageRoleToolResult, ToolCallID: "call", IsError: true, Content: json.RawMessage(`[{"type":"text","text":"failed"},{"type":"image","data":"` + png + `","mimeType":"image/png"}]`)}),
	}
	updates, err := replayUpdates(rows, defaultImageLimits())
	require.NoError(t, err)
	require.Len(t, updates, 6)
	require.Equal(t, "018f47ad-839d-7f70-b7f7-c01d6d97b675", *updates[2].AgentThoughtChunk.MessageId)
	require.Equal(t, "018f47ad-839d-7f70-b7f7-c01d6d97b675", *updates[3].AgentMessageChunk.MessageId)
	require.Equal(t, "Named", storeSessionTitle("fallback", rows))
	require.Equal(t, testCwd, storeSessionCwd(rows))
	require.True(t, storeSessionHasContent(rows))
	require.Equal(t, "018f47ad-839d-7f70-b7f7-c01d6d97b675", terminalAssistantMessageID(rows))

	require.Equal(t, "hello", storeSessionTitle("fallback", []SessionStoreEntry{rows[0], rows[1], rows[3]}))
	require.Equal(t, "fallback", storeSessionTitle("fallback", nil))
	require.Empty(t, storeSessionCwd(nil))
	require.False(t, storeSessionHasContent(nil))

	for _, message := range []pi.AgentMessage{
		{Role: "unknown", Content: json.RawMessage(`[]`)},
		{Role: messageRoleToolResult, Content: json.RawMessage(`[]`)},
		{Role: messageRoleUser, Content: json.RawMessage(`bad`)},
	} {
		mapped, failure := messageReplayUpdates(message, defaultImageLimits())
		require.Nil(t, failure)
		require.Nil(t, mapped)
	}

	require.Empty(t, terminalAssistantMessageID(nil))
	require.Empty(t, terminalAssistantMessageID([]SessionStoreEntry{
		json.RawMessage(`bad`), json.RawMessage(`{"type":"session"}`),
	}))

	content, failure := replayToolContent([]pi.ContentBlock{{Type: contentBlockTypeText}, {Type: "other"}}, defaultImageLimits())
	require.Nil(t, failure)
	require.Empty(t, content)
	_, ok := decodeStoreRow(json.RawMessage(`bad`))
	require.False(t, ok)
}

func TestReplayInvalidMetadataRows(t *testing.T) {
	rows := []SessionStoreEntry{
		json.RawMessage(`{"type":"message","message":"bad"}`),
		messageRow(t, pi.AgentMessage{Role: messageRoleAssistant, Content: json.RawMessage(`[]`)}),
		json.RawMessage(`{"type":"message","message":{"role":"user","content":{}}}`),
	}
	updates, err := replayUpdates(rows, defaultImageLimits())
	require.NoError(t, err)
	require.Empty(t, updates)
	require.Equal(t, "fallback", storeSessionTitle("fallback", rows))

	agent := NewAgent()
	connection := newDirectAgentClient()
	agent.setConnection(connection)
	require.NoError(t, (&agentSession{agent: agent, id: "session"}).replayStoredSession(t.Context(), rows))
	require.Empty(t, connection.notifications)
	require.Empty(t, terminalAssistantMessageID(rows))
	require.Empty(t, terminalAssistantMessageID([]SessionStoreEntry{
		messageRow(t, pi.AgentMessage{Role: messageRoleAssistant, Content: json.RawMessage(`[]`)}),
		messageRow(t, pi.AgentMessage{
			Role: messageRoleAssistant, ACPMessageID: "older", Content: json.RawMessage(`[]`),
		}),
		messageRow(t, pi.AgentMessage{Role: messageRoleAssistant, Content: json.RawMessage(`[]`)}),
	}))
}

func TestReplayStoredSessionPublishesDurableNativeIdentity(t *testing.T) {
	agent := NewAgent()
	connection := newDirectAgentClient()
	agent.setConnection(connection)
	session := &agentSession{agent: agent, id: "session"}

	textID := "018f47ad-839d-7f70-b7f7-c01d6d97b675"
	emptyID := "018f47ad-839d-7f70-b7f7-c01d6d97b676"
	rows := []SessionStoreEntry{
		messageRow(t, pi.AgentMessage{
			Role: messageRoleAssistant, ACPMessageID: textID,
			Content: json.RawMessage(`[{"type":"text","text":"answer"}]`),
		}),
		messageRow(t, pi.AgentMessage{
			Role: messageRoleAssistant, ACPMessageID: emptyID, Content: json.RawMessage(`[]`),
		}),
	}

	require.NoError(t, session.replayStoredSession(t.Context(), rows))
	require.Len(t, connection.notifications, 2)
	require.Equal(t, textID,
		anyMap(t, connection.notifications[0].Meta[piMetaKey])[jsonFieldMessageID])
	require.Equal(t, textID, *connection.notifications[0].Update.AgentMessageChunk.MessageId)
	require.Equal(t, emptyID,
		anyMap(t, connection.notifications[1].Meta[piMetaKey])[jsonFieldMessageID])
	require.NotNil(t, connection.notifications[1].Update.SessionInfoUpdate)
}

func TestStoreSessionTitleRoleFallbacks(t *testing.T) {
	rows := []SessionStoreEntry{
		json.RawMessage(`{"type":"message","message":{"role":"assistant","content":[]}}`),
		json.RawMessage(`{"type":"message","message":{"role":"user","content":{}}}`),
		json.RawMessage(`{"type":"message","message":{"role":"tool","content":[]}}`),
	}
	require.Equal(t, "fallback", storeSessionTitle("fallback", rows))
}

func TestReplayAssistantAndToolImages(t *testing.T) {
	png := fixtureBase64(t, "valid.png")
	gif := fixtureBase64(t, "valid.gif")
	messageID := "018f47ad-839d-7f70-b7f7-c01d6d97b675"

	agent := NewAgent()
	connection := newDirectAgentClient()
	agent.setConnection(connection)
	session := &agentSession{agent: agent, id: "session"}

	rows := []SessionStoreEntry{
		messageRow(t, pi.AgentMessage{
			Role: messageRoleAssistant, ACPMessageID: messageID,
			Content: json.RawMessage(`[{"type":"text","text":"look"},{"type":"image","data":"` + png + `","mimeType":"image/png"}]`),
		}),
		messageRow(t, pi.AgentMessage{
			Role: messageRoleToolResult, ToolCallID: "call",
			Content: json.RawMessage(`[{"type":"image","data":"` + png + `","mimeType":"image/png"},{"type":"image","data":"` + gif + `","mimeType":"image/gif"}]`),
		}),
	}
	require.NoError(t, session.replayStoredSession(t.Context(), rows))
	require.Len(t, connection.updates, 3)
	require.Equal(t, png, connection.updates[1].AgentMessageChunk.Content.Image.Data)
	require.Equal(t, messageID, *connection.updates[1].AgentMessageChunk.MessageId)
	toolUpdate := connection.updates[2].ToolCallUpdate
	require.Len(t, toolUpdate.Content, 2, "a stored multi-image tool result replays as one complete array")
	require.Equal(t, png, toolUpdate.Content[0].Content.Content.Image.Data)
	require.Equal(t, gif, toolUpdate.Content[1].Content.Content.Image.Data)
}

func TestReplayImageFailuresFailTheLoad(t *testing.T) {
	png := fixtureBase64(t, "valid.png")
	pngSize := int64(len(fixtureBytes(t, "valid.png")))

	tests := []struct {
		name   string
		row    SessionStoreEntry
		limits ImageLimits
		reason string
	}{
		{
			name: "swept assistant artifact",
			row: messageRow(t, pi.AgentMessage{Role: messageRoleAssistant, ACPMessageID: "id",
				Content: json.RawMessage(`[{"type":"image","mimeType":"image/png"}]`)}),
			limits: defaultImageLimits(),
			reason: imageReasonStorageFailed,
		},
		{
			name: "swept tool artifact",
			row: messageRow(t, pi.AgentMessage{Role: messageRoleToolResult, ToolCallID: "call",
				Content: json.RawMessage(`[{"type":"image","data":"","mimeType":"image/png"}]`)}),
			limits: defaultImageLimits(),
			reason: imageReasonStorageFailed,
		},
		{
			name: "corrupt stored artifact",
			row: messageRow(t, pi.AgentMessage{Role: messageRoleAssistant, ACPMessageID: "id",
				Content: json.RawMessage(`[{"type":"image","data":"bm90IGEgcmFzdGVy","mimeType":"image/png"}]`)}),
			limits: defaultImageLimits(),
			reason: imageReasonStorageFailed,
		},
		{
			name: "since-lowered per-image limit",
			row: messageRow(t, pi.AgentMessage{Role: messageRoleAssistant, ACPMessageID: "id",
				Content: json.RawMessage(`[{"type":"image","data":"` + png + `","mimeType":"image/png"}]`)}),
			limits: ImageLimits{MaxOutputBytesPerImage: pngSize - 1, MaxOutputBytesPerToolCall: defaultImageLimitBytes},
			reason: imageErrorTooLarge,
		},
		{
			name: "since-lowered per-tool-call limit",
			row: messageRow(t, pi.AgentMessage{Role: messageRoleToolResult, ToolCallID: "call",
				Content: json.RawMessage(`[{"type":"image","data":"` + png + `","mimeType":"image/png"},{"type":"image","data":"` + png + `","mimeType":"image/png"}]`)}),
			limits: ImageLimits{MaxOutputBytesPerImage: defaultImageLimitBytes, MaxOutputBytesPerToolCall: 2*pngSize - 1},
			reason: imageErrorTooLarge,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			agent := NewAgent(WithImageLimits(test.limits))
			connection := newDirectAgentClient()
			agent.setConnection(connection)
			session := &agentSession{agent: agent, id: "session"}

			// Replay is a restore: the stored artifact this adapter can no
			// longer reproduce fails the load with the closed restore token,
			// not with a turn failure.
			err := session.replayStoredSession(t.Context(), []SessionStoreEntry{test.row})
			requireRestoreFailure(t, err)

			updates, err := replayUpdates([]SessionStoreEntry{test.row}, test.limits)
			require.Error(t, err)
			require.Nil(t, updates)
		})
	}
}

func TestLiveAndReplayImageParity(t *testing.T) {
	png := fixtureBase64(t, "valid.png")
	gif := fixtureBase64(t, "valid.gif")
	messageID := "018f47ad-839d-7f70-b7f7-c01d6d97b675"

	liveAgent := NewAgent()
	liveConnection := newDirectAgentClient()
	liveAgent.setConnection(liveConnection)
	liveSession := &agentSession{agent: liveAgent, id: "session"}
	state := &promptTurnState{}

	_, err := liveSession.handleTurnEvent(t.Context(), pi.ToolExecutionStartEvent{ToolCallID: "call", ToolName: "generate"}, state)
	require.NoError(t, err)
	_, err = liveSession.handleTurnEvent(t.Context(), pi.ToolExecutionUpdateEvent{ToolCallID: "call", PartialResult: &pi.ToolResult{
		Content: []pi.ContentBlock{{Type: contentBlockTypeImage, Data: png, MimeType: "image/png"}},
	}}, state)
	require.NoError(t, err)
	_, err = liveSession.handleTurnEvent(t.Context(), pi.ToolExecutionEndEvent{ToolCallID: "call", Result: &pi.ToolResult{
		Content: []pi.ContentBlock{
			{Type: contentBlockTypeImage, Data: png, MimeType: "image/png"},
			{Type: contentBlockTypeImage, Data: gif, MimeType: "image/gif"},
		},
	}}, state)
	require.NoError(t, err)
	_, err = liveSession.handleTurnEvent(t.Context(), pi.MessageEndEvent{Message: pi.AgentMessage{
		Role: messageRoleAssistant, ACPMessageID: messageID,
		Content: json.RawMessage(`[{"type":"image","data":"` + png + `","mimeType":"image/png"}]`),
	}}, state)
	require.NoError(t, err)

	liveTerminal := liveConnection.updates[len(liveConnection.updates)-3].ToolCallUpdate
	require.NotNil(t, liveTerminal)
	liveAgentImage := liveConnection.updates[len(liveConnection.updates)-2].AgentMessageChunk
	require.NotNil(t, liveAgentImage)

	replayAgent := NewAgent()
	replayConnection := newDirectAgentClient()
	replayAgent.setConnection(replayConnection)
	replaySession := &agentSession{agent: replayAgent, id: "session"}

	require.NoError(t, replaySession.replayStoredSession(t.Context(), []SessionStoreEntry{
		messageRow(t, pi.AgentMessage{
			Role: messageRoleToolResult, ToolCallID: "call",
			Content: json.RawMessage(`[{"type":"image","data":"` + png + `","mimeType":"image/png"},{"type":"image","data":"` + gif + `","mimeType":"image/gif"}]`),
		}),
		messageRow(t, pi.AgentMessage{
			Role: messageRoleAssistant, ACPMessageID: messageID,
			Content: json.RawMessage(`[{"type":"image","data":"` + png + `","mimeType":"image/png"}]`),
		}),
	}))

	replayTerminal := replayConnection.updates[0].ToolCallUpdate
	require.Equal(t, liveTerminal.ToolCallId, replayTerminal.ToolCallId)
	require.Len(t, replayTerminal.Content, len(liveTerminal.Content))

	for index := range liveTerminal.Content {
		liveImage := liveTerminal.Content[index].Content.Content.Image
		replayImage := replayTerminal.Content[index].Content.Content.Image
		require.Equal(t, liveImage.Data, replayImage.Data)
		require.Equal(t, liveImage.MimeType, replayImage.MimeType)
	}

	replayAgentImage := replayConnection.updates[1].AgentMessageChunk
	require.Equal(t, liveAgentImage.Content.Image.Data, replayAgentImage.Content.Image.Data)
	require.Equal(t, liveAgentImage.Content.Image.MimeType, replayAgentImage.Content.Image.MimeType)
	require.Equal(t, *liveAgentImage.MessageId, *replayAgentImage.MessageId)
}
