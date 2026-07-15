package piacp

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/savid/acp-go-pi/internal/pi"
)

func TestReplayMappingsAndStoreMetadata(t *testing.T) {
	rows := []SessionStoreEntry{
		json.RawMessage(`bad`),
		json.RawMessage(`{"type":"session","cwd":"/cwd"}`),
		json.RawMessage(`{"type":"session_info","name":" Named "}`),
		messageRow(t, pi.AgentMessage{Role: messageRoleUser, Content: json.RawMessage(`[{"type":"text","text":"hello"},{"type":"image","data":"aW1n","mimeType":"image/png"}]`)}),
		messageRow(t, pi.AgentMessage{Role: messageRoleAssistant, ACPMessageID: "018f47ad-839d-7f70-b7f7-c01d6d97b675", Content: json.RawMessage(`[{"type":"thinking","thinking":"thought"},{"type":"text","text":"answer"},{"type":"toolCall","id":"call","name":"bash","arguments":{"command":"true"}}]`)}),
		messageRow(t, pi.AgentMessage{Role: messageRoleToolResult, ToolCallID: "call", IsError: true, Content: json.RawMessage(`[{"type":"text","text":"failed"},{"type":"image","data":"aW1n","mimeType":"image/png"}]`)}),
	}
	updates := replayUpdates(rows)
	require.Len(t, updates, 6)
	require.Equal(t, "018f47ad-839d-7f70-b7f7-c01d6d97b675", *updates[2].AgentThoughtChunk.MessageId)
	require.Equal(t, "018f47ad-839d-7f70-b7f7-c01d6d97b675", *updates[3].AgentMessageChunk.MessageId)
	require.Equal(t, "Named", storeSessionTitle("fallback", rows))
	require.Equal(t, "/cwd", storeSessionCwd(rows))
	require.True(t, storeSessionHasContent(rows))

	require.Equal(t, "hello", storeSessionTitle("fallback", []SessionStoreEntry{rows[0], rows[1], rows[3]}))
	require.Equal(t, "fallback", storeSessionTitle("fallback", nil))
	require.Empty(t, storeSessionCwd(nil))
	require.False(t, storeSessionHasContent(nil))
	require.Nil(t, messageReplayUpdates(pi.AgentMessage{Role: "unknown", Content: json.RawMessage(`[]`)}))
	require.Nil(t, messageReplayUpdates(pi.AgentMessage{Role: messageRoleToolResult, Content: json.RawMessage(`[]`)}))
	require.Nil(t, messageReplayUpdates(pi.AgentMessage{Role: messageRoleUser, Content: json.RawMessage(`bad`)}))

	content := toolCallContent([]pi.ContentBlock{{Type: contentBlockTypeText}, {Type: contentBlockTypeImage}, {Type: "other"}})
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
	require.Empty(t, replayUpdates(rows))
	require.Equal(t, "fallback", storeSessionTitle("fallback", rows))

	agent := NewAgent()
	connection := newDirectAgentClient()
	agent.setConnection(connection)
	require.NoError(t, (&agentSession{agent: agent, id: "session"}).replayStoredSession(t.Context(), rows))
	require.Empty(t, connection.notifications)
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
