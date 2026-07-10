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
		messageRow(t, pi.AgentMessage{Role: messageRoleAssistant, Content: json.RawMessage(`[{"type":"thinking","thinking":"thought"},{"type":"text","text":"answer"},{"type":"toolCall","id":"call","name":"bash","arguments":{"command":"true"}}]`)}),
		messageRow(t, pi.AgentMessage{Role: messageRoleToolResult, ToolCallID: "call", IsError: true, Content: json.RawMessage(`[{"type":"text","text":"failed"},{"type":"image","data":"aW1n","mimeType":"image/png"}]`)}),
	}
	updates := replayUpdates(rows)
	require.Len(t, updates, 6)
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
