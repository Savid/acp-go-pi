package piacp

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/savid/acp-go-core/image"
	"github.com/savid/acp-go-pi/internal/pi"
)

func TestReplayUpdates(t *testing.T) {
	t.Parallel()

	limits := image.DefaultLimits()

	user := pi.AgentMessage{Role: messageRoleUser, Content: json.RawMessage(`[{"type":"text","text":"hi"},{"type":"image","data":"` + tinyPNG + `","mimeType":"image/png"}]`)}
	updates, failure := replayUpdates(user, limits)
	require.Nil(t, failure)
	require.Len(t, updates, 2)
	require.NotNil(t, updates[1].UserMessageChunk.Content.Image)

	assistant := pi.AgentMessage{Role: messageRoleAssistant, ACPMessageID: "m1", Content: json.RawMessage(`[{"type":"thinking","thinking":"t"},{"type":"text","text":"a"},{"type":"toolCall","id":"c1","name":"bash","arguments":{"command":"ls"}},{"type":"image","data":"` + tinyPNG + `","mimeType":"image/png"}]`)}
	updates, failure = replayUpdates(assistant, limits)
	require.Nil(t, failure)
	require.Len(t, updates, 4)
	require.Equal(t, "m1", *updates[1].AgentMessageChunk.MessageId)
	require.NotNil(t, updates[2].ToolCall)
	require.NotNil(t, updates[3].AgentMessageChunk.Content.Image)

	broken := pi.AgentMessage{Role: messageRoleAssistant, Content: json.RawMessage(`[{"type":"image","data":"!!"}]`)}
	_, failure = replayUpdates(broken, limits)
	require.NotNil(t, failure)

	result := pi.AgentMessage{Role: messageRoleToolResult, ToolCallID: "c1", IsError: true, Content: json.RawMessage(`[{"type":"text","text":"no"}]`)}
	updates, failure = replayUpdates(result, limits)
	require.Nil(t, failure)
	require.Len(t, updates, 1)
	require.Len(t, updates[0].ToolCallUpdate.Content, 1)

	updates, failure = replayUpdates(pi.AgentMessage{Role: messageRoleToolResult}, limits)
	require.Nil(t, failure)
	require.Empty(t, updates)

	updates, failure = replayUpdates(pi.AgentMessage{Role: "custom"}, limits)
	require.Nil(t, failure)
	require.Empty(t, updates)

	updates, failure = replayUpdates(pi.AgentMessage{Role: messageRoleUser, Content: json.RawMessage(`{`)}, limits)
	require.Nil(t, failure)
	require.Empty(t, updates)
}
