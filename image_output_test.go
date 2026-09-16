package piacp

import (
	"testing"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"

	"github.com/savid/acp-go-core/image"
	"github.com/savid/acp-go-pi/internal/pi"
)

func TestToolImageOutput(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	h.initialize()
	session := h.newSession(WithSessionPiOptions(NewPiOptions(WithPiPermission(pi.PermissionModeAllow))))

	_, err := h.prompt(session.SessionId, "TOOLIMAGE", nil)
	require.NoError(t, err)

	tools := toolUpdates(h.rec.snapshot())
	last := tools[len(tools)-1].Update.ToolCallUpdate
	require.Equal(t, acp.ToolCallStatusCompleted, *last.Status)
	require.Len(t, last.Content, 1)
	require.NotNil(t, last.Content[0].Content.Content.Image)
	require.Equal(t, "image/png", last.Content[0].Content.Content.Image.MimeType)
	require.Equal(t, tinyPNG, last.Content[0].Content.Content.Image.Data)
}

func TestToolImageTooLargeReportsGuidance(t *testing.T) {
	t.Parallel()

	h := newHarness(t, WithImageLimits(ImageLimits{MaxOutputBytesPerImage: 10, MaxInputBytesPerImage: 1 << 20, MaxInputBytesPerPrompt: 1 << 20, MaxOutputBytesPerToolCall: 1 << 20}))
	h.initialize()
	session := h.newSession(WithSessionPiOptions(NewPiOptions(WithPiPermission(pi.PermissionModeAllow))))

	resp, err := h.prompt(session.SessionId, "TOOLIMAGE", nil)
	require.NoError(t, err)
	require.Equal(t, acp.StopReasonEndTurn, resp.StopReason)

	tools := toolUpdates(h.rec.snapshot())
	last := tools[len(tools)-1].Update.ToolCallUpdate
	require.Equal(t, acp.ToolCallStatusFailed, *last.Status)
	require.Equal(t, image.GuidanceTooLarge, last.Content[0].Content.Content.Text.Text)
}

func TestAssistantImageOutput(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	h.initialize()
	session := h.newSession()

	_, err := h.prompt(session.SessionId, "IMAGE", nil)
	require.NoError(t, err)

	images := 0

	for _, update := range h.rec.snapshot() {
		if chunk := update.Update.AgentMessageChunk; chunk != nil && chunk.Content.Image != nil {
			images++

			require.Equal(t, "image/png", chunk.Content.Image.MimeType)
			require.NotNil(t, chunk.MessageId)
		}
	}

	require.Equal(t, 1, images)
	require.Equal(t, "here", agentText(h.rec.snapshot()))
}

func TestAssistantImageRefusedInPlace(t *testing.T) {
	t.Parallel()

	h := newHarness(t, WithImageLimits(ImageLimits{MaxOutputBytesPerImage: 10}))
	h.initialize()
	session := h.newSession()

	_, err := h.prompt(session.SessionId, "IMAGE", nil)
	require.NoError(t, err)
	require.Equal(t, "here"+image.GuidanceTooLarge, agentText(h.rec.snapshot()))
}

func TestMapToolContentMergesAndBounds(t *testing.T) {
	t.Parallel()

	limits := image.Limits{MaxOutputBytesPerImage: 1 << 20, MaxOutputBytesPerToolCall: 1 << 20}

	first, failure := mapToolContent(nil, []pi.ContentBlock{{Type: contentBlockTypeText, Text: "a"}, {Type: contentBlockTypeText, Text: ""}}, limits)
	require.Nil(t, failure)
	require.Len(t, first, 1)

	second, failure := mapToolContent(first, []pi.ContentBlock{{Type: contentBlockTypeText, Text: "ab"}}, limits)
	require.Nil(t, failure)
	require.Len(t, second, 1)
	require.Equal(t, "ab", second[0].content.Content.Content.Text.Text)

	withImage, failure := mapToolContent(second, []pi.ContentBlock{{Type: contentBlockTypeImage, Data: tinyPNG}}, limits)
	require.Nil(t, failure)
	require.Len(t, withImage, 1)

	retained, failure := mapToolContent(withImage, []pi.ContentBlock{{Type: contentBlockTypeText, Text: "final"}}, limits)
	require.Nil(t, failure)
	require.Len(t, retained, 2)
	require.Len(t, toolContent(retained), 2)

	small := image.Limits{MaxOutputBytesPerImage: 1 << 20, MaxOutputBytesPerToolCall: 1}
	_, failure = mapToolContent(nil, []pi.ContentBlock{{Type: contentBlockTypeImage, Data: tinyPNG}}, small)
	require.NotNil(t, failure)
	require.Equal(t, image.ReasonTooLarge, failure.Reason)
}
