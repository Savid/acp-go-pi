package piacp

import (
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

	_, err := session.Prompt(lateCtx, TextPromptRequest("id", "hi"))
	require.Error(t, err)
}

func TestPromptClientPromptFailure(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	client := newStubPiClient()
	client.promptErr = errors.New("prompt")
	session := &agentSession{agent: agent, id: "id", client: client, proc: newStubProcess(false)}

	_, err := session.Prompt(t.Context(), TextPromptRequest("id", "hi"))
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

	_, err := session.Prompt(t.Context(), TextPromptRequest("id", "hi"))
	require.Error(t, err)
}

func TestFinishTurnCommitMirrorFailure(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)), WithTurnTimeout(time.Second))
	connection := newDirectAgentClient()
	agent.setConnection(connection)
	client := newStubPiClient()
	client.stats = pi.SessionStats{SessionID: "id"}
	session := &agentSession{agent: agent, id: "id", client: client, proc: newStubProcess(false), sessionFilePath: t.TempDir()}

	var timedOut atomic.Bool
	_, err := session.finishTurn(t.Context(), t.Context(), TextPromptRequest("id", "title"), &promptTurnState{}, &timedOut)
	require.Error(t, err)
}
