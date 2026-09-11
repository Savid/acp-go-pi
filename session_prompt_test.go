package piacp

import (
	"context"
	"encoding/base64"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"
)

func TestPromptMappingFlattensContext(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	h.initialize()
	session := h.newSession()

	uri := "file:///tmp/notes.md"
	request := PromptRequest(session.SessionId,
		acp.TextBlock("ECHO hello"),
		acp.ContentBlock{ResourceLink: &acp.ContentBlockResourceLink{Uri: uri, Name: "notes"}},
		acp.ContentBlock{Resource: &acp.ContentBlockResource{Resource: acp.EmbeddedResourceResource{
			TextResourceContents: &acp.TextResourceContents{Uri: uri, Text: "a < b"},
		}}},
	)

	_, err := h.conn.Prompt(h.ctx(), request)
	require.NoError(t, err)

	text := agentText(h.rec.snapshot())
	require.Contains(t, text, "ECHO hello\n"+uri+"\n"+uri+"\n\n<context ref=\""+uri+"\">\na &lt; b\n</context>")
}

func TestMapPromptSkipsUserOnlyText(t *testing.T) {
	t.Parallel()

	s := &session{agent: NewAgent(testOptions(t)...)}

	mapped, err := s.mapPrompt(context.Background(), []acp.ContentBlock{
		acp.TextBlock("kept"),
		{Text: &acp.ContentBlockText{Text: "hidden", Annotations: &acp.Annotations{Audience: []acp.Role{acp.RoleUser}}}},
	})
	require.NoError(t, err)
	require.Equal(t, "kept", mapped.message)

	_, err = s.mapPrompt(context.Background(), []acp.ContentBlock{
		{Text: &acp.ContentBlockText{Text: "hidden", Annotations: &acp.Annotations{Audience: []acp.Role{acp.RoleUser}}}},
	})
	require.Equal(t, "prompt", requestErrorData(t, err)["field"])
}

func TestPromptImagesForwardAndGate(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	h.initialize()
	session := h.newSession()

	_, err := h.conn.Prompt(h.ctx(), PromptRequest(session.SessionId, acp.TextBlock("ECHO with image"), acp.ImageBlock(tinyPNG, "image/png")))
	require.NoError(t, err)
	require.Contains(t, agentText(h.rec.snapshot()), "images:1")

	_, err = h.conn.Prompt(h.ctx(), PromptRequest(session.SessionId, acp.ImageBlock("not base64!", "image/png")))
	data := requestErrorData(t, err)
	require.Equal(t, "invalid_base64", data["error"])
	require.Equal(t, "prompt.image", data["field"])

	blob := "application/pdf"
	_, err = h.conn.Prompt(h.ctx(), PromptRequest(session.SessionId, acp.ContentBlock{Resource: &acp.ContentBlockResource{Resource: acp.EmbeddedResourceResource{
		BlobResourceContents: &acp.BlobResourceContents{Uri: "file:///x.pdf", Blob: base64.StdEncoding.EncodeToString([]byte("%PDF")), MimeType: &blob},
	}}}))
	data = requestErrorData(t, err)
	require.Equal(t, "unsupported", data["error"])
	require.Equal(t, "prompt.resource", data["field"])
}

func TestPromptImageRefusedForTextOnlyModel(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	h.initialize()
	session := h.newSession(WithSessionPiOptions(NewPiOptions(WithPiModel("fake/text-only"))))

	_, err := h.conn.Prompt(h.ctx(), PromptRequest(session.SessionId, acp.TextBlock("look"), acp.ImageBlock(tinyPNG, "image/png")))
	data := requestErrorData(t, err)
	require.Equal(t, "unsupported_by_model", data["error"])
	require.Equal(t, "prompt.image", data["field"])
	require.EqualValues(t, 0, data["index"])
}

func TestPromptRejectedByNative(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	h.initialize()
	session := h.newSession()

	_, err := h.prompt(session.SessionId, "REJECT", nil)
	require.Equal(t, -32603, requestErrorCode(t, err))

	data := requestErrorData(t, err)
	require.Equal(t, "pi_turn_failed", data["error"])
	require.Equal(t, "provider", data["cause"])
	require.Equal(t, "no provider key", data["message"])

	resp, err := h.prompt(session.SessionId, "HELLO", nil)
	require.NoError(t, err)
	require.Equal(t, acp.StopReasonEndTurn, resp.StopReason)
}

func TestPromptProviderError(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	h.initialize()
	session := h.newSession()

	_, err := h.prompt(session.SessionId, "ERROR", nil)
	data := requestErrorData(t, err)
	require.Equal(t, "pi_turn_failed", data["error"])
	require.Equal(t, "provider", data["cause"])
	require.Equal(t, "boom", data["message"])
}

func TestPromptCancelReturnsCancelled(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	h.initialize(withLifecycle())
	session := h.newSession()

	type result struct {
		resp acp.PromptResponse
		err  error
	}

	done := make(chan result, 1)

	go func() {
		resp, err := h.prompt(session.SessionId, "SLOW", promptMeta(1))
		done <- result{resp, err}
	}()

	h.rec.waitFor(t, func(updates []acp.SessionNotification) bool { return len(lifecycleEvents(updates)) >= 3 })
	require.NoError(t, h.conn.Cancel(h.ctx(), CancelRequest(session.SessionId)))
	require.NoError(t, h.conn.Cancel(h.ctx(), CancelRequest(session.SessionId)))

	got := <-done
	require.NoError(t, got.err)
	require.Equal(t, acp.StopReasonCancelled, got.resp.StopReason)

	resp, err := h.prompt(session.SessionId, "HELLO", promptMeta(2))
	require.NoError(t, err)
	require.Equal(t, acp.StopReasonEndTurn, resp.StopReason)
}

func TestPromptCancelledBeforeDispatch(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	h.initialize()
	session := h.newSession()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	resp, err := h.conn.Prompt(ctx, TextPromptRequest(session.SessionId, "HELLO"))
	if err == nil {
		require.Equal(t, acp.StopReasonCancelled, resp.StopReason)
	} else {
		require.Equal(t, -32800, requestErrorCode(t, err))
	}
}

func TestPromptRequestCancellationCancelsTurn(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	h.initialize(withLifecycle())
	session := h.newSession()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)

	go func() {
		request := TextPromptRequest(session.SessionId, "SLOW")
		request.Meta = promptMeta(1)
		_, err := h.conn.Prompt(ctx, request)
		done <- err
	}()

	h.rec.waitFor(t, func(updates []acp.SessionNotification) bool { return len(lifecycleEvents(updates)) >= 3 })
	cancel()

	err := <-done
	require.Error(t, err)

	h.rec.waitFor(t, func(updates []acp.SessionNotification) bool {
		types := eventTypes(lifecycleEvents(updates))

		return types[len(types)-1] == "state_update:idle"
	})

	resp, err := h.prompt(session.SessionId, "HELLO", promptMeta(2))
	require.NoError(t, err)
	require.Equal(t, acp.StopReasonEndTurn, resp.StopReason)
}

func TestPromptTurnTimeout(t *testing.T) {
	t.Parallel()

	h := newHarness(t, WithTurnTimeout(300*time.Millisecond))
	h.initialize()
	session := h.newSession()

	_, err := h.prompt(session.SessionId, "SLOW", nil)
	data := requestErrorData(t, err)
	require.Equal(t, "pi_turn_failed", data["error"])
	require.Equal(t, "timeout", data["cause"])
}

func TestPromptWrapperExtensionFailure(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	h.initialize()
	session := h.newSession()

	_, err := h.prompt(session.SessionId, "EXTERR", nil)
	data := requestErrorData(t, err)
	require.Equal(t, "extension", data["cause"])
	require.Equal(t, extensionFailureMessage, data["message"])
}

func TestPromptIdentityDriftPoisons(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	h.initialize()
	session := h.newSession()

	_, err := h.prompt(session.SessionId, "BADSESSION", nil)
	require.NoError(t, err)

	_, err = h.prompt(session.SessionId, "HELLO", nil)
	data := requestErrorData(t, err)
	require.Equal(t, "pi_session_poisoned", data["error"])
	require.Equal(t, "native_session_identity_drift", data["cause"])

	h.rec.waitFor(t, func(updates []acp.SessionNotification) bool {
		for _, update := range updates {
			if commands := update.Update.AvailableCommandsUpdate; commands != nil && len(commands.AvailableCommands) == 0 {
				return true
			}
		}

		return false
	})

	_, err = h.conn.CloseSession(h.ctx(), acp.CloseSessionRequest{SessionId: session.SessionId})
	require.NoError(t, err)
}

func TestBoundNativeCause(t *testing.T) {
	t.Parallel()

	long := make([]byte, nativeCauseMaxBytes+10)
	for index := range long {
		long[index] = 'a'
	}

	require.Len(t, boundNativeCause(string(long)), nativeCauseMaxBytes)
	require.Equal(t, "x", boundNativeCause("  x\xff "))
}

func TestJudgeCycle(t *testing.T) {
	t.Parallel()

	require.Equal(t, "cancelled", judgeCycle(&cycle{}, true).stopReason)
	require.Equal(t, "max_tokens", judgeCycle(&cycle{state: cycleState{stopReason: stopReasonLength}}, false).stopReason)
	require.Equal(t, "cancelled", judgeCycle(&cycle{state: cycleState{stopReason: stopReasonAborted}}, false).stopReason)
	require.Error(t, judgeCycle(&cycle{state: cycleState{stopReason: stopReasonError}}, false).failure)
	require.Equal(t, "end_turn", judgeCycle(&cycle{state: cycleState{stopReason: stopReasonStop}}, false).stopReason)
}
