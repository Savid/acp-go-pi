package piacp

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"

	acpcore "github.com/savid/acp-go-core"
	"github.com/savid/acp-go-core/image"
	"github.com/savid/acp-go-core/wire"
)

func TestPromptMappingFlattensContext(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	h.initialize()
	session := h.newSession()

	uri := "file:///tmp/notes.md"
	request := wire.PromptRequest(session.SessionId,
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

	_, err := h.conn.Prompt(h.ctx(), wire.PromptRequest(session.SessionId, acp.TextBlock("ECHO with image"), acp.ImageBlock(tinyPNG, "image/png")))
	require.NoError(t, err)
	require.Contains(t, agentText(h.rec.snapshot()), "images:1")

	_, err = h.conn.Prompt(h.ctx(), wire.PromptRequest(session.SessionId, acp.ImageBlock("not base64!", "image/png")))
	data := requestErrorData(t, err)
	require.Equal(t, "invalid_base64", data["error"])
	require.Equal(t, "prompt.image", data["field"])

	blob := "application/pdf"
	_, err = h.conn.Prompt(h.ctx(), wire.PromptRequest(session.SessionId, acp.ContentBlock{Resource: &acp.ContentBlockResource{Resource: acp.EmbeddedResourceResource{
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

	_, err := h.conn.Prompt(h.ctx(), wire.PromptRequest(session.SessionId, acp.TextBlock("look"), acp.ImageBlock(tinyPNG, "image/png")))
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
	require.NoError(t, h.conn.Cancel(h.ctx(), wire.CancelRequest(session.SessionId)))
	require.NoError(t, h.conn.Cancel(h.ctx(), wire.CancelRequest(session.SessionId)))

	got := <-done
	require.NoError(t, got.err)
	require.Equal(t, acp.StopReasonCancelled, got.resp.StopReason)

	events := lifecycleEvents(h.rec.snapshot())
	require.Equal(t, "cancelled", events[len(events)-1]["outcome"])

	resp, err := h.prompt(session.SessionId, "HELLO", promptMeta(2))
	require.NoError(t, err)
	require.Equal(t, acp.StopReasonEndTurn, resp.StopReason)
}

// A turn is the session's before the prompt has anything to dispatch, so a
// session/cancel that lands while pi is still being relaunched ends it there:
// the prompt answers cancelled, pi never receives the turn, and the lifecycle
// stream carries nothing for it.
func TestPromptCancelledWhileRelaunching(t *testing.T) {
	t.Parallel()

	held := filepath.Join(t.TempDir(), "relaunch-held")
	store := &mirrorFaultStore{SessionStore: acpcore.NewInMemorySessionStore()}
	h := newHarness(t, WithSessionStore(store),
		WithEnv(map[string]string{fakePiEnv: "1", fakePiEnvResumeHold: held}))
	h.initialize(withLifecycle())
	session := h.newSession()

	// A failed mirror commit ends the generation, so the next prompt relaunches
	// pi, and a relaunched fake announces itself and then answers nothing.
	store.fail.Store(true)

	_, err := h.prompt(session.SessionId, "HELLO", promptMeta(1))
	require.Equal(t, "pi_turn_failed", requestErrorData(t, err)["error"])
	store.fail.Store(false)

	type result struct {
		resp acp.PromptResponse
		err  error
	}

	done := make(chan result, 1)

	go func() {
		resp, promptErr := h.prompt(session.SessionId, "HELLO", promptMeta(2))
		done <- result{resp, promptErr}
	}()

	require.Eventually(t, func() bool {
		_, statErr := os.Stat(held)

		return statErr == nil
	}, testTimeout, time.Millisecond)

	require.NoError(t, h.conn.Cancel(h.ctx(), wire.CancelRequest(session.SessionId)))

	got := <-done
	require.NoError(t, got.err)
	require.Equal(t, acp.StopReasonCancelled, got.resp.StopReason)

	require.Equal(t, []string{"lifecycle_snapshot", "prompt_accepted", "state_update:running"},
		eventTypes(lifecycleEvents(h.rec.snapshot())),
		"a prompt pi never received opens no incarnation and publishes no acceptance")
}

// A turn belongs to its session, not to the request that started it: the
// pinned SDK cancels a session's previous prompt context before dispatching
// the next one, so a peer prompt this session refuses must leave the live turn
// running.
func TestRefusedPeerPromptLeavesTurnRunning(t *testing.T) {
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
		resp, err := h.prompt(session.SessionId, "WAIT", promptMeta(1))
		done <- result{resp, err}
	}()

	h.rec.waitFor(t, func(updates []acp.SessionNotification) bool { return len(lifecycleEvents(updates)) >= 3 })

	_, err := h.prompt(session.SessionId, "HELLO", promptMeta(2))
	require.Equal(t, "backpressure", requestErrorData(t, err)["error"])
	require.Equal(t, limitSessionPrompt, requestErrorData(t, err)["limit"])

	got := <-done
	require.NoError(t, got.err)
	require.Equal(t, acp.StopReasonEndTurn, got.resp.StopReason)
}

// The turn deadline bounds the whole prompt, including a relaunched pi that
// never answers. It fails with cause "timeout", never as a cancel.
func TestPromptTimesOutWhileRelaunching(t *testing.T) {
	t.Parallel()

	held := filepath.Join(t.TempDir(), "relaunch-held")
	store := &mirrorFaultStore{SessionStore: acpcore.NewInMemorySessionStore()}
	h := newHarness(t, WithTurnTimeout(300*time.Millisecond), WithSessionStore(store),
		WithEnv(map[string]string{fakePiEnv: "1", fakePiEnvResumeHold: held}))
	h.initialize()
	session := h.newSession()

	store.fail.Store(true)

	_, err := h.prompt(session.SessionId, "HELLO", nil)
	require.Equal(t, "pi_turn_failed", requestErrorData(t, err)["error"])
	store.fail.Store(false)

	_, err = h.prompt(session.SessionId, "HELLO", nil)
	data := requestErrorData(t, err)
	require.Equal(t, "pi_turn_failed", data["error"])
	require.Equal(t, "timeout", data["cause"])
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

func TestJudgeCycle(t *testing.T) {
	t.Parallel()

	s := &session{}

	require.Equal(t, "cancelled", s.judgeCycle(&cycle{}, true).stopReason)
	require.Equal(t, "max_tokens", s.judgeCycle(&cycle{state: cycleState{stopReason: stopReasonLength}}, false).stopReason)
	require.Equal(t, "cancelled", s.judgeCycle(&cycle{state: cycleState{stopReason: stopReasonAborted}}, false).stopReason)
	require.Error(t, s.judgeCycle(&cycle{state: cycleState{stopReason: stopReasonError}}, false).failure)
	require.Equal(t, "end_turn", s.judgeCycle(&cycle{state: cycleState{stopReason: "unrecognized"}}, false).stopReason)
}

// The handoff form reaches the same native input as the embedded form over the
// same bytes, the handoff path appears in no native field, and handoff bytes
// spend the prompt aggregate.
func TestPromptHandoffImageInput(t *testing.T) {
	t.Parallel()

	root := t.TempDir()

	raw, err := base64.StdEncoding.DecodeString(tinyPNG)
	require.NoError(t, err)

	path := filepath.Join(root, "input.png")
	require.NoError(t, os.WriteFile(path, raw, 0o600))

	digest := sha256.Sum256(raw)
	uri := "file://" + path

	block := func(envelope map[string]any) acp.ContentBlock {
		image := acp.ImageBlock("", "image/png")
		image.Image.Uri = &uri
		image.Image.Meta = map[string]any{wire.HandoffKey: envelope}

		return image
	}

	envelope := map[string]any{"version": 1, "digest": hex.EncodeToString(digest[:]), "sizeBytes": len(raw)}
	rooted := &session{agent: NewAgent(append(testOptions(t), WithInputHandoffRoot(root))...)}

	embedded, err := rooted.mapPrompt(t.Context(), []acp.ContentBlock{acp.ImageBlock(tinyPNG, "image/png")})
	require.NoError(t, err)

	transported, err := rooted.mapPrompt(t.Context(), []acp.ContentBlock{block(envelope)})
	require.NoError(t, err)
	require.Equal(t, embedded.images, transported.images, "the handoff form reaches the same native input")

	encoded, err := json.Marshal(transported.images)
	require.NoError(t, err)
	require.NotContains(t, string(encoded), root)
	require.NotContains(t, transported.message, root)

	unrooted := &session{agent: NewAgent(testOptions(t)...)}
	_, err = unrooted.mapPrompt(t.Context(), []acp.ContentBlock{block(envelope)})
	require.Equal(t, image.ErrorInvalidHandoff, requestErrorData(t, err)["error"])

	mismatch := map[string]any{"version": 1, "digest": hex.EncodeToString(make([]byte, sha256.Size)), "sizeBytes": len(raw)}
	_, err = rooted.mapPrompt(t.Context(), []acp.ContentBlock{block(mismatch)})
	require.Equal(t, image.ErrorDigestMismatch, requestErrorData(t, err)["error"])

	bounded := &session{agent: NewAgent(append(testOptions(t),
		WithInputHandoffRoot(root), WithImageLimits(ImageLimits{MaxInputBytesPerPrompt: int64(len(raw))}))...)}
	_, err = bounded.mapPrompt(t.Context(), []acp.ContentBlock{block(envelope), block(envelope)})
	require.Equal(t, image.ErrorTooLarge, requestErrorData(t, err)["error"], "handoff bytes spend the prompt aggregate")
}

// A $/cancel_request ends only the addressed handler's context: the turn it
// was driving stays the session's, completes successfully once, and the
// session keeps serving prompts.
func TestCancelRequestSettlesTheOriginalRequestOnce(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	permissionCtx, releasePermission := context.WithCancel(t.Context())
	defer releasePermission()
	entered := make(chan struct{}, 1)
	answer := h.rec.answer
	h.rec.answer = func(request acp.RequestPermissionRequest) acp.RequestPermissionResponse {
		entered <- struct{}{}
		<-permissionCtx.Done()

		return answer(request)
	}
	h.initialize(withLifecycle())
	session := h.newSession()

	request := wire.TextPromptRequest(session.SessionId, "TOOL")
	request.Meta = promptMeta(1)
	failed := make(chan error, 1)

	go func() {
		response, err := h.conn.Prompt(h.ctx(), request)
		if err == nil && response.StopReason != acp.StopReasonEndTurn {
			err = errors.New("request cancellation ended the native turn")
		}
		failed <- err
	}()

	select {
	case <-entered:
	case <-h.ctx().Done():
		t.Fatal("native permission request did not arrive")
	}
	require.NoError(t, h.input.cancelPrompt())

	_, busyErr := h.prompt(session.SessionId, "HELLO", promptMeta(2))
	require.Equal(t, "backpressure", requestErrorData(t, busyErr)["error"])
	releasePermission()
	h.rec.waitFor(t, func(updates []acp.SessionNotification) bool {
		return slices.Contains(eventTypes(lifecycleEvents(updates)), "state_update:idle")
	})

	idles := 0

	for _, update := range h.rec.snapshot() {
		envelope, _ := update.Meta[wire.LifecycleKey].(map[string]any)
		event, _ := envelope["event"].(map[string]any)
		if event["type"] == "state_update" && event["state"] == "idle" {
			idles++
			require.Equal(t, "success", event["outcome"])
			require.Equal(t, string(acp.StopReasonEndTurn), event["stopReason"])
		}
	}

	require.Equal(t, 1, idles, "the turn the cancelled request started settles exactly once")
	require.NoError(t, <-failed)

	resp, err := h.prompt(session.SessionId, "HELLO", promptMeta(3))
	require.NoError(t, err)
	require.Equal(t, acp.StopReasonEndTurn, resp.StopReason)
}

func TestPromptRefusesTextAfterImagesBeforeDispatch(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		tail acp.ContentBlock
	}{
		{"text", acp.TextBlock("after")},
		{"resource link", acp.ContentBlock{ResourceLink: &acp.ContentBlockResourceLink{Uri: "file:///notes.txt", Name: "notes"}}},
		{"text resource", acp.ContentBlock{Resource: &acp.ContentBlockResource{Resource: acp.EmbeddedResourceResource{
			TextResourceContents: &acp.TextResourceContents{Uri: "file:///notes.txt", Text: "after"},
		}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.initialize()
			session := h.newSession()
			h.rec.waitFor(t, func(updates []acp.SessionNotification) bool {
				return slices.ContainsFunc(updates, func(update acp.SessionNotification) bool {
					return update.Update.AvailableCommandsUpdate != nil
				})
			})
			before := h.rec.snapshot()
			_, err := h.conn.Prompt(h.ctx(), wire.PromptRequest(session.SessionId,
				acp.TextBlock("before"), acp.ImageBlock(tinyPNG, "image/png"), tc.tail))
			require.Equal(t, map[string]any{"error": "unsupported", "field": "prompt"}, requestErrorData(t, err))
			require.Equal(t, before, h.rec.snapshot(), "refusal publishes no turn updates")
			_, err = h.conn.Prompt(h.ctx(), wire.PromptRequest(session.SessionId, acp.TextBlock("ECHO after refusal")))
			require.NoError(t, err)
			require.Contains(t, agentText(h.rec.snapshot()[len(before):]), "after refusal")
			require.NotContains(t, agentText(h.rec.snapshot()[len(before):]), "images:")
		})
	}
}

func TestMapPromptAcceptsImageGroup(t *testing.T) {
	t.Parallel()
	s := &session{agent: NewAgent(testOptions(t)...)}
	mime := "image/png"
	blob := acp.ContentBlock{Resource: &acp.ContentBlockResource{Resource: acp.EmbeddedResourceResource{
		BlobResourceContents: &acp.BlobResourceContents{Uri: "file:///provenance.png", MimeType: &mime, Blob: tinyPNG},
	}}}
	for _, prefix := range [][]acp.ContentBlock{nil, {acp.TextBlock("caption")}} {
		blocks := append(slices.Clone(prefix), acp.ImageBlock(tinyPNG, "image/png"), blob,
			acp.ContentBlock{Text: &acp.ContentBlockText{Text: "display only", Annotations: &acp.Annotations{Audience: []acp.Role{acp.RoleUser}}}})
		mapped, err := s.mapPrompt(t.Context(), blocks)
		require.NoError(t, err)
		require.Len(t, mapped.images, 2)
		require.NotContains(t, mapped.message, "provenance")
		require.NotContains(t, mapped.message, "display only")
		if len(prefix) == 0 {
			require.Empty(t, mapped.message)
		} else {
			require.Equal(t, "caption", mapped.message)
		}
	}
}

// Handler cancellation can precede native dispatch; only session cancellation owns the turn.
func TestPromptOwnsCancellationBeforeDispatch(t *testing.T) {
	t.Parallel()
	a := NewAgent(testOptions(t)...)
	t.Cleanup(func() { _ = a.Close() })
	a.attach(newRecorder(), nil)
	_, err := a.Initialize(t.Context(), acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber})
	require.NoError(t, err)
	created, err := a.NewSession(t.Context(), wire.NewSessionRequest(t.TempDir()))
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	response, err := a.Prompt(ctx, wire.TextPromptRequest(created.SessionId, "HELLO"))
	require.NoError(t, err)
	require.Equal(t, acp.StopReasonEndTurn, response.StopReason)
}
