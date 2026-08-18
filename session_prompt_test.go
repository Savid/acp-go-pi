package piacp

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"

	"github.com/savid/acp-go-pi/internal/lifecycle"
	"github.com/savid/acp-go-pi/internal/pi"
)

func TestPromptContentMapping(t *testing.T) {
	mime := "image/png"
	png := fixtureBase64(t, "valid.png")
	textResource := acp.EmbeddedResourceResource{TextResourceContents: &acp.TextResourceContents{
		Uri: "file:///a<&\"'", Text: "body <&>",
	}}
	imageResource := acp.EmbeddedResourceResource{BlobResourceContents: &acp.BlobResourceContents{
		Uri: "file:///image", Blob: png, MimeType: &mime,
	}}
	userAnnotations := &acp.Annotations{Audience: []acp.Role{acp.RoleUser}}
	ignored := acp.TextBlock("ignored")
	ignored.Text.Annotations = userAnnotations

	mapped, err := promptToPi(t.Context(), []acp.ContentBlock{
		acp.TextBlock("hello"), ignored,
		acp.ImageBlock(png, "image/png"),
		acp.ResourceLinkBlock("link", " https://example.test "),
		acp.ResourceBlock(textResource),
		acp.ResourceBlock(imageResource),
	}, defaultImageLimits(), "")
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
		_, invalidErr := promptToPi(t.Context(), blocks, defaultImageLimits(), "")
		requireInvalidParams(t, invalidErr)
	}

	text, contextText, image, err := resourceToPi(textResource, newPromptImageBudget(defaultImageLimits(), ""))
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
	lateCtx := &poisonOnAdmissionContext{session: session}

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

func TestPromptParentCancellationContainsProcessTree(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	client := newStubPiClient()
	process := newStubProcess(false)
	session := &agentSession{agent: agent, id: "id", client: client, proc: process}
	session.startPump(client)

	ctx, cancel := context.WithCancel(context.Background())
	promptDone := make(chan struct {
		response acp.PromptResponse
		err      error
	}, 1)
	go func() {
		response, err := session.Prompt(ctx, TextPromptRequest("id", "parent-cancel", "hang"))
		promptDone <- struct {
			response acp.PromptResponse
			err      error
		}{response: response, err: err}
	}()

	require.Eventually(t, func() bool { return session.activeTurnDelivery() != nil }, time.Second, time.Millisecond)
	cancel()
	result := <-promptDone
	require.NoError(t, result.err)
	require.Equal(t, acp.StopReasonCancelled, result.response.StopReason)
	require.Equal(t, 1, process.killCalls)
	require.Equal(t, 1, process.closeCalls)
}

func TestPromptParentCancellationReturnsContainmentFailure(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	client := newStubPiClient()
	process := newStubProcess(false)
	process.close = pi.ErrProcessContainmentIncomplete
	session := &agentSession{agent: agent, id: "id", client: client, proc: process}
	session.startPump(client)

	ctx, cancel := context.WithCancel(context.Background())
	promptDone := make(chan error, 1)
	go func() {
		_, err := session.Prompt(ctx, TextPromptRequest("id", "parent-cancel-incomplete", "hang"))
		promptDone <- err
	}()

	require.Eventually(t, func() bool { return session.activeTurnDelivery() != nil }, time.Second, time.Millisecond)
	cancel()
	require.ErrorIs(t, <-promptDone, pi.ErrProcessContainmentIncomplete)
}

// TestSettlementReportsAnIncompleteContainmentBoundary pins that a boundary
// which did not complete outranks the terminal event it precedes: every accepted
// exit reports the containment failure and none of them commits or settles.
func TestSettlementReportsAnIncompleteContainmentBoundary(t *testing.T) {
	fenceErr := pi.ErrProcessContainmentIncomplete
	var timedOut atomic.Bool

	for name, outcome := range map[string]promptOutcome{
		"transport ended":  {transportEnded: true},
		"context ended":    {contextEnded: true},
		"natively settled": {settled: true},
	} {
		t.Run(name, func(t *testing.T) {
			done := make(chan struct{})
			close(done)
			session := &agentSession{
				agent:            NewAgent(WithLogger(slog.New(slog.DiscardHandler))),
				turnFenceStarted: true,
				turnFenceDone:    done,
				turnFenceErr:     fenceErr,
			}

			_, err := session.settlePrompt(
				t.Context(), TextPromptRequest("id", "turn", "done"), &promptTurnState{}, outcome, &timedOut,
			)
			require.ErrorIs(t, err, fenceErr)
		})
	}
}

func TestPromptTransportEndReturnsContainmentProofFailure(t *testing.T) {
	fenceErr := pi.ErrProcessContainmentIncomplete
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	client := newStubPiClient()
	process := newStubProcess(false)
	process.close = fenceErr
	session := &agentSession{agent: agent, id: "id", client: client, proc: process}
	session.startPump(client)
	t.Cleanup(session.stopPump)

	go func() {
		deadline := time.Now().Add(time.Second)
		for session.activeTurnDelivery() == nil && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}

		close(client.events)
		close(client.uiRequests)
	}()

	_, err := session.Prompt(t.Context(), TextPromptRequest("id", "transport-end", "hang"))
	require.ErrorIs(t, err, fenceErr)
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
		for session.activeTurnDelivery() == nil {
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

func TestSettlementCommitMirrorFailure(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)), WithTurnTimeout(time.Second))
	connection := newDirectAgentClient()
	agent.setConnection(connection)
	client := newStubPiClient()
	client.stats = pi.SessionStats{SessionID: "id"}
	session := &agentSession{agent: agent, id: "id", client: client, proc: newStubProcess(false), sessionFilePath: t.TempDir()}

	var timedOut atomic.Bool
	settled := promptOutcome{settled: true}
	_, err := session.settlePrompt(t.Context(), TextPromptRequest("id", "test-turn", "title"), &promptTurnState{}, settled, &timedOut)
	require.Error(t, err)

	session.turnCancelled = true
	session.turnCommitOnCancel = true
	session.turnNativeSettled = true
	_, err = session.settlePrompt(t.Context(), TextPromptRequest("id", "test-turn", "title"), &promptTurnState{}, settled, &timedOut)
	require.Error(t, err, "a settled cancellation must not hide its mirror failure")
}

func TestSettledCancelCommitsMirrorBeforeItsTerminalIdle(t *testing.T) {
	store := NewInMemorySessionStore()
	agent := NewAgent(
		WithLogger(slog.New(slog.DiscardHandler)),
		WithSessionStore(store),
	)
	path := filepath.Join(t.TempDir(), "session.jsonl")
	require.NoError(t, os.WriteFile(path, []byte("{\"type\":\"session\"}\n{\"type\":\"message\"}\n"), 0o600))
	session := &agentSession{
		agent:              agent,
		id:                 "id",
		sessionFilePath:    path,
		turnCancelled:      true,
		turnCommitOnCancel: true,
		turnNativeSettled:  true,
	}

	var timedOut atomic.Bool
	timedOut.Store(true)
	response, err := session.settlePrompt(
		t.Context(), TextPromptRequest("id", "test-turn", "title"), &promptTurnState{}, promptOutcome{settled: true}, &timedOut,
	)
	require.NoError(t, err)
	require.Equal(t, acp.StopReasonCancelled, response.StopReason,
		"an explicit cancel must win a simultaneous timeout")
	entries, err := store.Load(t.Context(), SessionKey{SessionID: "id"})
	require.NoError(t, err)
	require.Len(t, entries, 2, "the durable cancelled generation must precede the response")
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

	request := TextPromptRequest("id", "turn", "done")
	settle := func(outcome promptOutcome) (acp.PromptResponse, error) {
		return session.settlePrompt(t.Context(), request, &promptTurnState{}, outcome, &timedOut)
	}

	response, err := settle(promptOutcome{transportEnded: true})
	requirePiTurnFailure(t, err, failureCauseTransport)
	require.Empty(t, response.StopReason)
	client.err = errors.New("native stream")
	_, err = settle(promptOutcome{transportEnded: true})
	requirePiTurnFailure(t, err, failureCauseTransport)

	timedOut.Store(true)
	_, err = settle(promptOutcome{transportEnded: true})
	requirePiTurnFailure(t, err, failureCauseTimeout)
	_, err = settle(promptOutcome{contextEnded: true})
	requirePiTurnFailure(t, err, failureCauseTimeout)
	timedOut.Store(false)
	response, err = settle(promptOutcome{contextEnded: true})
	require.NoError(t, err)
	require.Equal(t, acp.StopReasonCancelled, response.StopReason)

	session.turnCancelled = true
	response, err = settle(promptOutcome{transportEnded: true})
	require.NoError(t, err)
	require.Equal(t, acp.StopReasonCancelled, response.StopReason)
	response, err = settle(promptOutcome{contextEnded: true})
	require.NoError(t, err)
	require.Equal(t, acp.StopReasonCancelled, response.StopReason)

	session.turnCancelled = false
	session.proc = newStubProcess(true)
	_, err = settle(promptOutcome{transportEnded: true})
	requirePiTurnFailure(t, err, failureCauseProcessExit)

	session.proc = newStubProcess(false)
	session.client = nil
	requirePiTurnFailure(t, session.transportFailure(t.Context()), failureCauseTransport)
}

func TestPromptAndSettlementErrorBranches(t *testing.T) {
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

		return session.settlePrompt(t.Context(), TextPromptRequest("id", "test-turn", "title"), state, promptOutcome{settled: true}, &timeout)
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

// TestPromptRejectsMalformedLifecycleCorrelation pins the validation order:
// the lifecycle submission identity is validated right after the route, so a
// negotiated prompt carrying neither a valid version nor a valid submission
// fails before admission and creates neither submission nor turn.
func TestPromptRejectsMalformedLifecycleCorrelation(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	agent.lifecycle = lifecycle.Negotiated{Versions: []int{1}, UpdatesOutsidePrompt: true, ActivityKinds: []lifecycle.ActivityKind{}}
	session := &agentSession{agent: agent, id: "id", client: newStubPiClient(), proc: newStubProcess(false)}

	missing := TextPromptRequest("id", "turn", "hi")
	_, err := session.Prompt(t.Context(), missing)
	require.Error(t, err, "a negotiated prompt without its correlation fails")

	malformed := TextPromptRequest("id", "turn", "hi")
	malformed.Meta[lifecycleMetaKey] = decodeMeta(t, `{"version":1.5}`)
	_, err = session.Prompt(t.Context(), malformed)
	require.Error(t, err, "a fractional version is no negotiated version")
}

// decodeMeta decodes a lifecycle correlation value the way the wire delivers
// it, so numbers arrive as the float64 a real host's decoder produces.
func decodeMeta(t *testing.T, raw string) map[string]any {
	t.Helper()
	var meta map[string]any
	require.NoError(t, json.Unmarshal([]byte(raw), &meta))

	return meta
}

// TestPromptAcceptanceDeliveryFailureFailsTheTurn pins that a prompt whose
// prompt_accepted event cannot be delivered still runs the whole settlement
// order and answers with the failure: the incarnation fences, the boundary
// record commits, and no terminal idle is published.
func TestPromptAcceptanceDeliveryFailureFailsTheTurn(t *testing.T) {
	store := NewInMemorySessionStore()
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)), WithSessionStore(store))
	agent.lifecycle = lifecycle.Negotiated{Versions: []int{1}, UpdatesOutsidePrompt: true, ActivityKinds: []lifecycle.ActivityKind{}}
	connection := newDirectAgentClient()
	agent.setConnection(connection)
	session := &agentSession{agent: agent, id: "id", client: newStubPiClient(), proc: newStubProcess(false)}
	require.NoError(t, session.openLifecycleStream(t.Context(), 0))

	connection.updateErr = errors.New("accept delivery")
	request := TextPromptRequest("id", "turn", "hi")
	request.Meta[lifecycleMetaKey] = decodeMeta(t, `{"version":1,"submission":{"submissionId":"submission","clientNonce":"nonce"}}`)
	_, err := session.Prompt(t.Context(), request)
	require.ErrorContains(t, err, "accept delivery")
	require.True(t, session.lc.fenced, "an undeliverable acceptance fences the incarnation")

	entries, loadErr := store.Load(t.Context(), SessionKey{SessionID: "id", Subpath: SessionStoreLifecycleSubpath})
	require.NoError(t, loadErr)
	require.Len(t, entries, 1, "the failed cycle still records how the incarnation ended")
	require.Contains(t, string(entries[0]), `"outcome":"failed"`)
}

// TestAwaitSettlementWaitsForTheWholeOrder pins the latch semantics: close and
// delete block until settlement has finished the whole order and then observe
// its result, and a completed latch settles nothing twice.
func TestAwaitSettlementWaitsForTheWholeOrder(t *testing.T) {
	session := &agentSession{}
	require.NoError(t, session.awaitSettlement(), "no armed settlement settles nothing")

	session.openSettlement()
	settleErr := errors.New("settlement commit failed")
	done := make(chan error, 1)
	go func() { done <- session.awaitSettlement() }()

	select {
	case err := <-done:
		t.Fatalf("awaitSettlement returned before the order completed: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	session.completeSettlement(settleErr)
	require.ErrorIs(t, <-done, settleErr)
	require.NoError(t, session.awaitSettlement(), "a completed latch is consumed")
}

func TestTurnOutcomeForStopReason(t *testing.T) {
	require.Equal(t, lifecycle.OutcomeLimit, turnOutcomeForStopReason(acp.StopReasonMaxTokens))
	require.Equal(t, lifecycle.OutcomeLimit, turnOutcomeForStopReason(acp.StopReasonMaxTurnRequests))
	require.Equal(t, lifecycle.OutcomeCancelled, turnOutcomeForStopReason(acp.StopReasonCancelled))
	require.Equal(t, lifecycle.OutcomeRefused, turnOutcomeForStopReason(acp.StopReasonRefusal))
	require.Equal(t, lifecycle.OutcomeSuccess, turnOutcomeForStopReason(acp.StopReasonEndTurn))
}
