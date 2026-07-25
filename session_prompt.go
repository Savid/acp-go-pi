package piacp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"

	"github.com/coder/acp-go-sdk"

	"github.com/savid/acp-go-pi/internal/pi"
)

const (
	fieldPrompt         = "prompt"
	fieldPromptImage    = "prompt.image"
	fieldPromptResource = "prompt.resource"

	errMissingResourceData = "missing resource data or uri"

	assistantEventTextDelta     = "text_delta"
	assistantEventThinkingDelta = "thinking_delta"

	stopReasonStop      = "stop"
	stopReasonLength    = "length"
	stopReasonMaxTokens = "max_tokens"
	stopReasonAborted   = "aborted"
	stopReasonError     = "error"
)

// piPrompt is one mapped native prompt: the message text plus attached
// images.
type piPrompt struct {
	Message string
	Images  []pi.ImageContent
	// ImageField is the request member the first attached image arrived on, so a
	// refusal of the prompt's images reports the channel that carried them
	// rather than assuming the image block form.
	ImageField string
}

// promptToPi converts ACP prompt content to pi's prompt command shape.
// Embedded context is flattened into the message text at map time; audio is
// rejected. pi's prompt images take base64 payloads only, so a validated
// handoff file's bytes are encoded into the native request and the handoff
// path itself never leaves the validation read. Image validation is
// deterministic and stops on the first failing block in request order.
func promptToPi(ctx context.Context, prompt []acp.ContentBlock, limits ImageLimits, handoffRoot string) (piPrompt, error) {
	if len(prompt) == 0 {
		return piPrompt{}, acp.NewInvalidParams(map[string]any{jsonFieldError: validationUnsupported, jsonFieldField: fieldPrompt})
	}

	textParts := make([]string, 0, len(prompt))
	contextParts := make([]string, 0)
	images := make([]pi.ImageContent, 0)
	budget := newPromptImageBudget(limits, handoffRoot)

	defer budget.closeHandoffRoot()

	for _, block := range prompt {
		if err := ctx.Err(); err != nil {
			return piPrompt{}, err
		}

		switch {
		case block.Text != nil:
			if textAudienceIsUserOnly(block.Text.Annotations) {
				continue
			}

			textParts = append(textParts, block.Text.Text)
		case block.Image != nil:
			data, err := budget.validateBlock(ctx, block.Image)
			if err != nil {
				return piPrompt{}, err
			}

			images = append(images, pi.NewImageContent(data, block.Image.MimeType))
		case block.ResourceLink != nil:
			textParts = append(textParts, strings.TrimSpace(block.ResourceLink.Uri))
		case block.Resource != nil:
			text, contextText, image, err := resourceToPi(block.Resource.Resource, budget)
			if err != nil {
				return piPrompt{}, err
			}

			if text != "" {
				textParts = append(textParts, text)
			}

			if contextText != "" {
				contextParts = append(contextParts, contextText)
			}

			if image != nil {
				images = append(images, *image)
			}
		default:
			return piPrompt{}, acp.NewInvalidParams(map[string]any{jsonFieldError: validationUnsupported, jsonFieldField: fieldPrompt})
		}
	}

	message := strings.Join(append(textParts, contextParts...), "\n")
	if strings.TrimSpace(message) == "" && len(images) == 0 {
		return piPrompt{}, acp.NewInvalidParams(map[string]any{jsonFieldError: validationUnsupported, jsonFieldField: fieldPrompt})
	}

	return piPrompt{Message: message, Images: images, ImageField: budget.firstImageField}, nil
}

func resourceToPi(resource acp.EmbeddedResourceResource, budget *promptImageBudget) (string, string, *pi.ImageContent, error) {
	if resource.TextResourceContents != nil {
		if err := budget.chargeText(int64(len(resource.TextResourceContents.Text))); err != nil {
			return "", "", nil, err
		}

		contextText := contextResourceText(resource.TextResourceContents.Uri, resource.TextResourceContents.Text)
		text := strings.TrimSpace(resource.TextResourceContents.Uri)

		return text, contextText, nil, nil
	}

	if resource.BlobResourceContents != nil {
		mimeType := ""
		if resource.BlobResourceContents.MimeType != nil {
			mimeType = *resource.BlobResourceContents.MimeType
		}

		if strings.HasPrefix(inboundMediaTypeRoute(mimeType), "image/") {
			if err := budget.validate(resource.BlobResourceContents.Blob, mimeType); err != nil {
				return "", "", nil, err
			}

			image := pi.NewImageContent(resource.BlobResourceContents.Blob, mimeType)

			return "", "", &image, nil
		}

		return "", "", nil, acp.NewInvalidParams(map[string]any{jsonFieldError: validationUnsupported, jsonFieldField: fieldPromptResource})
	}

	return "", "", nil, acp.NewInvalidParams(map[string]any{jsonFieldField: fieldPromptResource, jsonFieldError: errMissingResourceData})
}

func textAudienceIsUserOnly(annotations *acp.Annotations) bool {
	return annotations != nil &&
		len(annotations.Audience) == 1 &&
		annotations.Audience[0] == acp.RoleUser
}

func contextResourceText(uri string, text string) string {
	var output strings.Builder

	output.WriteString("\n<context ref=\"")
	output.WriteString(xmlEscape(uri))
	output.WriteString("\">\n")
	output.WriteString(xmlEscape(text))
	output.WriteString("\n</context>")

	return output.String()
}

func xmlEscape(text string) string {
	replacer := strings.NewReplacer(
		"&", "&amp;",
		"<", "&lt;",
		">", "&gt;",
		`"`, "&quot;",
		"'", "&apos;",
	)

	return replacer.Replace(text)
}

// Prompt sends one turn to pi and streams updates until the run settles.
func (s *agentSession) Prompt(ctx context.Context, params acp.PromptRequest) (acp.PromptResponse, error) {
	route, err := parseInboundTurnRoute(params.Meta)
	if err != nil {
		return acp.PromptResponse{}, err
	}

	if poisonErr := s.poisonedError(); poisonErr != nil {
		return acp.PromptResponse{}, poisonErr
	}

	releaseTurn, err := s.acquireTurn(ctx)
	if err != nil {
		return acp.PromptResponse{}, err
	}
	defer releaseTurn()

	if poisonErr := s.poisonedError(); poisonErr != nil {
		return acp.PromptResponse{}, poisonErr
	}

	mapped, err := promptToPi(ctx, params.Prompt, s.agent.imageLimits(), s.agent.inputHandoffRoot())
	if err != nil {
		return acp.PromptResponse{}, err
	}

	if err := s.rejectImagesForUnsupportedModel(mapped); err != nil {
		return acp.PromptResponse{}, err
	}

	if err := s.refreshMCPTools(ctx); err != nil {
		return acp.PromptResponse{}, s.nativeTurnFailure(err)
	}
	defer s.observeProviderProcess(context.WithoutCancel(ctx))

	s.resetTurnTools()

	turnCtx, cancel := context.WithCancel(ctx)
	turnCtx = withTurnRoute(turnCtx, route.turnNonce)

	sink := newTurnSink()

	s.cancelMu.Lock()
	s.mu.Lock()
	s.cancel = cancel
	s.turnCancelled = false
	s.turnNonce = route.turnNonce
	s.turnSink = sink
	s.turnNativeSettled = false
	s.turnFenceStarted = false
	s.turnFenceDone = make(chan struct{})
	s.turnFenceErr = nil
	s.turnSettling = false
	s.turnCommitOnCancel = false
	s.turnImagesEmitted = false
	s.mu.Unlock()
	s.cancelMu.Unlock()

	defer func() {
		cancel()

		s.cancelMu.Lock()
		defer s.cancelMu.Unlock()

		s.mu.Lock()
		s.cancel = nil
		s.turnCancelled = false
		s.turnNonce = ""
		s.turnSettling = false
		s.turnNativeSettled = false
		s.turnCommitOnCancel = false

		if s.turnSink == sink {
			s.turnSink = nil
		}
		s.mu.Unlock()

		close(sink.done)
		s.resetTurnTools()
	}()

	var timedOut atomic.Bool

	if timeout := s.agent.turnTimeout(); timeout > 0 {
		timer := time.AfterFunc(timeout, func() {
			_ = s.fenceTimedOutTurn(context.Background(), &timedOut)
		})
		defer timer.Stop()
	}

	client := s.currentClient()
	if promptErr := client.Prompt(turnCtx, mapped.Message, mapped.Images); promptErr != nil {
		fenceErr := s.fenceTurnAfterFailure(context.WithoutCancel(ctx))

		return acp.PromptResponse{}, errors.Join(s.nativeTurnFailure(promptErr), fenceErr)
	}

	state := &promptTurnState{}

	for {
		select {
		case event, ok := <-sink.events:
			if !ok {
				if fenceErr := s.fenceTurnAfterFailure(context.WithoutCancel(ctx)); fenceErr != nil {
					return acp.PromptResponse{}, fenceErr
				}

				return s.transportEndedTurn(params.MessageId, &timedOut)
			}

			s.emitRawPiEvent(turnCtx, event.RawJSON())

			settled, err := s.handleTurnEvent(turnCtx, event, state)
			if err != nil {
				return acp.PromptResponse{}, s.abortAfterEmitError(ctx, err)
			}

			if settled {
				return s.finishTurn(ctx, turnCtx, params, state, &timedOut)
			}
		case request := <-sink.uiRequests:
			s.emitRawPiEvent(turnCtx, request.RawJSON())

			if request.IsDialog() {
				s.dialogWG.Add(1)

				go func() {
					defer s.dialogWG.Done()
					defer recoverAgentGoroutine(context.WithoutCancel(turnCtx), agentLogger(s.agent), "UI dialog handler")

					s.handleUIDialog(turnCtx, request)
				}()
			}
		case <-turnCtx.Done():
			if fenceErr := s.fenceTurnAfterContext(context.WithoutCancel(ctx)); fenceErr != nil {
				return acp.PromptResponse{}, fenceErr
			}

			return s.contextEndedTurn(params.MessageId, &timedOut)
		}
	}
}

// handleTurnEvent maps one native event to ACP session updates; it reports
// whether the run settled.
func (s *agentSession) handleTurnEvent(ctx context.Context, event pi.Event, state *promptTurnState) (bool, error) {
	switch typed := event.(type) {
	case pi.AgentSettledEvent:
		return true, nil
	case pi.MessageStartEvent:
		if typed.Message.Role == messageRoleAssistant {
			state.model = typed.Message.Model
			state.provider = typed.Message.Provider
		}

		return false, nil
	case pi.MessageUpdateEvent:
		return false, s.emitAssistantDelta(ctx, typed.AssistantMessageEvent)
	case pi.MessageEndEvent:
		if typed.Message.Role == messageRoleAssistant {
			observeAssistantMessageEnd(typed.Message, state)

			if err := s.emitAssistantImages(ctx, typed.Message, state); err != nil {
				return false, err
			}

			if err := s.emitNativeMessageIdentity(ctx, typed.Message.ACPMessageID); err != nil {
				return false, err
			}
		}

		return false, nil
	case pi.ToolExecutionStartEvent:
		return false, s.publishNativeToolStart(ctx, typed)
	case pi.ToolExecutionUpdateEvent:
		if typed.PartialResult == nil {
			return false, nil
		}

		return false, s.publishNativeToolUpdate(ctx, typed.ToolCallID, typed.PartialResult.Content)
	case pi.ToolExecutionEndEvent:
		status := acp.ToolCallStatusCompleted
		if typed.IsError {
			status = acp.ToolCallStatusFailed
		}

		return false, s.publishNativeToolTerminal(ctx, typed.ToolCallID, status, typed.Result)
	default:
		return false, nil
	}
}

// emitAssistantImages projects image blocks carried by a finalized assistant
// message as agent message chunks, one image per chunk, validated and
// de-duplicated by native message identity plus artifact fingerprint. pi's
// streaming deltas carry only text and thinking, so the finalized message is
// the sole live source of assistant image content.
func (s *agentSession) emitAssistantImages(ctx context.Context, message pi.AgentMessage, state *promptTurnState) error {
	// A content payload that does not decode carries no image blocks to
	// project; unrelated decode problems keep their existing handling.
	blocks, _ := message.ContentBlocks()
	perImage := effectiveOutputImageLimit(s.agent.imageLimits().MaxOutputBytesPerImage)

	var messageIDPtr *string

	if message.ACPMessageID != "" {
		messageID := message.ACPMessageID
		messageIDPtr = &messageID
	}

	for index := range blocks {
		block := &blocks[index]
		if block.Type != contentBlockTypeImage {
			continue
		}

		image, failure := normalizeOutputImage(block.Data, block.MimeType, perImage)
		if failure != nil {
			return imageOutputTurnFailure(failure)
		}

		key := message.ACPMessageID + ":" + image.fingerprint

		if state.agentImages == nil {
			state.agentImages = make(map[string]struct{}, 1)
		}

		if _, seen := state.agentImages[key]; seen {
			continue
		}

		update := acp.SessionUpdate{AgentMessageChunk: &acp.SessionUpdateAgentMessageChunk{
			Content: acp.ImageBlock(image.data, image.mime), MessageId: messageIDPtr,
		}}
		if err := s.emitUpdatesWithNativeMessageID(ctx, []acp.SessionUpdate{update}, message.ACPMessageID); err != nil {
			return err
		}

		state.agentImages[key] = struct{}{}

		s.markTurnImageEmission()
	}

	return nil
}

// markTurnImageEmission records that the live turn delivered image bytes, so
// a failed mirror commit afterwards is a durability loss for emitted
// artifacts.
func (s *agentSession) markTurnImageEmission() {
	s.mu.Lock()
	s.turnImagesEmitted = true
	s.mu.Unlock()
}

func (s *agentSession) turnEmittedImages() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.turnImagesEmitted
}

// imageAwareMirrorFailure maps a failed mirror commit after image emission
// to the storage_failed image-output envelope: the turn delivered image
// bytes whose durable replay representation was lost.
func (s *agentSession) imageAwareMirrorFailure(err error) error {
	if !s.turnEmittedImages() {
		return err
	}

	return storageFailure("session mirror commit failed after image output: " + err.Error())
}

func (s *agentSession) emitAssistantDelta(ctx context.Context, delta pi.AssistantMessageEvent) error {
	switch delta.Type {
	case assistantEventTextDelta:
		if delta.Delta == "" {
			return nil
		}

		return s.emitUpdates(ctx, []acp.SessionUpdate{acp.UpdateAgentMessageText(delta.Delta)})
	case assistantEventThinkingDelta:
		if delta.Delta == "" {
			return nil
		}

		return s.emitUpdates(ctx, []acp.SessionUpdate{acp.UpdateAgentThoughtText(delta.Delta)})
	default:
		return nil
	}
}

func observeAssistantMessageEnd(message pi.AgentMessage, state *promptTurnState) {
	if message.ACPMessageID != "" {
		state.nativeMessageID = message.ACPMessageID
	}

	if message.Model != "" {
		state.model = message.Model
	}

	if message.Provider != "" {
		state.provider = message.Provider
	}

	if message.StopReason != "" {
		state.stopReason = message.StopReason
	}

	if message.ErrorMessage != "" {
		state.errorMessage = message.ErrorMessage
	}

	if message.Usage != nil {
		state.usage = mergeTurnUsage(state.usage, message.Usage)

		if message.Usage.Cost != nil {
			state.cost = message.Usage.Cost
		}
	}
}

func mergeTurnUsage(total *acp.Usage, next *pi.Usage) *acp.Usage {
	if next == nil {
		return total
	}

	if total == nil {
		total = &acp.Usage{
			CachedReadTokens:  acp.Ptr(0),
			CachedWriteTokens: acp.Ptr(0),
		}
	}

	total.InputTokens += int(next.Input)
	total.OutputTokens += int(next.Output)
	*total.CachedReadTokens += int(next.CacheRead)
	*total.CachedWriteTokens += int(next.CacheWrite)
	total.TotalTokens = total.InputTokens + total.OutputTokens + *total.CachedReadTokens + *total.CachedWriteTokens

	return total
}

// finishTurn runs the settle fence: usage query, usage update, session info,
// the awaited mirror commit, then the cancel guard and failure mapping.
func (s *agentSession) finishTurn(
	ctx context.Context,
	turnCtx context.Context,
	params acp.PromptRequest,
	state *promptTurnState,
	timedOut *atomic.Bool,
) (acp.PromptResponse, error) {
	if fenceErr := s.claimTurnSettlement(); fenceErr != nil {
		return acp.PromptResponse{}, fenceErr
	}

	// A routed user cancellation can win the settlement race after pi has
	// already emitted agent_settled. In that case the native session file is a
	// complete durable generation (including pi's aborted assistant row), so
	// publish it before the cancelled response. The cancel fence handles the
	// opposite race, where the pump observes agent_settled but stopPump wins
	// before the prompt goroutine receives it. Paths that never reach
	// agent_settled deliberately preserve the prior mirror instead.
	if s.wasTurnCancelled() {
		s.mu.Lock()
		commitCancelled := s.turnCommitOnCancel
		s.mu.Unlock()

		if !commitCancelled {
			return cancelledResponse(params.MessageId), nil
		}

		if err := s.commitMirror(context.WithoutCancel(ctx)); err != nil {
			return acp.PromptResponse{}, s.imageAwareMirrorFailure(err)
		}

		return cancelledResponse(params.MessageId), nil
	}

	if timedOut.Load() {
		return acp.PromptResponse{}, turnFailureError(failureCauseTimeout, fmt.Sprintf("pi turn exceeded %s", s.agent.turnTimeout()))
	}

	stats := s.settledSessionStats(turnCtx)

	// A native session id change mid-session breaks the wrapper's identity
	// invariant (for example an operator-seeded extension switching
	// sessions); the session is poisoned rather than mirrored under the
	// wrong key.
	if stats != nil && stats.SessionID != "" && stats.SessionID != string(s.id) {
		return acp.PromptResponse{}, s.poison(turnCtx, fmt.Sprintf("native session id drift: expected %s, got %s", s.id, stats.SessionID))
	}

	s.emitTurnUsageUpdate(turnCtx, state, stats)

	if err := s.emitLiveSessionInfoUpdate(turnCtx, params.Prompt); err != nil {
		return acp.PromptResponse{}, s.abortAfterEmitError(ctx, err)
	}

	if err := s.commitMirror(context.WithoutCancel(ctx)); err != nil {
		return acp.PromptResponse{}, s.imageAwareMirrorFailure(err)
	}

	if err := providerTurnFailure(state); err != nil {
		return acp.PromptResponse{}, err
	}

	return acp.PromptResponse{
		Meta:          nativeMessageResponseMeta(state.nativeMessageID),
		StopReason:    acpStopReason(s, state),
		Usage:         state.usage,
		UserMessageId: params.MessageId,
	}, nil
}

func acpStopReason(s *agentSession, state *promptTurnState) acp.StopReason {
	switch state.stopReason {
	case stopReasonLength, stopReasonMaxTokens:
		return acp.StopReasonMaxTokens
	case stopReasonAborted:
		// An abort the wrapper did not request still settles the run; the
		// nearest truthful terminal state is cancelled.
		return acp.StopReasonCancelled
	case stopReasonStop, "":
		return acp.StopReasonEndTurn
	default:
		s.agent.log.Debug("unknown pi stop reason", slog.String("stop_reason", state.stopReason))

		return acp.StopReasonEndTurn
	}
}

func (s *agentSession) settledSessionStats(ctx context.Context) *pi.SessionStats {
	stats, err := s.currentClient().GetSessionStats(ctx)
	if err != nil {
		s.agent.log.DebugContext(ctx, "get pi session stats failed", slog.String(jsonFieldError, err.Error()))

		return nil
	}

	return &stats
}

// emitTurnUsageUpdate reports harness-reported usage. size is the model's
// context window from get_session_stats (or the model catalog); it is 0 when
// unknown, never fabricated.
func (s *agentSession) emitTurnUsageUpdate(ctx context.Context, state *promptTurnState, stats *pi.SessionStats) {
	used := 0
	if state.usage != nil {
		used = state.usage.TotalTokens
	}

	size := int64(0)

	if stats != nil && stats.ContextUsage != nil {
		if stats.ContextUsage.Tokens != nil {
			used = int(*stats.ContextUsage.Tokens)
		}

		size = stats.ContextUsage.ContextWindow
	}

	if size == 0 {
		size = s.currentModelContextWindow()
	}

	if state.usage == nil && used == 0 && size == 0 {
		return
	}

	update := &acp.SessionUsageUpdate{
		Size: int(size),
		Used: used,
	}
	if state.cost != nil {
		update.Cost = &acp.Cost{Amount: state.cost.Total, Currency: "USD"}
	}

	_ = s.emitOptionalUpdates(ctx, []acp.SessionUpdate{{UsageUpdate: update}})
}

func (s *agentSession) currentModelContextWindow() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.contextWindowSize
}

// transportEndedTurn maps a mid-turn native transport end. The cancel guard
// runs first; otherwise the real cause is recovered from the child exit
// status and stderr tail before falling back to a transport classification.
func (s *agentSession) transportEndedTurn(messageID *string, timedOut *atomic.Bool) (acp.PromptResponse, error) {
	if fenceErr := s.awaitTurnFence(); fenceErr != nil {
		return acp.PromptResponse{}, fenceErr
	}

	if s.wasTurnCancelled() {
		return cancelledResponse(messageID), nil
	}

	if timedOut.Load() {
		return acp.PromptResponse{}, turnFailureError(failureCauseTimeout, fmt.Sprintf("pi turn exceeded %s", s.agent.turnTimeout()))
	}

	if exitMessage, exited := s.processExitMessage(); exited {
		s.agent.observe.RecordPiProcessExit(context.Background(), "unexpected", nil)

		return acp.PromptResponse{}, turnFailureError(failureCauseProcessExit, exitMessage)
	}

	message := "pi event stream closed mid-turn"

	if client := s.currentClient(); client != nil {
		if err := client.Err(); err != nil {
			message = err.Error()
		}
	}

	return acp.PromptResponse{}, turnFailureError(failureCauseTransport, message)
}

// contextEndedTurn maps a turn context that ended before the run settled:
// a user cancel whose drain window elapsed, a timeout whose abort never
// settled, or a cancelled host request context.
func (s *agentSession) contextEndedTurn(messageID *string, timedOut *atomic.Bool) (acp.PromptResponse, error) {
	if fenceErr := s.awaitTurnFence(); fenceErr != nil {
		return acp.PromptResponse{}, fenceErr
	}

	if s.wasTurnCancelled() {
		return cancelledResponse(messageID), nil
	}

	if timedOut.Load() {
		return acp.PromptResponse{}, turnFailureError(failureCauseTimeout, fmt.Sprintf("pi turn exceeded %s", s.agent.turnTimeout()))
	}

	return cancelledResponse(messageID), nil
}

func cancelledResponse(messageID *string) acp.PromptResponse {
	return acp.PromptResponse{
		StopReason:    acp.StopReasonCancelled,
		UserMessageId: messageID,
	}
}

// abortAfterEmitError aborts the native turn under a bounded background
// context after an update-emission failure, then surfaces the original error.
func (s *agentSession) abortAfterEmitError(ctx context.Context, err error) error {
	return errors.Join(err, s.fenceTurnAfterFailure(context.WithoutCancel(ctx)))
}
