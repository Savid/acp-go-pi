package piacp

import (
	"context"
	"errors"
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
		return piPrompt{}, unsupportedField(fieldPrompt)
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
			return piPrompt{}, unsupportedField(fieldPrompt)
		}
	}

	message := strings.Join(append(textParts, contextParts...), "\n")
	if strings.TrimSpace(message) == "" && len(images) == 0 {
		return piPrompt{}, unsupportedField(fieldPrompt)
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

		return "", "", nil, unsupportedField(fieldPromptResource)
	}

	return "", "", nil, unsupportedField(fieldPromptResource)
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
// Route validation runs first, then the lifecycle submission identity: a
// prompt never reports two rejections, and the order of two failures is never
// implementation-defined.
func (s *agentSession) Prompt(ctx context.Context, params acp.PromptRequest) (acp.PromptResponse, error) {
	route, err := parseInboundTurnRoute(params.Meta)
	if err != nil {
		return acp.PromptResponse{}, err
	}

	submission, err := s.agent.lifecyclePromptCorrelation(params.Meta)
	if err != nil {
		return acp.PromptResponse{}, err
	}

	if poisonErr := s.admissionFenceError(ctx); poisonErr != nil {
		return acp.PromptResponse{}, poisonErr
	}

	releaseTurn, err := s.acquireTurn(ctx)
	if err != nil {
		return acp.PromptResponse{}, err
	}

	defer releaseTurn()

	mapped, err := promptToPi(ctx, params.Prompt, s.agent.imageLimits(), s.agent.inputHandoffRoot())
	if err != nil {
		return acp.PromptResponse{}, err
	}

	if imageErr := s.rejectImagesForUnsupportedModel(mapped); imageErr != nil {
		return acp.PromptResponse{}, imageErr
	}

	delivery := newTurnDelivery()

	if admissionErr := s.claimPromptForeground(ctx, delivery); admissionErr != nil {
		return acp.PromptResponse{}, admissionErr
	}

	var cancel context.CancelFunc

	defer func() {
		if cancel != nil {
			cancel()
		}

		s.finishPromptForeground(delivery)
	}()

	outbox, client, reserveErr := s.refreshAndReservePrompt(ctx, delivery)
	if reserveErr != nil {
		var requestErr *acp.RequestError
		if errors.As(reserveErr, &requestErr) {
			return acp.PromptResponse{}, reserveErr
		}

		return acp.PromptResponse{}, s.nativeTurnFailure(ctx, reserveErr)
	}

	s.resetTurnTools()

	turnCtx, turnCancel := context.WithCancel(ctx)
	cancel = turnCancel
	turnCtx = withTurnRoute(turnCtx, route.turnNonce)

	s.cancelMu.Lock()
	s.mu.Lock()
	s.cancel = turnCancel
	s.turnCancelled = false
	s.turnNonce = route.turnNonce
	s.turnFenceStarted = false
	s.turnFenceDone = make(chan struct{})
	s.turnFenceErr = nil
	s.turnSettling = false
	s.turnCommitOnCancel = false
	s.turnImagesEmitted = false
	s.mu.Unlock()
	s.cancelMu.Unlock()

	var (
		timedOut       atomic.Bool
		nativeAccepted atomic.Bool
	)

	if timeout := s.agent.turnTimeout(); timeout > 0 {
		timer := time.AfterFunc(timeout, func() {
			_ = s.fenceTimedOutTurn(context.Background(), &timedOut)
		})
		defer timer.Stop()
	}

	boundary := pi.CallBoundary{
		BeforeDispatch: func() (func(), error) {
			return s.beginPromptDispatch(turnCtx, outbox, delivery)
		},
		Accepted: func(acceptCtx context.Context) error {
			nativeAccepted.Store(true)
			s.openSettlement()

			return s.acceptPromptResponse(acceptCtx, outbox, delivery, submission)
		},
	}
	if promptErr := client.PromptWithBoundary(turnCtx, mapped.Message, mapped.Images, boundary); promptErr != nil {
		if nativeAccepted.Load() {
			return s.settlePrompt(turnCtx, params, &promptTurnState{}, promptOutcome{failure: promptErr}, &timedOut)
		}

		if poisonErr := s.admissionFenceError(ctx); poisonErr != nil {
			return acp.PromptResponse{}, poisonErr
		}

		// The native dispatcher never took the frame, so this creates neither
		// submission nor turn and settles nothing.
		fenceErr := s.fenceTurnAfterFailure(context.WithoutCancel(ctx))

		return acp.PromptResponse{}, errors.Join(s.nativeTurnFailure(ctx, promptErr), fenceErr)
	}

	state := &promptTurnState{}
	outcomeOfTurn := s.runPromptTurn(turnCtx, outbox, delivery, state)

	return s.settlePrompt(turnCtx, params, state, outcomeOfTurn, &timedOut)
}

// finishPromptForeground ends one prompt's hold on the session and hands the
// router back. The order is the point: the turn's route, its delivery, and the
// shared tool tracker are all finished before the release, because the release
// wakes the pump into a drain that can open an agent-origin cycle immediately.
// Releasing first would let this turn's own cleanup erase the first tool that
// cycle started.
func (s *agentSession) finishPromptForeground(delivery *turnDelivery) {
	s.cancelMu.Lock()

	s.mu.Lock()
	s.cancel = nil
	s.turnCancelled = false
	s.turnNonce = ""
	s.turnSettling = false
	s.turnNativeSettled = false
	s.turnCommitOnCancel = false

	if s.turnEvents == delivery {
		s.turnEvents = nil
	}

	if s.promptAdmission == delivery {
		s.promptAdmission = nil
	}

	outbox := s.outbox
	s.mu.Unlock()

	delivery.abandonQueuedDialogs(context.Background(), s)
	close(delivery.done)
	s.resetTurnTools()
	s.cancelMu.Unlock()

	outbox.release(delivery)
	outbox.releasePromptAdmission(delivery)
}

// promptOutcome is how the streaming loop ended. Every accepted exit returns
// one rather than returning a response directly, so exactly one settlement
// point covers them all.
type promptOutcome struct {
	// settled reports that native pi crossed agent_settled for this turn.
	settled bool
	// transportEnded reports that this generation's event stream ended before
	// the run settled.
	transportEnded bool
	// contextEnded reports that the turn context ended before the run settled.
	contextEnded bool
	// failure is an update-emission or lifecycle-emission failure the loop
	// stopped on.
	failure error
}

// runPromptTurn streams the accepted turn until it ends, and reports how it
// ended instead of writing a response of its own.
func (s *agentSession) runPromptTurn(
	ctx context.Context,
	outbox *sessionOutbox,
	delivery *turnDelivery,
	state *promptTurnState,
) promptOutcome {
	deliveryCtx, cancelDelivery := context.WithTimeout(context.WithoutCancel(ctx), sessionSettleTimeout)
	defer cancelDelivery()

	for {
		select {
		case event, ok := <-delivery.events:
			if !ok {
				return promptOutcome{transportEnded: true}
			}

			settled, err := s.handleTurnEvent(deliveryCtx, event, state)
			if err != nil {
				return promptOutcome{failure: err}
			}

			if settled {
				return promptOutcome{settled: true}
			}
		case dialog := <-delivery.uiRequests:
			if dialog != nil && dialog.claim() {
				go func() {
					defer dialog.complete()
					defer recoverAgentGoroutine(context.WithoutCancel(ctx), agentLogger(s.agent), "UI dialog handler")

					s.handleNativeUIDialog(ctx, dialog)
				}()
			}
		case <-ctx.Done():
			// Internal containment cancels the turn context after the client has
			// terminalized a malformed or unterminated JSONL stream. That fixed
			// structural terminal outranks the cancellation it caused; otherwise
			// the same T4 record can nondeterministically become a successful
			// cancelled response.
			if outbox != nil && outbox.client != nil && outbox.client.Err() != nil {
				return promptOutcome{transportEnded: true}
			}

			return promptOutcome{contextEnded: true}
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
	case pi.ExtensionErrorEvent:
		// The wrapper-owned extensions are the permission bridge and the MCP
		// client, so an extension that threw is a cycle whose permission
		// admission or tool surface may no longer be the one the host believes
		// it authorized. The cycle fails closed and states nothing about which
		// extension or why: the native path and the thrown error are
		// adapter-internal and reach the operator's log, never the client.
		return false, extensionTurnFailure()
	default:
		// The remaining decoded types are foreground machinery inside
		// agent_settled's own scope — the agent and turn brackets, the queue
		// update, and the compaction and auto-retry pairs — plus a type this
		// package does not model. None carries an entity ACP or the lifecycle
		// extension can name, so none projects an update. They still reached
		// the session outbox, which is what keeps them off the raw-event
		// stream's gap detector.
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
			// Every verdict normalization raises here is one the model can act
			// on — bad base64, non-raster bytes, a contradicted media type, an
			// oversize image. An assistant image has no tool call to attribute
			// to, so the guidance takes the image's place as agent text and the
			// turn runs on with its context.
			guidance, _ := imageOutputGuidance(failure)

			if err := s.emitUpdates(ctx, []acp.SessionUpdate{{
				AgentMessageChunk: &acp.SessionUpdateAgentMessageChunk{
					Content: acp.TextBlock(guidance), MessageId: messageIDPtr,
				},
			}}); err != nil {
				return err
			}

			continue
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
		s.agent.log.Debug("unknown pi stop reason")

		return acp.StopReasonEndTurn
	}
}

func (s *agentSession) settledSessionStats(ctx context.Context) *pi.SessionStats {
	stats, err := s.currentClient().GetSessionStats(ctx)
	if err != nil {
		s.agent.log.DebugContext(ctx, "get pi session stats failed",
			slog.String(acpFieldSessionID, string(s.id)),
		)

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
