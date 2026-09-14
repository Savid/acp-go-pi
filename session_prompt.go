package piacp

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/coder/acp-go-sdk"

	"github.com/savid/acp-go-core/image"
	"github.com/savid/acp-go-core/lifecycle"
	"github.com/savid/acp-go-core/wire"
	"github.com/savid/acp-go-pi/internal/pi"
)

const (
	limitSessionPrompt = "session_prompt"

	stopReasonStop      = "stop"
	stopReasonLength    = "length"
	stopReasonMaxTokens = "max_tokens"
	stopReasonAborted   = "aborted"
	stopReasonError     = "error"

	// extensionFailureMessage is the whole of what a client is told about a
	// wrapper extension that threw: its path and thrown text are internal.
	extensionFailureMessage = "a pi extension failed"
	// nativeCauseMaxBytes bounds the native cause text a failure carries.
	nativeCauseMaxBytes = 2048
	// processExitGrace is how long failure classification waits for a dead
	// child to be reaped after its stdout closed.
	processExitGrace = 2 * time.Second
)

// nativePrompt is one mapped prompt: the message text plus attached images.
type nativePrompt struct {
	message string
	images  []pi.ImageContent
	// firstImage is the gated-media index and field of the first image, for
	// the selected-model refusal.
	firstImage *image.Decoded
}

// mapPrompt converts ACP prompt content to pi's prompt shape. Embedded
// context is appended to the message text; images run the core input gates
// and travel as inline base64.
func (s *session) mapPrompt(ctx context.Context, blocks []acp.ContentBlock) (nativePrompt, error) {
	if len(blocks) == 0 {
		return nativePrompt{}, wire.Unsupported("prompt")
	}

	decoded, refusal, err := image.ValidatePrompt(ctx, blocks, image.Options{
		Limits:      s.agent.options.ImageLimits.core(),
		HandoffRoot: s.agent.options.InputHandoffRoot,
		Blobs:       func(string) image.BlobDisposition { return image.BlobRefuse },
	})
	if err != nil {
		return nativePrompt{}, err
	}

	if refusal != nil {
		return nativePrompt{}, refusal.InvalidParams()
	}

	textParts := make([]string, 0, len(blocks))
	contextParts := make([]string, 0)

	for _, block := range blocks {
		switch {
		case block.Text != nil:
			if audienceIsUserOnly(block.Text.Annotations) {
				continue
			}

			textParts = append(textParts, block.Text.Text)
		case block.Image != nil:
		case block.ResourceLink != nil:
			textParts = append(textParts, strings.TrimSpace(block.ResourceLink.Uri))
		case block.Resource != nil:
			if text := block.Resource.Resource.TextResourceContents; text != nil {
				textParts = append(textParts, strings.TrimSpace(text.Uri))
				contextParts = append(contextParts, contextResourceText(text.Uri, text.Text))
			}
		default:
			return nativePrompt{}, wire.Unsupported("prompt")
		}
	}

	prompt := nativePrompt{
		message: strings.Join(append(textParts, contextParts...), "\n"),
		images:  make([]pi.ImageContent, 0, len(decoded)),
	}

	for index := range decoded {
		if prompt.firstImage == nil {
			prompt.firstImage = &decoded[index]
		}

		prompt.images = append(prompt.images, pi.NewImageContent(base64.StdEncoding.EncodeToString(decoded[index].Data), decoded[index].MIME))
	}

	if strings.TrimSpace(prompt.message) == "" && len(prompt.images) == 0 {
		return nativePrompt{}, wire.Unsupported("prompt")
	}

	return prompt, nil
}

func audienceIsUserOnly(annotations *acp.Annotations) bool {
	return annotations != nil && len(annotations.Audience) == 1 && annotations.Audience[0] == acp.RoleUser
}

func contextResourceText(uri string, text string) string {
	escape := strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;", `"`, "&quot;", "'", "&apos;")

	return "\n<context ref=\"" + escape.Replace(uri) + "\">\n" + escape.Replace(text) + "\n</context>"
}

// rejectImagesForModel refuses image input pre-turn when the selected model's
// catalog entry says it accepts no images. An absent model, field, or empty
// list forwards.
func (s *session) rejectImagesForModel(prompt nativePrompt) error {
	if prompt.firstImage == nil {
		return nil
	}

	s.mu.Lock()
	model := s.model
	models := s.models
	s.mu.Unlock()

	for index := range models {
		candidate := &models[index]
		if candidate.Ref() != model || len(candidate.Input) == 0 {
			continue
		}

		if slices.Contains(candidate.Input, "image") {
			return nil
		}

		return image.UnsupportedByModel(prompt.firstImage.Field, prompt.firstImage.Index).InvalidParams()
	}

	return nil
}

// prompt sends one turn to pi and streams updates until the run settles.
func (s *session) prompt(ctx context.Context, params acp.PromptRequest, raw json.RawMessage) (acp.PromptResponse, error) {
	meta := lifecycle.RetainRequestMetadata(params.Meta, raw)

	submission, paramErr := lifecycle.DecodePromptCorrelation(meta, s.lifecycleNegotiated())
	if paramErr != nil {
		return acp.PromptResponse{}, invalidParam(paramErr)
	}

	if err := s.admissionError(); err != nil {
		return acp.PromptResponse{}, err
	}

	release, err := s.acquireGate(limitSessionPrompt)
	if err != nil {
		return acp.PromptResponse{}, err
	}
	defer release()

	s.mu.Lock()
	busy := s.cycle != nil
	s.mu.Unlock()

	if busy {
		return acp.PromptResponse{}, wire.Backpressure(limitSessionPrompt)
	}

	mapped, err := s.mapPrompt(ctx, params.Prompt)
	if err != nil {
		if ctx.Err() != nil {
			return cancelledResponse(params), nil
		}

		return acp.PromptResponse{}, err
	}

	// session/cancel cancels this request's context through the SDK. A cancel
	// that lands before native dispatch creates neither submission nor turn and
	// answers cancelled.
	if ctx.Err() != nil {
		return cancelledResponse(params), nil
	}

	rt, err := s.ensureRuntime(ctx)
	if err != nil {
		return acp.PromptResponse{}, err
	}

	if err := s.rejectImagesForModel(mapped); err != nil {
		return acp.PromptResponse{}, err
	}

	t := &turn{
		cycle:      cycle{origin: lifecycle.CauseSubmission, state: cycleState{tools: make(map[string]*toolState)}},
		submission: submission,
		settled:    make(chan struct{}),
		finished:   make(chan struct{}),
	}
	defer close(t.finished)

	s.mu.Lock()
	s.turn = t
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		if s.turn == t {
			s.turn = nil
		}
		s.mu.Unlock()
	}()

	if timeout := s.agent.options.TurnTimeout; timeout > 0 {
		timer := time.AfterFunc(timeout, func() { s.timeout(context.WithoutCancel(ctx), t) })
		defer timer.Stop()
	}

	if err := rt.client.Prompt(ctx, mapped.message, mapped.images); err != nil {
		if !t.accepted {
			if ctx.Err() != nil {
				return cancelledResponse(params), nil
			}

			return acp.PromptResponse{}, s.dispatchFailure(ctx, rt, err)
		}
	}

	s.acceptTurn(ctx, t)

	select {
	case <-t.settled:
	case <-ctx.Done():
		s.cancel(ctx)

		select {
		case <-t.settled:
		case <-time.After(sessionAbortTimeout):
			t.settle(turnTransportEnded)
		}
	}

	return s.settleTurn(ctx, rt, t, params)
}

func cancelledResponse(params acp.PromptRequest) acp.PromptResponse {
	return acp.PromptResponse{StopReason: acp.StopReasonCancelled, UserMessageId: params.MessageId}
}

// dispatchFailure classifies a prompt command pi never accepted: a native
// rejection carries its text as a provider failure, a dead child is a
// process exit, and everything else is transport.
func (s *session) dispatchFailure(ctx context.Context, rt *runtime, err error) error {
	var commandErr *pi.CommandError
	if errors.As(err, &commandErr) {
		return turnFailure(wire.CauseProvider, commandErr.Message)
	}

	return s.transportFailure(ctx, rt, err)
}

// transportFailure recovers the real cause behind a lost native stream: the
// child's exit status and last stderr line where it died, otherwise the
// transport error.
func (s *session) transportFailure(ctx context.Context, rt *runtime, err error) error {
	waitCtx, cancel := context.WithTimeout(ctx, processExitGrace)
	defer cancel()

	if result, waitErr := rt.proc.Wait(waitCtx); waitErr == nil {
		message := fmt.Sprintf("pi process exited with status %d", result.ExitCode)
		if result.Signal != 0 {
			message = fmt.Sprintf("pi process was killed by signal %d", result.Signal)
		}

		if line := rt.stderr.lastLine(); line != "" {
			message += ": " + line
		}

		return turnFailure(wire.CauseProcessExit, message)
	}

	if err == nil {
		err = rt.client.Err()
	}

	if err == nil {
		err = errors.New("pi event stream closed mid-turn")
	}

	return turnFailure(wire.CauseTransport, err.Error())
}

func turnFailure(cause string, message string) *acp.RequestError {
	return wire.TurnFailed(vendor, wire.TurnFailure{Cause: cause, Message: boundNativeCause(message)})
}

// boundNativeCause is the single gate every native cause text passes through
// before it reaches a client.
func boundNativeCause(message string) string {
	if len(message) > nativeCauseMaxBytes {
		message = message[:nativeCauseMaxBytes]
	}

	return strings.TrimSpace(strings.ToValidUTF8(message, ""))
}

// cycleVerdict is how one cycle ended, in the terms the lifecycle stream and
// the prompt response need.
type cycleVerdict struct {
	outcome    lifecycle.Outcome
	stopReason string
	failure    error
}

// judgeCycle records how a natively settled cycle finished. The cancel guard
// runs before every failure mapping.
func judgeCycle(c *cycle, cancelled bool) cycleVerdict {
	switch {
	case cancelled:
		return cycleVerdict{outcome: lifecycle.OutcomeCancelled, stopReason: lifecycle.StopReasonCancelled}
	case c.failure != nil:
		return cycleVerdict{outcome: lifecycle.OutcomeFailed, failure: c.failure}
	case c.state.stopReason == stopReasonError:
		message := strings.TrimSpace(c.state.errorMessage)
		if message == "" {
			message = "pi reported a turn error"
		}

		return cycleVerdict{outcome: lifecycle.OutcomeFailed, failure: turnFailure(wire.CauseProvider, message)}
	}

	stop := acp.StopReasonEndTurn
	outcome := lifecycle.OutcomeSuccess

	switch c.state.stopReason {
	case stopReasonLength, stopReasonMaxTokens:
		stop = acp.StopReasonMaxTokens
		outcome = lifecycle.OutcomeLimit
	case stopReasonAborted:
		stop = acp.StopReasonCancelled
		outcome = lifecycle.OutcomeCancelled
	}

	return cycleVerdict{outcome: outcome, stopReason: string(stop)}
}

// settleTurn is the one settlement point every accepted prompt reaches:
// usage and session info, the durable mirror commit, the terminal idle, and
// only then the response or error.
func (s *session) settleTurn(ctx context.Context, rt *runtime, t *turn, params acp.PromptRequest) (acp.PromptResponse, error) {
	settleCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), sessionSettleTimeout)
	defer cancel()

	var verdict cycleVerdict

	switch {
	case t.cancelled:
		verdict = cycleVerdict{outcome: lifecycle.OutcomeCancelled, stopReason: lifecycle.StopReasonCancelled}
	case t.timedOut:
		verdict = cycleVerdict{outcome: lifecycle.OutcomeFailed, failure: turnFailure(wire.CauseTimeout, fmt.Sprintf("pi turn exceeded %s", s.agent.options.TurnTimeout))}
	case t.ended == turnTransportEnded:
		verdict = cycleVerdict{outcome: lifecycle.OutcomeFailed, failure: s.transportFailure(settleCtx, rt, nil)}
	default:
		verdict = judgeCycle(&t.cycle, false)
	}

	if t.ended == turnSettled {
		if !t.cancelled {
			stats := s.settledStats(settleCtx, rt)
			s.emitUsage(settleCtx, &t.state, stats)
			s.emitSessionInfo(settleCtx, params.Prompt)
		}

		if err := s.commitMirror(settleCtx); err != nil {
			s.stopRuntime(settleCtx, rt)
			s.lcFence()
			verdict.failure = s.mirrorFailure(&t.state, err)
			verdict.outcome = lifecycle.OutcomeFailed
		}
	}

	if err := s.lcIdle(settleCtx, &t.cycle, verdict); err != nil && verdict.failure == nil {
		verdict.failure = err
	}

	if t.ended == turnTransportEnded {
		s.lcFence()
	}

	if verdict.failure != nil {
		return acp.PromptResponse{}, verdict.failure
	}

	return acp.PromptResponse{
		StopReason:    acp.StopReason(verdict.stopReason),
		Usage:         t.state.usage,
		UserMessageId: params.MessageId,
	}, nil
}

// settledStats reads pi's post-turn statistics. A native session id that no
// longer matches the ACP session poisons it.
func (s *session) settledStats(ctx context.Context, rt *runtime) *pi.SessionStats {
	stats, err := rt.client.GetSessionStats(ctx)
	if err != nil {
		s.agent.log.DebugContext(ctx, "get pi session stats failed", slog.String("session_id", string(s.id)))

		return nil
	}

	if stats.SessionID != "" && stats.SessionID != string(s.id) {
		s.poisonSession(ctx, "native_session_identity_drift")

		return nil
	}

	return &stats
}

// mirrorFailure maps a failed mirror commit onto the turn-failure shape. A
// turn that delivered image bytes lost their durable replay representation.
func (s *session) mirrorFailure(state *cycleState, err error) error {
	s.agent.log.Error("session mirror commit failed", slog.String("session_id", string(s.id)), slog.String("reason", err.Error()))

	failure := wire.TurnFailure{Cause: wire.CauseTransport, Message: "session mirror commit failed"}
	if state.imagesEmitted {
		failure.Message = "image output is no longer available from the artifact store"
		failure.Stage = image.OutputStage
		failure.Reason = image.ReasonStorageFailed
	}

	return wire.TurnFailed(vendor, failure)
}
