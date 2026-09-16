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

	stopReasonLength    = "length"
	stopReasonMaxTokens = "max_tokens"
	stopReasonAborted   = "aborted"
	stopReasonError     = "error"

	// extensionFailureMessage is the whole of what a client is told about a
	// wrapper extension that threw: its path and thrown text are internal.
	extensionFailureMessage = "a pi extension failed"
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
		Limits:           s.agent.options.ImageLimits.core(),
		HandoffRoot:      s.agent.options.InputHandoffRoot,
		Blobs:            func(string) image.BlobDisposition { return image.BlobRefuse },
		TextBeforeImages: true,
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
			if wire.AudienceIsUserOnly(block.Text.Annotations) {
				continue
			}

			textParts = append(textParts, block.Text.Text)
		case block.Image != nil:
		case block.ResourceLink != nil:
			textParts = append(textParts, strings.TrimSpace(block.ResourceLink.Uri))
		case block.Resource != nil:
			if text := block.Resource.Resource.TextResourceContents; text != nil {
				textParts = append(textParts, strings.TrimSpace(text.Uri))
				contextParts = append(contextParts, wire.ContextResourceText(text.Uri, text.Text))
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
		return acp.PromptResponse{}, wire.ParamRefusal(paramErr)
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

	// The turn outlives this request: the pinned SDK cancels a session's
	// previous prompt context before it dispatches the next one, so a peer
	// prompt this session refuses must not end the live turn. Only the session
	// cancels a turn.
	turnCtx, cancelTurn := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelTurn()

	t := &turn{
		cycle:      cycle{Cycle: lifecycle.Cycle{Origin: lifecycle.CauseSubmission}, state: cycleState{tools: make(map[string]*toolState)}},
		submission: submission,
		cancel:     cancelTurn,
		settled:    make(chan struct{}),
		finished:   make(chan struct{}),
	}
	defer close(t.finished)

	// The turn is the session's before any of the work a session/cancel must be
	// able to interrupt: image decode and a relaunch of pi. It carries no
	// generation yet, so no pump attributes anything to it and nothing is
	// published for it until it dispatches.
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

	mapped, err := s.mapPrompt(turnCtx, params.Prompt)
	if turnCtx.Err() != nil {
		return s.endedBeforeDispatch(t, params)
	}

	if err != nil {
		return acp.PromptResponse{}, err
	}

	rt, err := s.ensureRuntime(turnCtx)
	if turnCtx.Err() != nil {
		return s.endedBeforeDispatch(t, params)
	}

	if err != nil {
		return acp.PromptResponse{}, err
	}

	if err := s.rejectImagesForModel(mapped); err != nil {
		return acp.PromptResponse{}, err
	}

	// pi is about to receive the turn, so its pump owns the turn from here.
	s.mu.Lock()
	t.rt = rt
	s.mu.Unlock()

	if err := rt.client.Prompt(turnCtx, mapped.message, mapped.images); err != nil {
		if !s.turnAccepted(t) {
			if s.turnCancelled(t) {
				return wire.CancelledResponse(params), nil
			}

			return acp.PromptResponse{}, s.dispatchFailure(context.WithoutCancel(ctx), rt, err)
		}
	}

	s.acceptTurn(turnCtx, t)

	select {
	case <-t.settled:
	case <-turnCtx.Done():
		select {
		case <-t.settled:
		case <-time.After(sessionAbortTimeout):
			// The child is still alive: end its generation before the turn
			// reports a lost transport, so nothing publishes on the incarnation
			// this fences.
			s.stopRuntime(context.WithoutCancel(ctx), rt)
			t.settle(turnTransportEnded)
		}
	}

	return s.settleTurn(ctx, rt, t, params)
}

// dispatchFailure classifies a prompt command pi never accepted: a native
// rejection carries its text as a provider failure, a dead child is a
// process exit, and everything else is transport.
func (s *session) dispatchFailure(ctx context.Context, rt *runtime, err error) error {
	var commandErr *pi.CommandError
	if errors.As(err, &commandErr) {
		return wire.TurnFailed(vendor, wire.TurnFailure{Cause: wire.CauseProvider, Message: commandErr.Message})
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

		if line := rt.proc.StderrLastLine(); line != "" {
			message += ": " + line
		}

		return wire.TurnFailed(vendor, wire.TurnFailure{Cause: wire.CauseProcessExit, Message: message})
	}

	if err == nil {
		err = rt.client.Err()
	}

	if err == nil {
		err = errors.New("pi event stream closed mid-turn")
	}

	return wire.TurnFailed(vendor, wire.TurnFailure{Cause: wire.CauseTransport, Message: err.Error()})
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
func (s *session) judgeCycle(c *cycle, cancelled bool) cycleVerdict {
	failure := s.cycleFailure(c)

	switch {
	case cancelled:
		return cycleVerdict{outcome: lifecycle.OutcomeCancelled, stopReason: lifecycle.StopReasonCancelled}
	case failure != nil:
		return cycleVerdict{outcome: lifecycle.OutcomeFailed, failure: failure}
	case c.state.stopReason == stopReasonError:
		message := strings.TrimSpace(c.state.errorMessage)
		if message == "" {
			message = "pi reported a turn error"
		}

		return cycleVerdict{outcome: lifecycle.OutcomeFailed, failure: wire.TurnFailed(vendor, wire.TurnFailure{Cause: wire.CauseProvider, Message: message})}
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

	s.mu.Lock()
	cancelled, timedOut := t.cancelled, t.timedOut
	s.mu.Unlock()

	var verdict cycleVerdict

	switch {
	case cancelled:
		verdict = cycleVerdict{outcome: lifecycle.OutcomeCancelled, stopReason: lifecycle.StopReasonCancelled}
	case timedOut:
		verdict = cycleVerdict{outcome: lifecycle.OutcomeFailed, failure: s.turnTimeout()}
	case t.ended == turnTransportEnded:
		verdict = cycleVerdict{outcome: lifecycle.OutcomeFailed, failure: s.transportFailure(settleCtx, rt, nil)}
	default:
		verdict = s.judgeCycle(&t.cycle, false)
	}

	if t.ended == turnSettled {
		if !cancelled {
			stats := s.settledStats(settleCtx, rt)
			s.emitUsage(settleCtx, &t.state, stats)
			s.emitSessionInfo(settleCtx, params.Prompt)
		}

		if err := s.commitMirror(settleCtx); err != nil {
			s.stopRuntime(settleCtx, rt)
			s.lc.Fence()
			verdict.failure = s.mirrorFailure(&t.state, err)
			verdict.outcome = lifecycle.OutcomeFailed
		}
	}

	if err := s.lc.Idle(settleCtx, t.Cycle, verdict.stopReason, verdict.outcome); err != nil && verdict.failure == nil {
		verdict.failure = err
	}

	if s.claimFence(t, rt) {
		s.lc.Fence()
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

// endedBeforeDispatch answers a prompt the session ended before pi received
// it: no native turn exists, no submission was published, and nothing has to be
// terminalized. The deadline fails the prompt; every other end is a cancel.
func (s *session) endedBeforeDispatch(t *turn, params acp.PromptRequest) (acp.PromptResponse, error) {
	s.mu.Lock()
	timedOut := t.timedOut
	s.mu.Unlock()

	if timedOut {
		return acp.PromptResponse{}, s.turnTimeout()
	}

	return wire.CancelledResponse(params), nil
}

// turnTimeout is the failure a turn that outran its deadline carries.
func (s *session) turnTimeout() error {
	return wire.TurnFailed(vendor, wire.TurnFailure{Cause: wire.CauseTimeout, Message: fmt.Sprintf("pi turn exceeded %s", s.agent.options.TurnTimeout)})
}

// turnCancelled reports whether the session has cancelled this turn.
func (s *session) turnCancelled(t *turn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return t.cancelled
}

// claimFence records that the turn published its terminal lifecycle event and
// reports whether this caller owes the incarnation fence. An incarnation ends
// with its native generation, so whichever of the turn and the pump acts last
// fences it exactly once.
func (s *session) claimFence(t *turn, rt *runtime) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	t.lcSettled = true

	return s.runtime != rt
}

// settledStats reads pi's post-turn statistics. A native session id that no
// longer matches the ACP session poisons it.
func (s *session) settledStats(ctx context.Context, rt *runtime) *pi.SessionStats {
	stats, err := rt.client.GetSessionStats(ctx)
	if err != nil {
		s.agent.log.DebugContext(ctx, "get pi session stats failed", slog.String("session_id", string(s.id)))

		return nil
	}

	if stats.SessionID != "" && stats.SessionID != s.nativeID {
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
