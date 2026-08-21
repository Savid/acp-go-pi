package piacp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/acp-go-sdk"

	"github.com/savid/acp-go-pi/internal/lifecycle"
	"github.com/savid/acp-go-pi/internal/pi"
)

// sessionSettleTimeout bounds the settlement of one accepted turn. Settlement
// runs on a context detached from the request, so a client that walked away
// still gets a boundary that is either fully recorded or explicitly failed,
// and close never waits on it forever.
var sessionSettleTimeout = 60 * time.Second

var errPromptSettlement = errors.New("prompt settlement failed")

// turnSettlement is the signal close and delete wait on. It is published only
// after the whole settlement order completed — the durable commit, the terminal
// lifecycle event, and any quiescence fact — never at the native settle marker,
// because everything that makes the boundary durable happens after it.
type turnSettlement struct {
	done        chan struct{}
	waiting     chan struct{}
	waitingOnce sync.Once
	finishOnce  sync.Once
	err         error
}

// openSettlement arms the latch for a turn the native dispatcher has accepted.
// A prompt that failed before acceptance settles nothing and arms nothing.
func (s *agentSession) openSettlement() {
	s.mu.Lock()
	s.settlement = &turnSettlement{
		done:    make(chan struct{}),
		waiting: make(chan struct{}),
	}
	s.mu.Unlock()
}

func (s *agentSession) completeSettlement(err error) {
	s.mu.Lock()

	settlement := s.settlement
	if settlement == nil {
		s.mu.Unlock()

		return
	}

	settlement.finishOnce.Do(func() {
		settlement.err = err
		close(settlement.done)
	})
	s.mu.Unlock()
}

// awaitSettlement blocks until the in-flight settlement has finished the whole
// order and reports its result. Teardown removes the roots a settlement is
// still writing through, so it waits rather than racing it; the settlement's
// own bounded context is what makes the wait terminate.
func (s *agentSession) awaitSettlement() error {
	return s.awaitSettlementContext(context.Background())
}

func (s *agentSession) awaitSettlementContext(ctx context.Context) error {
	s.mu.Lock()
	settlement := s.settlement
	s.mu.Unlock()

	if settlement == nil {
		return nil
	}

	settlement.waitingOnce.Do(func() { close(settlement.waiting) })

	select {
	case <-settlement.done:
	case <-ctx.Done():
		return fmt.Errorf("%w: join prompt settlement: %v", pi.ErrProcessContainmentIncomplete, ctx.Err())
	}

	return settlement.err
}

// turnVerdict is how one accepted cycle ended, in the terms the lifecycle
// stream, the durable boundary record, and the v1 response all need.
type turnVerdict struct {
	outcome    lifecycle.Outcome
	stopReason string
	response   acp.PromptResponse
	// failure is the v1 error this cycle answers with, or nil for a cycle that
	// answers with a response.
	failure error
	// nativeSafe reports that pi made this cycle's transcript durable, so the
	// mirror may adopt it. The store is a raw mirror of pi's own file: a cycle
	// pi never wrote cannot be mirrored, and inventing a row for it would be
	// fabricating a conversation entry.
	nativeSafe bool
	detail     string
}

// settlePrompt is the one settlement point every accepted exit reaches. The
// order it runs is the contract's, and each step is a precondition of the next:
// the containment boundary any incarnation-ending path crossed, the durable
// foreground-prefix commit, the terminal idle, and only then the prompt's own
// response or error.
func (s *agentSession) settlePrompt(
	turnCtx context.Context,
	params acp.PromptRequest,
	state *promptTurnState,
	outcome promptOutcome,
	timedOut *atomic.Bool,
) (resp acp.PromptResponse, err error) {
	settleCtx, cancelSettle := context.WithTimeout(context.WithoutCancel(turnCtx), sessionSettleTimeout)
	defer cancelSettle()

	s.mu.Lock()
	outbox := s.outbox
	s.mu.Unlock()

	defer func() {
		if recover() != nil {
			containmentErr := s.containGenerationSync(
				settleCtx,
				outbox,
				"prompt settlement failed",
			)
			if pi.ProcessContainmentComplete(containmentErr) {
				s.fenceLifecycleStream()
			}

			resp = acp.PromptResponse{}
			err = errors.Join(errPromptSettlement, containmentErr)
		}

		s.completeSettlement(err)
	}()

	if boundaryErr := s.joinTurnBoundary(settleCtx, outcome); boundaryErr != nil {
		// Durability outranks the terminal event: a containment boundary that
		// did not complete emits no terminal idle, and the incarnation ends
		// unsettled so the next snapshot states the truth.
		if pi.ProcessContainmentComplete(boundaryErr) {
			s.fenceLifecycleStream()
		}

		if timedOut.Load() {
			return acp.PromptResponse{}, errors.Join(
				turnFailureError(failureCauseTimeout, fmt.Sprintf("pi turn exceeded %s", s.agent.turnTimeout())),
				boundaryErr,
			)
		}

		return acp.PromptResponse{}, boundaryErr
	}

	if outcome.settled && !s.wasTurnCancelled() {
		if observeErr := s.observeSettledSession(settleCtx, params, state); observeErr != nil {
			s.fenceLifecycleStream()

			return acp.PromptResponse{}, observeErr
		}
	}

	verdict := s.judgeTurn(settleCtx, params, state, outcome, timedOut)

	if commitErr := s.commitTurnBoundary(settleCtx, verdict); commitErr != nil {
		s.fenceLifecycleStream()

		return acp.PromptResponse{}, errors.Join(verdict.failure, commitErr)
	}

	idleErr := s.lifecycleSettleTurn(settleCtx, verdict.stopReason, verdict.outcome)

	if s.turnBoundaryFenced() {
		s.fenceLifecycleStream()
	}

	if joined := errors.Join(verdict.failure, idleErr); joined != nil {
		return acp.PromptResponse{}, joined
	}

	return verdict.response, nil
}

// joinTurnBoundary completes the boundary this exit crossed. A natively settled
// turn has no native work left to contain and only linearizes against a
// coincident cancel; every other exit ends the incarnation, so its containment
// boundary completes before anything durable is written.
func (s *agentSession) joinTurnBoundary(ctx context.Context, outcome promptOutcome) error {
	if outcome.settled {
		return s.claimTurnSettlement()
	}

	if err := s.fenceTurnAfterFailure(ctx); err != nil {
		return err
	}

	// A fence another path started owns the boundary, so settlement reports the
	// result that boundary reached rather than starting a second one.
	return s.awaitTurnFence()
}

// turnBoundaryFenced reports whether this turn's containment fence ran, which
// is what ends the incarnation the stream speaks for.
func (s *agentSession) turnBoundaryFenced() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.turnFenceStarted
}

// observeSettledSession runs the post-terminal native reads. They are a read of
// a process that is still alive after agent_settled, never a durability or
// quiescence proof: nothing here decides whether the boundary was reached.
func (s *agentSession) observeSettledSession(
	ctx context.Context,
	params acp.PromptRequest,
	state *promptTurnState,
) error {
	stats := s.settledSessionStats(ctx)

	// A native session id change mid-session breaks the wrapper's identity
	// invariant (for example an operator-seeded extension switching
	// sessions); the session is poisoned rather than mirrored under the
	// wrong key.
	if stats != nil && stats.SessionID != "" && stats.SessionID != string(s.id) {
		return s.poison(ctx, fmt.Sprintf("native session id drift: expected %s, got %s", s.id, stats.SessionID))
	}

	s.emitTurnUsageUpdate(ctx, state, stats)

	return s.emitLiveSessionInfoUpdate(ctx, params.Prompt)
}

// judgeTurn records how the cycle ended. The cancel guard runs before every
// failure mapping, so a native error observed while the turn is cancelled is a
// cancellation rather than a failure, and a timeout is a failure rather than a
// cancellation.
func (s *agentSession) judgeTurn(
	ctx context.Context,
	params acp.PromptRequest,
	state *promptTurnState,
	outcome promptOutcome,
	timedOut *atomic.Bool,
) turnVerdict {
	switch {
	case s.wasTurnCancelled():
		return s.cancelledVerdict(params)
	case timedOut.Load():
		return turnVerdict{
			outcome: lifecycle.OutcomeFailed,
			failure: turnFailureError(failureCauseTimeout, fmt.Sprintf("pi turn exceeded %s", s.agent.turnTimeout())),
			detail:  "the turn deadline expired before pi settled",
		}
	case outcome.failure != nil:
		return turnVerdict{
			outcome: lifecycle.OutcomeFailed,
			failure: outcome.failure,
			detail:  "an update this cycle streamed could not be delivered",
		}
	case outcome.transportEnded:
		return turnVerdict{
			outcome: lifecycle.OutcomeFailed,
			failure: s.transportFailure(ctx),
			detail:  "the native event stream ended before pi settled",
		}
	case outcome.contextEnded:
		return turnVerdict{
			outcome:    lifecycle.OutcomeCancelled,
			stopReason: lifecycle.StopReasonCancelled,
			response:   cancelledResponse(params.MessageId),
			detail:     "the request context ended before pi settled",
		}
	}

	return s.settledVerdict(params, state)
}

func (s *agentSession) cancelledVerdict(params acp.PromptRequest) turnVerdict {
	s.mu.Lock()
	adopt := s.turnCommitOnCancel && s.turnNativeSettled
	s.mu.Unlock()

	verdict := turnVerdict{
		outcome:    lifecycle.OutcomeCancelled,
		stopReason: lifecycle.StopReasonCancelled,
		response:   cancelledResponse(params.MessageId),
		nativeSafe: adopt,
	}
	if !adopt {
		verdict.detail = "the cancelled cycle's native rows were not adopted into the mirror"
	}

	return verdict
}

// settledVerdict maps a natively settled cycle. pi crossed agent_settled, so
// its session file is a complete durable generation whatever the cycle
// produced, including an aborted or provider-failed one.
func (s *agentSession) settledVerdict(params acp.PromptRequest, state *promptTurnState) turnVerdict {
	if providerErr := providerTurnFailure(state); providerErr != nil {
		return turnVerdict{
			outcome:    lifecycle.OutcomeFailed,
			failure:    providerErr,
			nativeSafe: true,
			detail:     "pi reported a provider error for this cycle",
		}
	}

	stop := acpStopReason(s, state)

	return turnVerdict{
		outcome:    turnOutcomeForStopReason(stop),
		stopReason: string(stop),
		nativeSafe: true,
		response: acp.PromptResponse{
			Meta:          nativeMessageResponseMeta(state.nativeMessageID),
			StopReason:    stop,
			Usage:         state.usage,
			UserMessageId: params.MessageId,
		},
	}
}

// turnOutcomeForStopReason records how a settled cycle finished. The outcome is
// the cycle's recorded result and the stop reason is the standard ACP v1 one;
// neither is derived from the other's absence.
func turnOutcomeForStopReason(stop acp.StopReason) lifecycle.Outcome {
	switch stop {
	case acp.StopReasonMaxTokens, acp.StopReasonMaxTurnRequests:
		return lifecycle.OutcomeLimit
	case acp.StopReasonCancelled:
		return lifecycle.OutcomeCancelled
	case acp.StopReasonRefusal:
		return lifecycle.OutcomeRefused
	default:
		return lifecycle.OutcomeSuccess
	}
}

// transportFailure recovers the real cause behind a mid-turn stream end: the
// child's exit status and stderr tail where it died, the transport error
// otherwise, and never a bare EOF.
func (s *agentSession) transportFailure(ctx context.Context) error {
	// A malformed or unterminated JSONL record is the transport's terminal
	// fact even when containment makes the child exit immediately afterwards.
	// Classifying the induced exit first would erase the structural failure and
	// reintroduce a scheduler-dependent T4 result.
	if client := s.currentClient(); client != nil && errors.Is(client.Err(), pi.ErrJSONLStructural) {
		return turnFailureError(failureCauseTransport, pi.ErrJSONLStructural.Error())
	}

	if exitMessage, exited := s.processExitCause(ctx, "pi process exited"); exited {
		s.agent.observe.RecordPiProcessExit(context.Background(), "unexpected", nil)

		return turnFailureError(failureCauseProcessExit, exitMessage)
	}

	message := "pi event stream closed mid-turn"

	if client := s.currentClient(); client != nil {
		if err := client.Err(); err != nil {
			message = err.Error()

			s.agent.log.ErrorContext(ctx, "pi turn transport failed",
				slog.String(acpFieldSessionID, string(s.id)),
				slog.String(failureFieldCause, failureCauseTransport),
			)
		}
	}

	return turnFailureError(failureCauseTransport, message)
}

// commitTurnBoundary makes the cycle's foreground prefix durable. The native
// mirror is adopted only where pi wrote it; the boundary record is written
// either way, so a cycle whose transcript pi never wrote still leaves the store
// able to say truthfully how the last incarnation ended.
func (s *agentSession) commitTurnBoundary(ctx context.Context, verdict turnVerdict) error {
	if verdict.nativeSafe {
		if err := s.commitMirror(ctx); err != nil {
			return s.imageAwareMirrorFailure(err)
		}
	}

	return s.commitLifecycleBoundary(ctx, s.boundaryRecord(verdict))
}

func (s *agentSession) boundaryRecord(verdict turnVerdict) lifecycleBoundaryRecord {
	s.mu.Lock()
	rows := s.mirroredRows
	s.mu.Unlock()

	streamID, turnID, cycleID := s.lifecycleIdentity()

	record := lifecycleBoundaryRecord{
		StreamID:    streamID,
		TurnID:      turnID,
		CycleID:     cycleID,
		Outcome:     string(verdict.outcome),
		StopReason:  verdict.stopReason,
		NativeRows:  rows,
		NativeState: nativeStateRetained,
		Detail:      verdict.detail,
	}
	if verdict.nativeSafe {
		record.NativeState = nativeStateCommitted
	}

	return record
}

// lifecycleIdentity reports the incarnation, turn, and cycle a boundary record
// names. They are empty where the host negotiated no lifecycle stream, which is
// a record of the same boundary without the identities that would be invented.
func (s *agentSession) lifecycleIdentity() (string, string, string) {
	s.lcMu.Lock()
	defer s.lcMu.Unlock()

	if s.lc.stream == nil {
		return "", "", ""
	}

	return s.lc.stream.ID(), s.lc.turnID, s.lc.cycleID
}

func cancelledResponse(messageID *string) acp.PromptResponse {
	return acp.PromptResponse{
		StopReason:    acp.StopReasonCancelled,
		UserMessageId: messageID,
	}
}
