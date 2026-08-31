package piacp

import (
	"context"
	"log/slog"

	"github.com/coder/acp-go-sdk"

	"github.com/savid/acp-go-pi/internal/lifecycle"
	"github.com/savid/acp-go-pi/internal/pi"
)

// agentCycleSettleDetail is what the boundary record says about a cycle no
// prompt asked for. It names the origin, because a later reader restoring this
// session has no submission to attribute those native rows to.
const agentCycleSettleDetail = "an agent-origin cycle settled between prompts"

// openAgentCycle opens the lifecycle identity for the agent-origin cycle the
// router already owns. The router took the foreground under the same lock that
// classified agent_start, so this runs with the cycle's admission already
// settled; all that remains is to state it.
//
// The cycle is agent-origin: it states no acceptance, no submission, no run,
// and it never borrows the route nonce of a prompt that is not running.
func (s *agentSession) openAgentCycle(ctx context.Context, outbox *sessionOutbox, cycle *agentCycle) {
	if err := s.lifecycleOpenAgentCycle(ctx, cycle); err != nil {
		s.agent.log.ErrorContext(ctx, "open agent-origin cycle on the lifecycle stream failed",
			slog.String(acpFieldSessionID, string(s.id)),
		)
		s.containGeneration(ctx, outbox, "the agent-origin cycle could not be opened on the lifecycle stream")

		return
	}

	s.resetTurnTools()
}

// handleAgentOriginEvent routes one record of an open agent-origin cycle.
//
// Native agent_settled is the only thing that ends this cycle normally. An
// extension failure marks the cycle failed and keeps draining to that marker:
// minting a terminal state here would name an end the adapter never observed,
// and the records still arriving would then belong to a cycle the host has
// already been told is over. A record this session could not project is
// different in kind — the host's view of the cycle is already incomplete — so
// the generation is contained rather than settled with a verdict nobody saw the
// evidence for.
func (s *agentSession) handleAgentOriginEvent(
	ctx context.Context,
	outbox *sessionOutbox,
	cycle *agentCycle,
	event pi.Event,
) {
	switch event.(type) {
	case pi.AgentSettledEvent:
		s.settleAgentCycle(ctx, outbox, cycle)

		return
	case pi.ExtensionErrorEvent:
		// The wrapper-owned extensions are the permission bridge and the MCP
		// client, so a cycle whose extension surface threw may no longer be
		// running under the authorization the host believes it granted. The
		// cycle is failed here and stated as failed at its native end; the
		// extension path and the thrown text are adapter-internal.
		cycle.failure = true

		return
	}

	if _, err := s.handleTurnEvent(ctx, event, cycle.state); err != nil {
		s.agent.log.ErrorContext(ctx, "project agent-origin cycle output failed",
			slog.String(acpFieldSessionID, string(s.id)),
		)
		s.containGeneration(ctx, outbox, "an agent-origin cycle's update could not be delivered")
	}
}

// settleAgentCycle moves the cycle into settlement and runs its boundary off
// the pump. The pump must keep draining the transport for the life of the
// process, so the durable commit and the terminal event run beside it while the
// router retains every later record in arrival order.
func (s *agentSession) settleAgentCycle(ctx context.Context, outbox *sessionOutbox, cycle *agentCycle) {
	releaseProducer, admitted := outbox.producers.acquire(1)
	if !admitted {
		return
	}

	if !outbox.beginCycleSettlement(cycle) {
		releaseProducer()

		return
	}

	settleCtx, cancelSettle := context.WithTimeout(context.WithoutCancel(ctx), sessionSettleTimeout)

	go func() {
		defer cancelSettle()
		defer releaseProducer()
		defer func() {
			handleAgentGoroutinePanic(
				settleCtx,
				agentLogger(s.agent),
				"agent cycle settlement",
				func(any) {
					s.containGeneration(settleCtx, outbox, "the agent-origin cycle settlement panicked")
				},
				recover(),
			)
		}()

		s.completeAgentCycle(settleCtx, outbox, cycle)
	}()
}

// completeAgentCycle runs the agent-origin settlement order. It is the
// foreground order with the prompt removed: the durable native mirror and the
// boundary record are committed before the terminal idle, because a terminal
// state no store stands behind is a claim the next incarnation cannot honour.
//
// Every step is a precondition of the next, and a step that fails contains the
// generation instead of continuing. A cycle whose transcript, boundary, or
// terminal event did not land states no success afterwards and does not hand
// the foreground back: the session has just proved it cannot describe what
// happened, and admitting the next turn would build on that gap.
func (s *agentSession) completeAgentCycle(ctx context.Context, outbox *sessionOutbox, cycle *agentCycle) {
	verdict := agentCycleVerdict(cycle)

	// Usage is the accumulation this cycle's own messages reported. The pump is
	// the sole reader of the native transport, so a synchronous stats request
	// from the settlement beside it would ask a process whose answer only the
	// pump can deliver.
	s.emitTurnUsageUpdate(ctx, cycle.state, nil)

	if err := s.retireManagedGeneration(ctx, outbox); err != nil {
		s.containAgentCycle(ctx, outbox, "the agent-origin cycle's native generation could not be retired")

		return
	}

	if err := s.commitMirror(ctx); err != nil {
		s.containAgentCycle(ctx, outbox, "the agent-origin cycle's native rows could not be mirrored")

		return
	}

	if err := s.commitAgentCycleBoundary(ctx, cycle, verdict); err != nil {
		s.containAgentCycle(ctx, outbox, "the agent-origin cycle's boundary could not be committed")

		return
	}

	if err := s.lifecycleSettleAgentCycle(ctx, cycle, verdict); err != nil {
		s.containAgentCycle(ctx, outbox, "the agent-origin cycle's terminal idle could not be delivered")

		return
	}

	outbox.finishCycle()
}

// containAgentCycle ends the generation a settlement step failed on. The fixed
// cause names the failed stage without carrying native or user content.
func (s *agentSession) containAgentCycle(ctx context.Context, outbox *sessionOutbox, cause string) {
	s.agent.log.ErrorContext(ctx, "agent-origin cycle settlement failed",
		slog.String(acpFieldSessionID, string(s.id)),
		slog.String("cause", cause),
	)

	s.containGeneration(ctx, outbox, cause)
}

// agentCycleVerdict records how the cycle ended in the terms its boundary and
// its terminal event need. A failed cycle states no stop reason, because no ACP
// v1 stop reason names a failure.
func agentCycleVerdict(cycle *agentCycle) turnVerdict {
	// The mirror commits before this verdict is used, so the cycle's native rows
	// are durable whatever it produced.
	if cycle.failure || cycle.state.stopReason == stopReasonError {
		return turnVerdict{
			outcome:    lifecycle.OutcomeFailed,
			nativeSafe: true,
			detail:     agentCycleSettleDetail,
		}
	}

	stop := acp.StopReasonEndTurn

	switch cycle.state.stopReason {
	case stopReasonLength, stopReasonMaxTokens:
		stop = acp.StopReasonMaxTokens
	case stopReasonAborted:
		stop = acp.StopReasonCancelled
	}

	return turnVerdict{
		outcome:    turnOutcomeForStopReason(stop),
		stopReason: string(stop),
		nativeSafe: true,
		detail:     agentCycleSettleDetail,
	}
}

// commitAgentCycleBoundary makes the cycle's prefix durable under the same
// order a prompt's boundary uses, and names the cycle it actually opened rather
// than whatever identity the stream happens to hold now. The mirror already
// committed, so this boundary always reports the native rows as committed.
func (s *agentSession) commitAgentCycleBoundary(ctx context.Context, cycle *agentCycle, verdict turnVerdict) error {
	s.mu.Lock()
	rows := s.mirroredRows
	s.mu.Unlock()

	return s.commitLifecycleBoundary(ctx, lifecycleBoundaryRecord{
		StreamID:    s.lifecycleStreamID(),
		TurnID:      cycle.turnID,
		CycleID:     cycle.cycleID,
		Outcome:     string(verdict.outcome),
		StopReason:  verdict.stopReason,
		NativeRows:  rows,
		NativeState: nativeStateCommitted,
		Detail:      verdict.detail,
	})
}

// agentWorkPending reports that this session owns work no client turn can
// interleave with: an agent-origin cycle, its settlement, records retained
// behind one, an active restore, or a native queue pi has not drained.
func (s *agentSession) agentWorkPending() bool {
	s.mu.Lock()
	outbox := s.outbox
	s.mu.Unlock()

	return outbox.agentBusy()
}

// beginRestore holds the session's single foreground for an active
// session/load or session/resume. It excludes an in-flight prompt and every new
// agent-origin cycle for the whole replay, in one transition, so the transcript
// the restore answers with cannot be overtaken by work that starts while it is
// being read.
func (s *agentSession) beginRestore(ctx context.Context) (func(), error) {
	if err := s.admissionFenceError(ctx); err != nil {
		return nil, err
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.mu.Lock()

	if s.closing {
		s.mu.Unlock()

		return nil, unknownSessionError()
	}

	if s.poisonCause != "" {
		s.mu.Unlock()

		return nil, s.admissionFenceError(ctx)
	}

	turn := s.turnQueueLocked()
	outbox := s.outbox

	select {
	case turn <- struct{}{}:
	default:
		s.mu.Unlock()

		return nil, backpressureError(limitSessionRestore)
	}

	if !outbox.beginRestore() {
		s.mu.Unlock()
		<-turn

		return nil, backpressureError(limitSessionRestore)
	}
	s.mu.Unlock()

	return func() {
		outbox.finishRestore()
		<-turn
	}, nil
}
