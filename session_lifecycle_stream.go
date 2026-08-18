package piacp

import (
	"context"
	"errors"

	"github.com/coder/acp-go-sdk"

	"github.com/savid/acp-go-pi/internal/lifecycle"
)

// errLifecycleStreamFenced reports that the incarnation that owned the stream
// ended. Its events are gone with it and are never reconstructed.
var errLifecycleStreamFenced = errors.New("lifecycle stream is fenced")

// lifecycleState is the session's ordered lifecycle emitter for exactly one pi
// process generation. The stream survives ordinary prompts, because the process
// does; it ends when the generation ends, and the session's close fences the
// session itself.
//
// Every method serializes on mu through delivery, so a sequence claimed before
// a send is also delivered in that order.
type lifecycleState struct {
	negotiated lifecycle.Negotiated
	stream     *lifecycle.Stream
	generation uint64
	cycleID    string
	turnID     string
	// blockers are the announced actions still blocking the current foreground
	// cycle. The cycle is released when the last one resolves, never the first.
	blockers map[string]struct{}
	fenced   bool
	closed   bool
	// vacancyProven records what the last completed boundary proved, so an
	// opening snapshot states the boundary it has rather than the class the
	// configuration advertises.
	vacancyProven bool
}

// openLifecycleStream mints a fresh incarnation for the current process
// generation and states the whole truth it can: no turn, no activity, no
// pending action, and the quiescence fact the last completed boundary actually
// proved.
func (s *agentSession) openLifecycleStream(ctx context.Context, generation uint64) error {
	negotiated := s.agent.lifecycleNegotiated()
	if !negotiated.Present() {
		return nil
	}

	streamID, err := newLifecycleID("stream")
	if err != nil {
		return err
	}

	cycleID, err := newLifecycleID("cycle")
	if err != nil {
		return err
	}

	s.lcMu.Lock()
	defer s.lcMu.Unlock()

	if s.lc.closed {
		return nil
	}

	s.lc.negotiated = negotiated
	s.lc.stream = lifecycle.NewStream(streamID, negotiated)
	s.lc.generation = generation
	s.lc.cycleID = cycleID
	s.lc.turnID = ""
	s.lc.blockers = make(map[string]struct{})
	s.lc.fenced = false

	return s.emitLifecycleLocked(ctx, lifecycle.SnapshotEvent(cycleID, s.openingQuiescenceLocked()))
}

// openingQuiescenceLocked states the boundary the session actually holds. A
// configuration that advertises a proof class still opens negative until one of
// its boundaries has completed, because the advertisement names what the
// configuration can prove and never what it has proved.
func (s *agentSession) openingQuiescenceLocked() lifecycle.QuiescenceFact {
	if !s.lc.negotiated.AuthoritativeQuiescence || !s.lc.vacancyProven {
		return lifecycle.QuiescenceFact{}
	}

	return lifecycle.QuiescenceFact{Quiescent: true, Source: s.lc.negotiated.QuiescenceSource}
}

// fenceLifecycleStream ends the incarnation. A fenced stream is terminal: its
// undelivered events are lost with the generation that owned them and are never
// reconstructed on a later one.
func (s *agentSession) fenceLifecycleStream() {
	s.lcMu.Lock()
	defer s.lcMu.Unlock()

	s.lc.fenced = true
	s.lc.turnID = ""
	s.lc.blockers = nil
}

// closeLifecycleSession fences the session itself. Once close containment has
// completed, the session admits no further incarnation of itself.
func (s *agentSession) closeLifecycleSession() {
	s.lcMu.Lock()
	defer s.lcMu.Unlock()

	s.lc.closed = true
	s.lc.fenced = true
	s.lc.turnID = ""
	s.lc.blockers = nil
}

// lifecycleAcceptTurn records that the native dispatcher took durable ownership
// of the prompt frame and opens the foreground cycle it caused.
func (s *agentSession) lifecycleAcceptTurn(ctx context.Context, submission lifecycle.Submission) error {
	s.lcMu.Lock()
	defer s.lcMu.Unlock()

	if s.lc.stream == nil {
		return nil
	}

	turnID, err := newLifecycleID("turn")
	if err != nil {
		return err
	}

	cycleID, err := newLifecycleID("cycle")
	if err != nil {
		return err
	}

	s.lc.turnID = turnID
	s.lc.cycleID = cycleID

	if err := s.emitLifecycleLocked(ctx, lifecycle.AcceptedEvent(submission, turnID)); err != nil {
		return err
	}

	return s.emitLifecycleLocked(ctx, lifecycle.RunningEvent(cycleID, turnID))
}

// lifecycleSettleTurn ends the accepted cycle with the outcome the turn
// actually reached. Every blocker the cycle still holds terminalizes first: the
// resolution is the reason the foreground may move.
func (s *agentSession) lifecycleSettleTurn(ctx context.Context, stopReason string, outcome lifecycle.Outcome) error {
	s.lcMu.Lock()
	defer s.lcMu.Unlock()

	if s.lc.stream == nil || s.lc.turnID == "" {
		return nil
	}

	if err := s.terminalizeBlockersLocked(ctx); err != nil {
		return err
	}

	turnID, cycleID := s.lc.turnID, s.lc.cycleID
	s.lc.turnID = ""

	return s.emitLifecycleLocked(ctx, lifecycle.IdleEvent(cycleID, turnID, stopReason, outcome))
}

// pendingAction is the lifecycle identity of one request awaiting an answer. It
// is minted before the request is sent, so the request itself carries the
// identity the ordered action will announce.
type pendingAction struct {
	actionID string
	streamID string
	owner    lifecycle.Owner
}

// prepareLifecycleAction mints the identity for one permission or elicitation
// without publishing anything. Nothing is announced yet: the request that
// answers the action goes on the wire first.
func (s *agentSession) prepareLifecycleAction() (pendingAction, bool, error) {
	s.lcMu.Lock()
	defer s.lcMu.Unlock()

	if s.lc.stream == nil || s.lc.fenced || s.lc.turnID == "" {
		return pendingAction{}, false, nil
	}

	actionID, err := newLifecycleID("action")
	if err != nil {
		return pendingAction{}, false, err
	}

	return pendingAction{
		actionID: actionID,
		streamID: s.lc.stream.ID(),
		owner:    lifecycle.Owner{Type: lifecycle.OwnerTurn, ID: s.lc.turnID},
	}, true, nil
}

// announceLifecycleAction publishes the ordered action for a request already on
// the wire, plus the transition its blocking causes. A blocking action never
// moves the foreground by itself, so the accompanying transition is emitted with
// it and only for the first blocker of the cycle.
func (s *agentSession) announceLifecycleAction(
	ctx context.Context,
	action pendingAction,
	kind lifecycle.ActionKind,
) error {
	s.lcMu.Lock()
	defer s.lcMu.Unlock()

	if s.lc.stream == nil || s.lc.turnID == "" {
		return nil
	}

	event := lifecycle.ActionEvent(action.actionID, kind, lifecycle.ActionPending, action.owner, true)
	if err := s.emitLifecycleLocked(ctx, event); err != nil {
		return err
	}

	first := len(s.lc.blockers) == 0
	s.lc.blockers[action.actionID] = struct{}{}

	if !first {
		return nil
	}

	return s.emitLifecycleLocked(ctx, lifecycle.RequiresActionEvent(s.lc.cycleID, s.lc.turnID))
}

// lifecycleResolveAction resolves an announced action exactly once and releases
// the cycle when the last action blocking it resolves.
func (s *agentSession) lifecycleResolveAction(ctx context.Context, actionID string, state lifecycle.ActionState) error {
	s.lcMu.Lock()
	defer s.lcMu.Unlock()

	if s.lc.stream == nil {
		return nil
	}

	if _, held := s.lc.blockers[actionID]; !held {
		return nil
	}

	delete(s.lc.blockers, actionID)

	if err := s.emitLifecycleLocked(ctx, lifecycle.ActionResolvedEvent(actionID, state)); err != nil {
		return err
	}

	if len(s.lc.blockers) > 0 || s.lc.turnID == "" {
		return nil
	}

	return s.emitLifecycleLocked(ctx, lifecycle.RunningEvent(s.lc.cycleID, s.lc.turnID))
}

// terminalizeBlockersLocked cancels every action still blocking the cycle. A
// cancelled cycle terminalizes its blockers before it reports its terminal
// idle, because terminal is immutable and a later real event on one of them
// would be a post-terminal mutation.
func (s *agentSession) terminalizeBlockersLocked(ctx context.Context) error {
	for actionID := range s.lc.blockers {
		delete(s.lc.blockers, actionID)

		if err := s.emitLifecycleLocked(ctx, lifecycle.ActionResolvedEvent(actionID, lifecycle.ActionCancelled)); err != nil {
			return err
		}
	}

	return nil
}

// lifecycleTerminalizeOwned terminalizes every entity the session still owns at
// its close boundary, so the host's projection agrees with the store the close
// is about to commit.
func (s *agentSession) lifecycleTerminalizeOwned(ctx context.Context) error {
	s.lcMu.Lock()
	defer s.lcMu.Unlock()

	if s.lc.stream == nil || s.lc.fenced {
		return nil
	}

	if err := s.terminalizeBlockersLocked(ctx); err != nil {
		return err
	}

	if s.lc.turnID == "" {
		return nil
	}

	turnID, cycleID := s.lc.turnID, s.lc.cycleID
	s.lc.turnID = ""

	return s.emitLifecycleLocked(ctx, lifecycle.IdleEvent(cycleID, turnID, lifecycle.StopReasonCancelled, lifecycle.OutcomeCancelled))
}

// lifecycleCertifyBoundary states the quiescence fact a completed close
// boundary produced. It is emitted only where the configuration's proof class
// actually completed, and only after the resumable snapshot is durable.
func (s *agentSession) lifecycleCertifyBoundary(ctx context.Context, barrier string) error {
	s.lcMu.Lock()
	defer s.lcMu.Unlock()

	s.lc.vacancyProven = true

	if s.lc.stream == nil || s.lc.fenced || !s.lc.negotiated.AuthoritativeQuiescence {
		return nil
	}

	return s.emitLifecycleLocked(ctx, lifecycle.QuiescenceEvent(lifecycle.QuiescenceFact{
		Quiescent: true,
		Source:    s.lc.negotiated.QuiescenceSource,
		Watermark: s.lc.stream.State().ReducedThrough,
		Barrier:   barrier,
	}))
}

// emitLifecycleLocked claims the next sequence, reduces the event through the
// same reducer the family battery drives, and delivers the envelope on its
// identity-only carrier. An event this session cannot state truthfully is never
// published, and a delivery that fails fences the incarnation rather than
// leaving a gap the host cannot see.
func (s *agentSession) emitLifecycleLocked(ctx context.Context, event lifecycle.Event) error {
	if s.lc.fenced {
		return errLifecycleStreamFenced
	}

	envelope, err := s.lc.stream.Emit(event)
	if err != nil {
		s.lc.fenced = true

		return err
	}

	if err := s.deliverLifecycleNotification(ctx, envelope); err != nil {
		s.lc.fenced = true

		return err
	}

	return nil
}

// deliverLifecycleNotification writes one carrier notification. It carries the
// session identity it must carry and the envelope, and nothing else: the route
// envelope and the native message identity belong to notifications that mean
// something on their own.
func (s *agentSession) deliverLifecycleNotification(ctx context.Context, envelope map[string]any) error {
	s.agent.mu.Lock()
	closed := s.agent.closed
	conn := s.agent.conn
	s.agent.mu.Unlock()

	if closed {
		return errAgentClosed
	}

	if conn == nil {
		return errACPConnectionNotAttached
	}

	return conn.SessionUpdate(ctx, acp.SessionNotification{
		Meta:      lifecycleNotificationMeta(envelope),
		SessionId: s.id,
		Update:    lifecycleCarrier(),
	})
}
