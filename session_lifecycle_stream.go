package piacp

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/coder/acp-go-sdk"

	"github.com/savid/acp-go-pi/internal/lifecycle"
)

const lifecycleFieldVersion = "version"

const lifecycleFieldStreamID = "streamId"

const lifecycleFieldAction = "action"

const lifecycleFieldActionID = "actionId"

const lifecycleOwnerIDKey = "id"

const lifecycleOwnerKey = "owner"

var lifecycleRandRead = rand.Read

// errLifecycleStreamFenced reports that the incarnation that owned the stream
// ended. Its events are gone with it and are never reconstructed.
var errLifecycleStreamFenced = errors.New("lifecycle stream is fenced")

// errLifecycleActionUnowned refuses a permission or elicitation a live
// incarnation has no turn to attribute. The correlation names an owner or the
// request does not go out.
var errLifecycleActionUnowned = errors.New("no open turn owns this lifecycle action")

// errLifecycleActionAnnouncement is the fixed native-facing refusal used when
// the host request crossed its write fence but its lifecycle announcement did
// not become durable on the ordered stream.
var errLifecycleActionAnnouncement = errors.New("lifecycle action announcement failed")

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
	// origin is the provenance of the turn currently holding the foreground. A
	// cycle ends for the reason it opened, so an agent-origin cycle never
	// reports the submission cause of a prompt that did not cause it.
	origin lifecycle.Cause
	// lostTurnID and lostCycleID are the identity a fence retired without a
	// terminal event. The relaunch path writes that loss down as a boundary
	// record, and it runs after the fence, so the identity outlives the fence
	// that took it: a loss recorded under an empty turn would say a generation
	// died owning nothing when it died mid-cycle.
	lostTurnID  string
	lostCycleID string
	// blockers are the announced actions still blocking the current foreground
	// cycle. The cycle is released when the last one resolves, never the first.
	blockers map[string]struct{}
	fenced   bool
	closed   bool
	// delivery is the ordered handoff chain. Stream reduction is claimed under
	// lcMu, while host I/O waits outside it; each handoff waits for its exact
	// predecessor before writing.
	delivery      *lifecycleDeliveryFence
	quarantineErr error
	// vacancyProven records what the last completed boundary proved, so an
	// opening snapshot states the boundary it has rather than the class the
	// configuration advertises.
	vacancyProven bool
}

type lifecycleDeliveryFence struct {
	done chan struct{}
	err  error
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
	s.lc.origin = ""
	s.lc.lostTurnID = ""
	s.lc.lostCycleID = ""
	s.lc.blockers = make(map[string]struct{})
	s.lc.fenced = false
	s.lc.quarantineErr = nil
	initial := &lifecycleDeliveryFence{done: make(chan struct{})}
	close(initial.done)
	s.lc.delivery = initial

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

	s.fenceLifecycleLocked()
}

// fenceLifecycleGeneration ends the incarnation a named native generation
// owned. A containment raised on one generation never retires the stream a
// later incarnation has already opened: the failure belongs to the process that
// produced it, and the successor mints its own identity space.
func (s *agentSession) fenceLifecycleGeneration(generation uint64) {
	s.lcMu.Lock()
	defer s.lcMu.Unlock()

	if s.lc.generation != generation {
		return
	}

	s.fenceLifecycleLocked()
}

// fenceLifecycleLocked retires the open turn without a terminal event and keeps
// its identity for the loss record the relaunch path still owes.
func (s *agentSession) fenceLifecycleLocked() {
	if s.lc.turnID != "" {
		s.lc.lostTurnID = s.lc.turnID
		s.lc.lostCycleID = s.lc.cycleID
	}

	s.lc.fenced = true
	s.lc.turnID = ""
	s.lc.origin = ""
	s.lc.blockers = nil
}

// closeLifecycleSession fences the session itself. Once close containment has
// completed, the session admits no further incarnation of itself.
//
// The emitter learns the same fact the session just recorded. Its own validator
// is the last thing an envelope passes before it becomes bytes, so telling it
// the session is over means a post-close event fails closed where it is minted
// rather than relying on this struct's flag being consulted first on every path
// that ever reaches emission.
func (s *agentSession) closeLifecycleSession() {
	s.lcMu.Lock()
	defer s.lcMu.Unlock()

	s.fenceLifecycleLocked()
	s.lc.closed = true

	if s.lc.stream != nil {
		s.lc.stream.Close()
	}
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
	s.lc.origin = lifecycle.CauseSubmission

	if err := s.emitLifecycleLocked(ctx, lifecycle.AcceptedEvent(submission, turnID)); err != nil {
		return err
	}

	return s.emitLifecycleLocked(ctx, lifecycle.RunningEvent(cycleID, turnID))
}

// lifecycleOpenAgentCycle opens the foreground cycle the harness began on its
// own. Exactly two events open a turn and this is the second: an
// activity-caused running transition. It states no acceptance, because no
// client frame was accepted, and no submission or run identity, because there
// is none to echo.
//
// A generation that no longer owns a negotiated stream is an ownership failure.
// The cycle belongs to the incarnation that produced it, and silently treating
// the mismatch as absence would leave the router owning invisible work.
func (s *agentSession) lifecycleOpenAgentCycle(ctx context.Context, cycle *agentCycle) error {
	negotiated := s.agent.lifecycleNegotiated().Present()

	s.lcMu.Lock()
	defer s.lcMu.Unlock()

	if s.lc.stream == nil {
		if !negotiated && !s.lc.negotiated.Present() {
			return nil
		}

		return errLifecycleStreamFenced
	}

	if cycle == nil || s.lc.fenced || s.lc.closed {
		return errLifecycleStreamFenced
	}

	if s.lc.generation != cycle.generation {
		return fmt.Errorf("%w: native generation %d does not own lifecycle generation %d",
			errLifecycleStreamFenced, cycle.generation, s.lc.generation)
	}

	if s.lc.turnID != "" {
		return fmt.Errorf("%w: lifecycle foreground is already owned", errLifecycleStreamFenced)
	}

	if cycle.generation == 0 {
		return fmt.Errorf("%w: agent cycle generation is absent", errLifecycleStreamFenced)
	}

	turnID, err := newLifecycleID("turn")
	if err != nil {
		return err
	}

	cycleID, err := newLifecycleID("cycle")
	if err != nil {
		return err
	}

	// Install ownership before delivery. A failed running transition is a lost
	// cycle of this incarnation, not work whose identity never existed; the
	// containment fence must therefore be able to retain these exact ids.
	s.lc.turnID = turnID
	s.lc.cycleID = cycleID
	s.lc.origin = lifecycle.CauseActivity
	cycle.turnID = turnID
	cycle.cycleID = cycleID

	if err := s.emitLifecycleLocked(ctx, lifecycle.AgentRunningEvent(cycleID, turnID)); err != nil {
		return err
	}

	return nil
}

// lifecycleSettleAgentCycle ends the agent-origin cycle with the outcome it
// reached. It names the cycle's own identity rather than whatever the stream
// holds now, so a cycle whose turn was already ended elsewhere is not ended
// twice.
func (s *agentSession) lifecycleSettleAgentCycle(ctx context.Context, cycle *agentCycle, verdict turnVerdict) error {
	negotiated := s.agent.lifecycleNegotiated().Present()

	s.lcMu.Lock()
	defer s.lcMu.Unlock()

	if s.lc.stream == nil {
		if !negotiated && !s.lc.negotiated.Present() {
			return nil
		}

		return errLifecycleStreamFenced
	}

	if cycle == nil || s.lc.fenced || s.lc.closed || s.lc.generation != cycle.generation {
		return errLifecycleStreamFenced
	}

	if cycle.turnID == "" || cycle.cycleID == "" ||
		s.lc.turnID != cycle.turnID || s.lc.cycleID != cycle.cycleID ||
		s.lc.origin != lifecycle.CauseActivity {
		return fmt.Errorf("%w: agent cycle does not own the lifecycle foreground", errLifecycleStreamFenced)
	}

	if err := s.terminalizeBlockersLocked(ctx); err != nil {
		return err
	}

	if err := s.emitLifecycleLocked(ctx, lifecycle.IdleEventFor(
		lifecycle.CauseActivity, cycle.cycleID, cycle.turnID, verdict.stopReason, verdict.outcome,
	)); err != nil {
		return err
	}

	// Only a delivered terminal idle releases the identity. A delivery failure
	// is an incarnation loss and fenceLifecycleLocked must still see the cycle.
	s.lc.turnID = ""
	s.lc.origin = ""

	return nil
}

// lifecycleLossIdentity is the identity a generation-loss record names. A live
// turn answers for itself; a turn a fence already retired answers with the
// identity that fence kept, because the loss the relaunch path is about to
// write down is exactly that turn's.
func (s *agentSession) lifecycleLossIdentity() (string, string, string) {
	s.lcMu.Lock()
	defer s.lcMu.Unlock()

	if s.lc.stream == nil {
		return "", "", ""
	}

	if s.lc.turnID != "" {
		return s.lc.stream.ID(), s.lc.turnID, s.lc.cycleID
	}

	if s.lc.lostTurnID != "" {
		return s.lc.stream.ID(), s.lc.lostTurnID, s.lc.lostCycleID
	}

	return s.lc.stream.ID(), "", s.lc.cycleID
}

// clearLifecycleLoss forgets a retired identity once its loss is durable. It
// runs only after the commit, so a failed commit leaves the identity for the
// retry that still owes the record.
func (s *agentSession) clearLifecycleLoss() {
	s.lcMu.Lock()
	defer s.lcMu.Unlock()

	s.lc.lostTurnID = ""
	s.lc.lostCycleID = ""
}

// lifecycleStreamID reports the incarnation a boundary record names, or the
// empty string where the host negotiated no lifecycle stream.
func (s *agentSession) lifecycleStreamID() string {
	s.lcMu.Lock()
	defer s.lcMu.Unlock()

	if s.lc.stream == nil {
		return ""
	}

	return s.lc.stream.ID()
}

// lifecycleSettleTurn ends the accepted cycle with the outcome the turn
// actually reached. Every blocker the cycle still holds terminalizes first: the
// resolution is the reason the foreground may move.
func (s *agentSession) lifecycleSettleTurn(ctx context.Context, stopReason string, outcome lifecycle.Outcome) error {
	s.mu.Lock()
	outbox := s.outbox
	s.mu.Unlock()

	s.lcMu.Lock()

	if s.lc.stream == nil || s.lc.turnID == "" {
		s.lcMu.Unlock()

		return nil
	}

	if err := s.terminalizeBlockersLocked(ctx); err != nil {
		s.lcMu.Unlock()

		return errors.Join(err, s.containGenerationSync(ctx, outbox, "terminal lifecycle delivery failed"))
	}

	origin, err := s.lc.turnOriginLocked()
	if err != nil {
		s.fenceLifecycleLocked()
		s.lcMu.Unlock()

		return err
	}

	turnID, cycleID := s.lc.turnID, s.lc.cycleID

	emitErr := s.emitLifecycleLocked(ctx, lifecycle.IdleEventFor(origin, cycleID, turnID, stopReason, outcome))
	if emitErr == nil {
		s.lc.turnID = ""
		s.lc.origin = ""
	}
	s.lcMu.Unlock()

	if emitErr != nil {
		return errors.Join(emitErr, s.containGenerationSync(ctx, outbox, "terminal lifecycle delivery failed"))
	}

	return nil
}

func (l *lifecycleState) turnOriginLocked() (lifecycle.Cause, error) {
	switch l.origin {
	case lifecycle.CauseSubmission, lifecycle.CauseActivity:
		return l.origin, nil
	default:
		return "", fmt.Errorf("%w: open lifecycle turn has no valid origin", errLifecycleStreamFenced)
	}
}

// pendingAction is the lifecycle identity of one request awaiting an answer. It
// is minted before the request is sent, so the request itself carries the
// identity the ordered action will announce.
type pendingAction struct {
	actionID string
	streamID string
	// generation is the native process incarnation the owner turn ran on. An
	// action captured for one incarnation is never announced on another, even
	// where a later stream reuses the same identity space.
	generation uint64
	owner      lifecycle.Owner
	outbox     *sessionOutbox
}

// prepareLifecycleAction mints the identity for one permission or elicitation
// without publishing anything. Nothing is announced yet: the request that
// answers the action goes on the wire first.
//
// With no negotiated incarnation the request goes out plainly. A fenced
// negotiated incarnation is an ownership failure and refuses the request.
//
// A live incarnation with no open turn is neither. While version 1 is
// negotiated the correlation is stamped on *every* permission and elicitation,
// so there is no shape for one without an owner to name. A dialog only reaches
// this path from an open foreground delivery — a dialog with no foreground
// cycle to block is already cancelled at the pump — so this state is a dialog
// handler that outlived the turn that spawned it, and the request it would send
// asks for an answer to a turn that is over. It is refused on the same terms
// the pump refuses an unattributable dialog: the caller answers pi with a
// native cancel rather than putting a correlation-less request on the wire.
func (s *agentSession) prepareLifecycleAction() (pendingAction, bool, error) {
	return s.prepareLifecycleActionFor(nil)
}

func (s *agentSession) prepareLifecycleActionFor(outbox *sessionOutbox) (pendingAction, bool, error) {
	if outbox == nil {
		s.mu.Lock()
		outbox = s.outbox
		s.mu.Unlock()
	}

	s.lcMu.Lock()
	defer s.lcMu.Unlock()

	if s.lc.stream == nil {
		negotiated := s.lc.negotiated.Present()
		if !negotiated && s.agent != nil {
			negotiated = s.agent.lifecycleNegotiated().Present()
		}

		if !negotiated {
			return pendingAction{}, false, nil
		}

		return pendingAction{}, false, errLifecycleStreamFenced
	}

	if s.lc.fenced || s.lc.closed {
		return pendingAction{}, false, errLifecycleStreamFenced
	}

	if s.lc.turnID == "" {
		return pendingAction{}, false, errLifecycleActionUnowned
	}

	if outbox != nil && outbox.generation != s.lc.generation {
		return pendingAction{}, false, errLifecycleActionUnowned
	}

	actionID, err := newLifecycleID("action")
	if err != nil {
		return pendingAction{}, false, err
	}

	return pendingAction{
		actionID:   actionID,
		streamID:   s.lc.stream.ID(),
		generation: s.lc.generation,
		owner:      lifecycle.Owner{Type: lifecycle.OwnerTurn, ID: s.lc.turnID},
		outbox:     outbox,
	}, true, nil
}

// revokeLifecycleAction removes any blocker inserted before a failed
// announcement fenced delivery. No compensating frame is emitted: the ordered
// stream could not publish the announcement completely and is contained with
// its exact native generation.
func (s *agentSession) revokeLifecycleAction(action pendingAction) {
	s.lcMu.Lock()
	defer s.lcMu.Unlock()

	if s.lc.generation != action.generation {
		return
	}

	delete(s.lc.blockers, action.actionID)
	s.fenceLifecycleLocked()
}

// announceLifecycleAction publishes the ordered action for a request already on
// the wire, plus the transition its blocking causes. A blocking action never
// moves the foreground by itself, so the accompanying transition is emitted with
// it and only for the first blocker of the cycle.
//
// The owner is authenticated against the live foreground, not merely against
// the existence of one. An action captured while turn A held the foreground is
// announced only while turn A still holds it on the same incarnation: a delayed
// one arriving under turn B would otherwise block B's cycle on a request B
// never caused and hand B a blocker it can never resolve. The refusal is what
// makes the caller answer pi with a native cancel instead.
func (s *agentSession) announceLifecycleAction(
	ctx context.Context,
	action pendingAction,
	kind lifecycle.ActionKind,
) error {
	s.lcMu.Lock()
	defer s.lcMu.Unlock()

	if s.lc.stream == nil {
		return nil
	}

	if s.lc.fenced || s.lc.generation != action.generation ||
		s.lc.stream.ID() != action.streamID || s.lc.turnID != action.owner.ID {
		return errLifecycleActionUnowned
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
	s.mu.Lock()
	outbox := s.outbox
	s.mu.Unlock()

	return s.lifecycleResolveCapturedAction(ctx, outbox, actionID, state)
}

func (s *agentSession) lifecycleResolveCapturedAction(
	ctx context.Context,
	outbox *sessionOutbox,
	actionID string,
	state lifecycle.ActionState,
) error {
	s.lcMu.Lock()

	if s.lc.stream == nil {
		s.lcMu.Unlock()

		return nil
	}

	if _, held := s.lc.blockers[actionID]; !held {
		s.lcMu.Unlock()

		return nil
	}

	if err := s.emitLifecycleLocked(ctx, lifecycle.ActionResolvedEvent(actionID, state)); err != nil {
		s.lcMu.Unlock()

		return errors.Join(err, s.containGenerationSync(ctx, outbox, "action resolution delivery failed"))
	}

	delete(s.lc.blockers, actionID)

	if len(s.lc.blockers) > 0 || s.lc.turnID == "" {
		s.lcMu.Unlock()

		return nil
	}

	err := s.emitLifecycleLocked(ctx, lifecycle.RunningEvent(s.lc.cycleID, s.lc.turnID))
	s.lcMu.Unlock()

	if err != nil {
		return errors.Join(err, s.containGenerationSync(ctx, outbox, "action resolution delivery failed"))
	}

	return nil
}

// terminalizeBlockersLocked cancels every action still blocking the cycle. A
// cancelled cycle terminalizes its blockers before it reports its terminal
// idle, because terminal is immutable and a later real event on one of them
// would be a post-terminal mutation.
func (s *agentSession) terminalizeBlockersLocked(ctx context.Context) error {
	for actionID := range s.lc.blockers {
		if err := s.emitLifecycleLocked(ctx, lifecycle.ActionResolvedEvent(actionID, lifecycle.ActionCancelled)); err != nil {
			return err
		}

		delete(s.lc.blockers, actionID)
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

	origin, err := s.lc.turnOriginLocked()
	if err != nil {
		s.fenceLifecycleLocked()

		return err
	}

	turnID, cycleID := s.lc.turnID, s.lc.cycleID
	s.lc.turnID = ""
	s.lc.origin = ""

	return s.emitLifecycleLocked(ctx, lifecycle.IdleEventFor(
		origin, cycleID, turnID, lifecycle.StopReasonCancelled, lifecycle.OutcomeCancelled,
	))
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
	if s.lc.fenced || s.lc.quarantineErr != nil {
		if s.lc.quarantineErr != nil {
			return s.lc.quarantineErr
		}

		return errLifecycleStreamFenced
	}

	envelope, err := s.lc.stream.Emit(event)
	if err != nil {
		s.lc.fenced = true

		return err
	}

	generation := s.lc.generation
	prior := s.lc.delivery
	delivery := &lifecycleDeliveryFence{done: make(chan struct{})}
	s.lc.delivery = delivery

	// Host I/O may block or ignore cancellation. The ordered delivery owner is
	// retained in the chain, but it never retains lcMu while it waits.
	s.lcMu.Unlock()

	if prior != nil {
		<-prior.done
		err = prior.err
	}

	if err == nil {
		err = s.deliverLifecycleNotification(ctx, envelope)
	}

	s.lcMu.Lock()

	if s.lc.generation != generation {
		err = errors.Join(err, errLifecycleStreamFenced)
	}

	if s.lc.quarantineErr != nil {
		err = errors.Join(err, s.lc.quarantineErr)
	}

	if err != nil {
		s.lc.fenced = true
	}

	delivery.err = err
	close(delivery.done)

	return err
}

func (s *agentSession) quarantineLifecycleGeneration(generation uint64, err error) {
	if err == nil {
		return
	}

	s.lcMu.Lock()
	defer s.lcMu.Unlock()

	if s.lc.generation != generation || s.lc.quarantineErr != nil {
		return
	}

	s.lc.quarantineErr = errors.Join(ErrContainmentIncomplete, err)
}

func (s *agentSession) lifecycleGenerationQuarantine(generation uint64) error {
	s.lcMu.Lock()
	defer s.lcMu.Unlock()

	if s.lc.quarantineErr == nil {
		return nil
	}

	if s.lc.generation != generation {
		return errLifecycleStreamFenced
	}

	return s.lc.quarantineErr
}

// deliverLifecycleNotification writes one carrier notification. It carries the
// session identity it must carry and the envelope, and nothing else: the route
// envelope and the native message identity belong to notifications that mean
// something on their own.
func (s *agentSession) deliverLifecycleNotification(ctx context.Context, envelope map[string]any) error {
	s.mu.Lock()
	detached := s.lifecycleDeliveryDetached
	s.mu.Unlock()

	if detached {
		return nil
	}

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

func (s *agentSession) detachLifecycleDelivery() {
	s.mu.Lock()
	s.lifecycleDeliveryDetached = true
	s.mu.Unlock()
}

// lifecycleActionMeta renders the action correlation value the agent stamps on
// every permission and elicitation it emits while the extension is negotiated.
// The action id is lifecycle identity only: it never routes or authorizes the
// callback, which stays the reserved route envelope's job.
func lifecycleActionMeta(streamID, actionID string, owner lifecycle.Owner) map[string]any {
	return map[string]any{lifecycleMetaKey: map[string]any{
		lifecycleFieldVersion:  lifecycle.Version,
		lifecycleFieldStreamID: streamID,
		lifecycleFieldAction: map[string]any{
			lifecycleFieldActionID: actionID,
			lifecycleOwnerKey:      map[string]any{jsonFieldType: string(owner.Type), lifecycleOwnerIDKey: owner.ID},
		},
	}}
}

// newLifecycleID mints one opaque lifecycle identifier. The identities this
// adapter mints are adapter provenance: they name a stream, cycle, turn, or
// action inside one incarnation and are never derived from a native id or from
// the route nonce.
func newLifecycleID(prefix string) (string, error) {
	var data [16]byte
	if _, err := lifecycleRandRead(data[:]); err != nil {
		return "", err
	}

	return prefix + "-" + hex.EncodeToString(data[:]), nil
}

// lifecycleNotificationMeta is the envelope carrier's `_meta`. The envelope
// rides the notification, never the update object's own per-entity metadata.
func lifecycleNotificationMeta(envelope map[string]any) map[string]any {
	return map[string]any{lifecycleMetaKey: envelope}
}

// lifecycleCarrier is the identity-only session_info_update every envelope
// rides. It sets no title and no timestamp, so carrying an envelope mutates no
// state a client reduces.
func lifecycleCarrier() acp.SessionUpdate {
	return acp.SessionUpdate{SessionInfoUpdate: &acp.SessionSessionInfoUpdate{}}
}
