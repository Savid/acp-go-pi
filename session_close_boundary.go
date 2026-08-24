package piacp

import (
	"context"
	"errors"
	"slices"

	"github.com/savid/acp-go-pi/internal/lifecycle"
	"github.com/savid/acp-go-pi/internal/pi"
)

// closeBoundaryBarrier names the proof a completed close produced, so a host
// reading the quiescence fact can tell which boundary stated it.
const closeBoundaryBarrier = "process-containment:close"

// settleCloseBoundary runs the close-fenced settlement order after the native
// tree is contained:
//
//	whole-tree containment/vacancy proof → durable foreground mirror commit
//	→ terminalize the entities the session still owns → durable resumable
//	lifecycle-boundary commit → quiescence → stream fence
//
// The close response follows, written by the caller. A boundary that did not
// complete terminalizes nothing, commits nothing new, and emits no quiescence
// fact: declaring terminal a set of work the session has just proved it cannot
// contain would make the next real event about it a post-terminal mutation.
func (s *agentSession) settleCloseBoundary(ctx context.Context, proc piProcess, containmentErr error) error {
	if !pi.ProcessContainmentComplete(containmentErr) {
		return nil
	}

	terminal := s.prepareCloseLifecycleTerminal()
	vacant := nativeBoundaryVacant(proc)

	if err := s.commitCloseForegroundMirror(ctx); err != nil {
		s.fenceLifecycleStream()

		return err
	}

	if s.persistenceFenced() {
		s.fenceLifecycleStream()

		return nil
	}

	if err := s.publishCloseLifecycleTerminal(ctx, terminal); err != nil {
		s.fenceLifecycleStream()

		return err
	}

	if err := s.commitCloseResumableBoundary(ctx, vacant, terminal); err != nil {
		s.fenceLifecycleStream()

		return err
	}

	if !vacant {
		s.fenceLifecycleStream()

		return nil
	}

	if err := s.lifecycleCertifyBoundary(ctx, closeBoundaryBarrier); err != nil {
		s.fenceLifecycleStream()

		return err
	}

	s.fenceLifecycleStream()

	return nil
}

func (s *agentSession) persistenceFenced() bool {
	s.commitMu.Lock()
	defer s.commitMu.Unlock()

	return s.persistFenced
}

// nativeBoundaryVacant reports whether the contained boundary enumerated its
// own tree and found it empty. A boundary that cannot enumerate its membership
// reports nothing rather than a floor of zero, so an unproven boundary can never
// be read as a proven one.
func nativeBoundaryVacant(proc piProcess) bool {
	inventory, ok := proc.(providerProcessInventory)
	if !ok {
		return false
	}

	count, available := inventory.ProviderDescendantCount()

	return available && count == 0
}

// commitCloseBoundary commits the resumable snapshot the quiescence fact stands
// behind. A quiescence fact asserts that no later event is possible from the
// work it fences, so the store must already hold everything a later
// session/load or session/resume needs.
func (s *agentSession) commitCloseForegroundMirror(ctx context.Context) error {
	return s.commitMirror(ctx)
}

func (s *agentSession) commitCloseResumableBoundary(
	ctx context.Context,
	vacant bool,
	terminal closeLifecycleTerminal,
) error {
	s.mu.Lock()
	rows := s.mirroredRows
	s.mu.Unlock()

	detail := "session close contained the native tree and committed the resumable snapshot"
	if !vacant {
		detail = "session close contained the native tree without enumerating its membership"
	}

	return s.commitLifecycleBoundary(ctx, lifecycleBoundaryRecord{
		StreamID:      terminal.streamID,
		TurnID:        terminal.turnID,
		CycleID:       terminal.cycleID,
		Outcome:       terminal.outcome,
		StopReason:    terminal.stopReason,
		NativeRows:    rows,
		NativeState:   nativeStateCommitted,
		Detail:        detail,
		VacancyProven: vacant,
	})
}

type closeLifecycleTerminal struct {
	generation uint64
	streamID   string
	turnID     string
	cycleID    string
	origin     lifecycle.Cause
	blockers   []string
	outcome    string
	stopReason string
	live       bool
}

// prepareCloseLifecycleTerminal captures the exact host-visible transitions a
// completed boundary owes without publishing or mutating them. The foreground
// mirror is committed before this plan can escape; the resumable lifecycle
// boundary is committed immediately after its terminal notifications.
func (s *agentSession) prepareCloseLifecycleTerminal() closeLifecycleTerminal {
	s.lcMu.Lock()
	defer s.lcMu.Unlock()

	terminal := closeLifecycleTerminal{
		generation: s.lc.generation,
		turnID:     s.lc.turnID,
		cycleID:    s.lc.cycleID,
		origin:     s.lc.origin,
		live:       s.lc.stream != nil && !s.lc.fenced && !s.lc.closed,
	}
	if s.lc.stream != nil {
		terminal.streamID = s.lc.stream.ID()
	}

	for actionID := range s.lc.blockers {
		terminal.blockers = append(terminal.blockers, actionID)
	}

	slices.Sort(terminal.blockers)

	if terminal.turnID != "" {
		terminal.outcome = string(lifecycle.OutcomeCancelled)
		terminal.stopReason = lifecycle.StopReasonCancelled
	}

	return terminal
}

func (s *agentSession) publishCloseLifecycleTerminal(ctx context.Context, terminal closeLifecycleTerminal) error {
	if !terminal.live {
		return nil
	}

	s.lcMu.Lock()
	defer s.lcMu.Unlock()

	if s.lc.stream == nil || s.lc.fenced || s.lc.closed ||
		s.lc.generation != terminal.generation || s.lc.stream.ID() != terminal.streamID ||
		s.lc.turnID != terminal.turnID || s.lc.cycleID != terminal.cycleID || s.lc.origin != terminal.origin ||
		!sameLifecycleBlockers(s.lc.blockers, terminal.blockers) {
		s.fenceLifecycleLocked()

		return errors.New("close lifecycle ownership changed before durable terminal publication")
	}

	for _, actionID := range terminal.blockers {
		if err := s.emitLifecycleLocked(ctx, lifecycle.ActionResolvedEvent(actionID, lifecycle.ActionCancelled)); err != nil {
			return err
		}

		delete(s.lc.blockers, actionID)
	}

	if terminal.turnID == "" {
		return nil
	}

	if err := s.emitLifecycleLocked(ctx, lifecycle.IdleEventFor(
		terminal.origin,
		terminal.cycleID,
		terminal.turnID,
		terminal.stopReason,
		lifecycle.Outcome(terminal.outcome),
	)); err != nil {
		return err
	}

	s.lc.turnID = ""
	s.lc.origin = ""

	return nil
}

func sameLifecycleBlockers(current map[string]struct{}, expected []string) bool {
	if len(current) != len(expected) {
		return false
	}

	for _, actionID := range expected {
		if _, ok := current[actionID]; !ok {
			return false
		}
	}

	return true
}

// recordGenerationLoss ends the incarnation a dying native generation owned. A
// generation swap is not a user cancel and not a close, so the loss is written
// down explicitly: the stream stops here, and nothing the lost generation never
// delivered is reconstructed on the next one.
func (s *agentSession) recordGenerationLoss(ctx context.Context) error {
	streamID, turnID, cycleID := s.lifecycleLossIdentity()
	if streamID == "" {
		return nil
	}

	s.fenceLifecycleStream()

	s.mu.Lock()
	rows := s.mirroredRows
	s.mu.Unlock()

	if err := s.commitLifecycleBoundary(ctx, lifecycleBoundaryRecord{
		StreamID:    streamID,
		TurnID:      turnID,
		CycleID:     cycleID,
		NativeRows:  rows,
		NativeState: nativeStateRetained,
		Detail:      "the native process generation was replaced and its lifecycle stream ended with it",
	}); err != nil {
		return err
	}

	s.clearLifecycleLoss()

	return nil
}
