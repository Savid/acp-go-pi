package piacp

import (
	"context"

	"github.com/savid/acp-go-pi/internal/pi"
)

// closeBoundaryBarrier names the proof a completed close produced, so a host
// reading the quiescence fact can tell which boundary stated it.
const closeBoundaryBarrier = "process-containment:close"

// settleCloseBoundary runs the close-fenced settlement order after the native
// tree is contained:
//
//	whole-tree containment/vacancy proof → terminalize the entities the session
//	still owns → durable resumable-snapshot commit → quiescence
//
// The close response follows, written by the caller. A boundary that did not
// complete terminalizes nothing, commits nothing new, and emits no quiescence
// fact: declaring terminal a set of work the session has just proved it cannot
// contain would make the next real event about it a post-terminal mutation.
func (s *agentSession) settleCloseBoundary(ctx context.Context, proc piProcess, containmentErr error) error {
	if !pi.ProcessContainmentComplete(containmentErr) {
		return nil
	}

	if err := s.lifecycleTerminalizeOwned(ctx); err != nil {
		return err
	}

	if err := s.commitCloseBoundary(ctx, nativeBoundaryVacant(proc)); err != nil {
		return err
	}

	if !nativeBoundaryVacant(proc) {
		return nil
	}

	return s.lifecycleCertifyBoundary(ctx, closeBoundaryBarrier)
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
func (s *agentSession) commitCloseBoundary(ctx context.Context, vacant bool) error {
	if err := s.commitMirror(ctx); err != nil {
		return err
	}

	streamID, turnID, cycleID := s.lifecycleIdentity()

	s.mu.Lock()
	rows := s.mirroredRows
	s.mu.Unlock()

	detail := "session close contained the native tree and committed the resumable snapshot"
	if !vacant {
		detail = "session close contained the native tree without enumerating its membership"
	}

	return s.commitLifecycleBoundary(ctx, lifecycleBoundaryRecord{
		StreamID:      streamID,
		TurnID:        turnID,
		CycleID:       cycleID,
		NativeRows:    rows,
		NativeState:   nativeStateCommitted,
		Detail:        detail,
		VacancyProven: vacant,
	})
}

// recordGenerationLoss ends the incarnation a dying native generation owned. A
// generation swap is not a user cancel and not a close, so the loss is written
// down explicitly: the stream stops here, and nothing the lost generation never
// delivered is reconstructed on the next one.
func (s *agentSession) recordGenerationLoss(ctx context.Context) error {
	streamID, turnID, cycleID := s.lifecycleIdentity()
	if streamID == "" {
		return nil
	}

	s.fenceLifecycleStream()

	s.mu.Lock()
	rows := s.mirroredRows
	s.mu.Unlock()

	return s.commitLifecycleBoundary(ctx, lifecycleBoundaryRecord{
		StreamID:    streamID,
		TurnID:      turnID,
		CycleID:     cycleID,
		NativeRows:  rows,
		NativeState: nativeStateRetained,
		Detail:      "the native process generation was replaced and its lifecycle stream ended with it",
	})
}
