package piacp

import (
	"context"

	"github.com/coder/acp-go-sdk"

	"github.com/savid/acp-go-pi/internal/pi"
)

// resetTurnTools fences every exact-ID record, then detaches the index before
// another turn can start. It is never called while s.mu is held:
// permission completion takes that lock while unwinding its tracked dialog.
func (s *agentSession) resetTurnTools() {
	s.toolMu.Lock()
	for _, state := range s.turnTools {
		fenceTurnTool(state)
	}

	s.turnTools = nil
	s.toolMu.Unlock()
}

func fenceTurnTool(state *turnToolCall) {
	state.mu.Lock()
	_ = state.status
	state.mu.Unlock()
}

// lockToolCallState acquires the per-ID record before releasing the index
// lock. This closes the otherwise possible teardown gap where an operation
// could retain a detached record and publish through it after the reset fence.
func (s *agentSession) lockToolCallState(toolCallID string) *turnToolCall {
	s.toolMu.Lock()

	if s.turnTools == nil {
		s.turnTools = make(map[string]*turnToolCall)
	}

	state := s.turnTools[toolCallID]
	if state == nil {
		state = &turnToolCall{}
		s.turnTools[toolCallID] = state
	}

	state.mu.Lock()
	s.toolMu.Unlock()

	return state
}

func (s *agentSession) publishNativeToolStart(ctx context.Context, event pi.ToolExecutionStartEvent) error {
	state := s.lockToolCallState(event.ToolCallID)
	defer state.mu.Unlock()

	if state.nativeStartPublished || state.terminalPublished {
		return nil
	}

	if state.published {
		opts := []acp.ToolCallUpdateOpt{
			acp.WithUpdateTitle(event.ToolName),
			acp.WithUpdateKind(toolKindForName(event.ToolName)),
			acp.WithUpdateStatus(acp.ToolCallStatusInProgress),
		}
		if len(event.Args) > 0 {
			opts = append(opts, acp.WithUpdateRawInput(event.Args))
		}

		if err := s.emitUpdates(ctx, []acp.SessionUpdate{
			acp.UpdateToolCall(acp.ToolCallId(event.ToolCallID), opts...),
		}); err != nil {
			return err
		}
	} else {
		opts := []acp.ToolCallStartOpt{
			acp.WithStartKind(toolKindForName(event.ToolName)),
			acp.WithStartStatus(acp.ToolCallStatusInProgress),
		}
		if len(event.Args) > 0 {
			opts = append(opts, acp.WithStartRawInput(event.Args))
		}

		if err := s.emitUpdates(ctx, []acp.SessionUpdate{
			acp.StartToolCall(acp.ToolCallId(event.ToolCallID), event.ToolName, opts...),
		}); err != nil {
			return err
		}

		state.published = true
	}

	state.nativeStartPublished = true
	state.status = acp.ToolCallStatusInProgress

	return nil
}

func (s *agentSession) publishNativeToolUpdate(ctx context.Context, toolCallID string, update acp.SessionUpdate) error {
	state := s.lockToolCallState(toolCallID)
	defer state.mu.Unlock()

	if state.terminalPublished {
		return nil
	}

	return s.emitUpdates(ctx, []acp.SessionUpdate{update})
}

func (s *agentSession) publishNativeToolTerminal(
	ctx context.Context,
	toolCallID string,
	status acp.ToolCallStatus,
	update acp.SessionUpdate,
) error {
	state := s.lockToolCallState(toolCallID)
	defer state.mu.Unlock()

	if state.terminalPublished {
		return nil
	}

	if err := s.emitUpdates(ctx, []acp.SessionUpdate{update}); err != nil {
		return err
	}

	state.terminalPublished = true
	state.status = status

	return nil
}
