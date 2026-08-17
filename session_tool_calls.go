package piacp

import (
	"context"
	"errors"

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

// publishNativeToolUpdate emits one partial tool result as a complete
// content snapshot. ACP replaces the tool call's content array wholesale on
// every content-bearing update, so each update carries the full current
// array; an update whose snapshot is empty is skipped rather than erasing
// delivered content with an empty replacement.
func (s *agentSession) publishNativeToolUpdate(ctx context.Context, toolCallID string, blocks []pi.ContentBlock) error {
	state := s.lockToolCallState(toolCallID)
	defer state.mu.Unlock()

	if state.terminalPublished {
		return nil
	}

	snapshot, failure := buildToolContent(state.content, blocks, s.agent.imageLimits())
	if failure != nil {
		return s.failToolImageOutputLocked(ctx, toolCallID, state, failure)
	}

	// Merging only appends, so an unchanged length means this partial adds
	// nothing; re-transmitting the identical array would only repeat its
	// base64 payloads.
	if len(snapshot) == 0 || len(snapshot) == len(state.content) {
		return nil
	}

	if err := s.emitUpdates(ctx, []acp.SessionUpdate{acp.UpdateToolCall(
		acp.ToolCallId(toolCallID),
		acp.WithUpdateContent(toolContentSnapshot(snapshot)),
	)}); err != nil {
		return err
	}

	s.recordToolContentLocked(state, snapshot)

	return nil
}

// publishNativeToolTerminal emits the terminal tool update: the native
// status plus, when the native result carries mappable content, the complete
// final content snapshot. A result without mappable content is a status-only
// update that leaves the delivered array intact.
func (s *agentSession) publishNativeToolTerminal(
	ctx context.Context,
	toolCallID string,
	status acp.ToolCallStatus,
	result *pi.ToolResult,
) error {
	state := s.lockToolCallState(toolCallID)
	defer state.mu.Unlock()

	if state.terminalPublished {
		return nil
	}

	opts := []acp.ToolCallUpdateOpt{acp.WithUpdateStatus(status)}

	var snapshot []toolContentItem

	if result != nil {
		var failure *imageOutputError

		snapshot, failure = buildToolContent(state.content, result.Content, s.agent.imageLimits())
		if failure != nil {
			return s.failToolImageOutputLocked(ctx, toolCallID, state, failure)
		}

		if len(snapshot) > 0 {
			opts = append(opts, acp.WithUpdateContent(toolContentSnapshot(snapshot)))
		}
	}

	if err := s.emitUpdates(ctx, []acp.SessionUpdate{
		acp.UpdateToolCall(acp.ToolCallId(toolCallID), opts...),
	}); err != nil {
		return err
	}

	s.recordToolContentLocked(state, snapshot)

	state.terminalPublished = true
	state.status = status

	return nil
}

// recordToolContentLocked retains the emitted snapshot for replace-semantics
// merging and notes image delivery for the turn's durability accounting.
func (s *agentSession) recordToolContentLocked(state *turnToolCall, snapshot []toolContentItem) {
	if len(snapshot) == 0 {
		return
	}

	state.content = snapshot

	for _, item := range snapshot {
		if item.imageBytes > 0 {
			s.markTurnImageEmission()

			return
		}
	}
}

// failToolImageOutputLocked handles an adapter-side image representation
// failure on tool provenance. The tool call reports failed either way. A
// verdict the model can act on carries its guidance as that call's own
// content and the turn runs on with its context; the artifact window breaking
// is emitted as a status-only update for attribution and then fails the turn
// with the image-output envelope.
func (s *agentSession) failToolImageOutputLocked(
	ctx context.Context,
	toolCallID string,
	state *turnToolCall,
	failure *imageOutputError,
) error {
	guidance, recoverable := imageOutputGuidance(failure)

	opts := []acp.ToolCallUpdateOpt{acp.WithUpdateStatus(acp.ToolCallStatusFailed)}
	if recoverable {
		opts = append(opts, acp.WithUpdateContent([]acp.ToolCallContent{
			acp.ToolContent(acp.TextBlock(guidance)),
		}))
	}

	emitErr := s.emitUpdates(ctx, []acp.SessionUpdate{
		acp.UpdateToolCall(acp.ToolCallId(toolCallID), opts...),
	})

	state.terminalPublished = true
	state.status = acp.ToolCallStatusFailed

	if recoverable {
		return emitErr
	}

	return errors.Join(imageOutputTurnFailure(failure), emitErr)
}
