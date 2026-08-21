package piacp

import (
	"context"
	"log/slog"
	"strings"

	"github.com/coder/acp-go-sdk"

	"github.com/savid/acp-go-pi/internal/observer"
	"github.com/savid/acp-go-pi/internal/pi"
)

const (
	permissionOptionAllow acp.PermissionOptionId = "allow"
	permissionOptionDeny  acp.PermissionOptionId = "deny"

	uiMethodSelect  = "select"
	uiMethodConfirm = "confirm"
	uiMethodInput   = "input"
	uiMethodEditor  = "editor"

	toolNameRead     = "read"
	toolNameEdit     = "edit"
	toolNameWrite    = "write"
	toolNameBash     = "bash"
	toolNameGrep     = "grep"
	toolNameFind     = "find"
	toolNameGlob     = "glob"
	toolNameLs       = "ls"
	toolNameFetch    = "fetch"
	toolNameWebFetch = "web_fetch"
)

// handleNativeUIDialog answers one blocking extension UI dialog. A dialog whose
// select title carries the bridge permission marker is a tool-call permission
// request; every other dialog is a non-permission question relayed as
// elicitation. Exactly one UIResponse is always written back so the bridge
// extension never hangs.
func (s *agentSession) handleNativeUIDialog(ctx context.Context, dialog *nativeDialog) {
	if dialog == nil {
		return
	}

	request := dialog.request
	if strings.HasPrefix(request.Title, pi.PermissionTitleMarker) {
		prompt, ok := pi.ParsePermissionTitle(request.Title)
		if request.Method != uiMethodSelect || !ok {
			dialog.answer(ctx, s, pi.UICancelResponse(request.ID))

			return
		}

		s.handlePermissionDialog(ctx, dialog, prompt)

		return
	}

	s.handleNativeElicitationDialog(ctx, dialog)
}

// registerDialog wraps ctx in a tracked cancellable context so that
// session/cancel and teardown can resolve a pending dialog as cancelled
// instead of leaving pi's extension blocked.
func (s *agentSession) registerDialog(ctx context.Context, id string) (context.Context, func()) {
	dialogCtx, cancel := context.WithCancelCause(ctx)
	entry := &dialogCancel{cancel: cancel}

	s.mu.Lock()
	if s.pendingDialogs == nil {
		s.pendingDialogs = make(map[string]*dialogCancel)
	}

	s.pendingDialogs[id] = entry
	turnCancelled := s.turnCancelled
	s.mu.Unlock()

	if turnCancelled {
		cancel(errSessionInteractionClosed)
	}

	return dialogCtx, func() {
		s.mu.Lock()
		if s.pendingDialogs[id] == entry {
			delete(s.pendingDialogs, id)
		}
		s.mu.Unlock()

		cancel(context.Canceled)
	}
}

// handlePermissionDialog maps one bridge permission dialog to ACP
// session/request_permission. Deny, a cancelled dialog, and every error path
// fail closed: the bridge blocks the tool call and the turn continues.
func (s *agentSession) handlePermissionDialog(ctx context.Context, dialog *nativeDialog, prompt pi.PermissionPrompt) {
	request := dialog.request
	ctx, finish := s.agent.observe.StartPermission(ctx, prompt.ToolName, s.permissionMode)

	answer := s.requestBoundPermissionAnswer(ctx, dialog, prompt)
	finish(observer.PermissionResult{
		Behavior: answer,
		Mode:     s.permissionMode,
		ToolName: prompt.ToolName,
	})

	var response pi.UIResponse
	if answer == string(permissionOptionAllow) {
		response = pi.UIValueResponse(request.ID, pi.PermissionOptionAllow)
	} else {
		response = pi.UIValueResponse(request.ID, pi.PermissionOptionDeny)
	}

	dialog.answer(ctx, s, response)
}

func (s *agentSession) requestBoundPermissionAnswer(ctx context.Context, dialog *nativeDialog, prompt pi.PermissionPrompt) string {
	request := dialog.request

	if strings.TrimSpace(prompt.ToolCallID) == "" {
		return string(permissionOptionDeny)
	}

	conn := s.agent.connection()
	if conn == nil {
		return string(permissionOptionDeny)
	}

	dialogCtx, finishDialog := s.registerDialog(ctx, request.ID)
	defer finishDialog()

	// UI requests and native tool events arrive on independent channels. Hold
	// the exact-ID tool lock from the claim through the permission call:
	// a native start or terminal can neither overtake pending publication nor
	// invalidate the call before the ACP client admits the request.
	state := s.lockToolCallState(prompt.ToolCallID)
	defer state.mu.Unlock()

	turnNonce, active := s.permissionTurnNonce(dialogCtx)
	if !active {
		return string(permissionOptionDeny)
	}

	if state.permissionRequested || state.terminalPublished {
		return string(permissionOptionDeny)
	}

	state.permissionRequested = true

	title := prompt.ToolName

	kind := toolKindForName(prompt.ToolName)
	if !state.published {
		startOpts := []acp.ToolCallStartOpt{
			acp.WithStartKind(kind),
			acp.WithStartStatus(acp.ToolCallStatusPending),
		}
		if len(prompt.Input) > 0 {
			startOpts = append(startOpts, acp.WithStartRawInput(prompt.Input))
		}

		pendingCtx := withTurnRoute(dialogCtx, turnNonce)
		if err := s.emitUpdates(pendingCtx, []acp.SessionUpdate{
			acp.StartToolCall(acp.ToolCallId(prompt.ToolCallID), prompt.ToolName, startOpts...),
		}); err != nil {
			s.agent.log.DebugContext(ctx, "publish pending permission tool call failed closed",
				slog.String(acpFieldSessionID, string(s.id)),
			)

			return string(permissionOptionDeny)
		}

		state.published = true
		state.status = acp.ToolCallStatusPending
	}

	status := state.status

	toolCall := acp.ToolCallUpdate{
		ToolCallId: acp.ToolCallId(prompt.ToolCallID),
		Title:      &title,
		Kind:       &kind,
		Status:     &status,
	}
	if len(prompt.Input) > 0 {
		toolCall.RawInput = prompt.Input
	}

	resp, err := s.requestAnnouncedPermission(dialogCtx, conn, acp.RequestPermissionRequest{
		SessionId: s.id,
		ToolCall:  toolCall,
		Options: []acp.PermissionOption{
			{OptionId: permissionOptionAllow, Name: "Allow", Kind: acp.PermissionOptionKindAllowOnce},
			{OptionId: permissionOptionDeny, Name: "Deny", Kind: acp.PermissionOptionKindRejectOnce},
		},
	}, dialogActionBinding{outbox: dialog.outbox, failNative: func() {
		dialog.answer(context.WithoutCancel(ctx), s, pi.UIValueResponse(request.ID, pi.PermissionOptionDeny))
	}})
	if err != nil {
		s.agent.log.DebugContext(ctx, "permission request failed closed",
			slog.String(acpFieldSessionID, string(s.id)),
		)

		return string(permissionOptionDeny)
	}

	if resp.Outcome.Selected == nil {
		return string(permissionOptionDeny)
	}

	if resp.Outcome.Selected.OptionId == permissionOptionAllow {
		return string(permissionOptionAllow)
	}

	return string(permissionOptionDeny)
}

// permissionTurnNonce captures the active prompt route for a bridge dialog.
// The dialog context and session must name the same active turn. Cancellation,
// missing routes, teardown, and stale dialog goroutines all fail closed instead
// of being rebound to a later turn.
func (s *agentSession) permissionTurnNonce(ctx context.Context) (string, bool) {
	if ctx.Err() != nil {
		return "", false
	}

	contextNonce := turnNonceFromContext(ctx)

	s.mu.Lock()
	activeNonce := s.turnNonce
	active := s.cancel != nil && activeNonce != ""
	s.mu.Unlock()

	if !active || contextNonce == "" || contextNonce != activeNonce {
		return "", false
	}

	return activeNonce, true
}

// respondExactUIDialog writes one answer to the client captured from the
// generation that emitted it. There is intentionally no current-client
// fallback: a successor must never receive an ancestor's dialog response.
func (s *agentSession) respondExactUIDialog(
	ctx context.Context,
	outbox *sessionOutbox,
	client piClient,
	response pi.UIResponse,
) {
	if client == nil || outbox == nil || outbox.client != client {
		return
	}

	responseCtx, cancelResponse := context.WithTimeout(ctx, sessionInterruptTimeout)
	defer cancelResponse()

	if err := outbox.dispatchMu.lock(responseCtx); err != nil {
		return
	}
	defer outbox.dispatchMu.Unlock()

	outbox.mu.Lock()
	current := outbox.client == client && !outbox.ended && !outbox.fenced
	outbox.mu.Unlock()

	if !current {
		return
	}

	if err := client.RespondUI(response); err != nil {
		s.agent.log.DebugContext(ctx, "respond to pi UI dialog failed",
			slog.String(acpFieldSessionID, string(s.id)),
		)
	}
}

// respondExactClientUIDialog resolves the generation owner by exact client
// identity for deferred provider-auth replies whose flow retained the emitter
// but not the outbox pointer. A client is construction-owned by one generation
// and is never reused by a successor.
func (s *agentSession) respondExactClientUIDialog(
	ctx context.Context,
	client piClient,
	response pi.UIResponse,
) {
	if client == nil {
		return
	}

	s.mu.Lock()

	var exact *sessionOutbox
	if s.outbox != nil && s.outbox.client == client {
		exact = s.outbox
	} else {
		for _, retained := range s.containmentOutboxes {
			if retained != nil && retained.client == client {
				exact = retained

				break
			}
		}
	}
	s.mu.Unlock()

	s.respondExactUIDialog(ctx, exact, client, response)
}

func toolKindForName(toolName string) acp.ToolKind {
	switch toolName {
	case toolNameRead:
		return acp.ToolKindRead
	case toolNameEdit, toolNameWrite:
		return acp.ToolKindEdit
	case toolNameBash:
		return acp.ToolKindExecute
	case toolNameGrep, toolNameFind, toolNameGlob, toolNameLs:
		return acp.ToolKindSearch
	case toolNameFetch, toolNameWebFetch:
		return acp.ToolKindFetch
	default:
		return acp.ToolKindOther
	}
}
