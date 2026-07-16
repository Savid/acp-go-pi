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

// handleUIDialog answers one blocking extension UI dialog. A dialog whose
// select title carries the bridge permission marker is a tool-call permission
// request; every other dialog is a non-permission question relayed as
// elicitation. Exactly one UIResponse is always written back so the bridge
// extension never hangs.
func (s *agentSession) handleUIDialog(ctx context.Context, request pi.UIRequest) {
	if strings.HasPrefix(request.Title, pi.PermissionTitleMarker) {
		prompt, ok := pi.ParsePermissionTitle(request.Title)
		if request.Method != uiMethodSelect || !ok {
			s.respondUIDialog(ctx, pi.UICancelResponse(request.ID))

			return
		}

		s.handlePermissionDialog(ctx, request, prompt)

		return
	}

	s.handleElicitationDialog(ctx, request)
}

// registerDialog wraps ctx in a tracked cancellable context so that
// session/cancel and teardown can resolve a pending dialog as cancelled
// instead of leaving pi's extension blocked.
func (s *agentSession) registerDialog(ctx context.Context, id string) (context.Context, func()) {
	dialogCtx, cancel := context.WithCancel(ctx)
	entry := &dialogCancel{cancel: cancel}

	s.mu.Lock()
	if s.pendingDialogs == nil {
		s.pendingDialogs = make(map[string]*dialogCancel)
	}

	s.pendingDialogs[id] = entry
	turnCancelled := s.turnCancelled
	s.mu.Unlock()

	if turnCancelled {
		cancel()
	}

	return dialogCtx, func() {
		s.mu.Lock()
		if s.pendingDialogs[id] == entry {
			delete(s.pendingDialogs, id)
		}
		s.mu.Unlock()

		cancel()
	}
}

// handlePermissionDialog maps one bridge permission dialog to ACP
// session/request_permission. Deny, a cancelled dialog, and every error path
// fail closed: the bridge blocks the tool call and the turn continues.
func (s *agentSession) handlePermissionDialog(ctx context.Context, request pi.UIRequest, prompt pi.PermissionPrompt) {
	ctx, finish := s.agent.observe.StartPermission(ctx, prompt.ToolName, s.permissionMode)

	answer := s.requestPermissionAnswer(ctx, request, prompt)
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

	s.respondUIDialog(ctx, response)
}

func (s *agentSession) requestPermissionAnswer(ctx context.Context, request pi.UIRequest, prompt pi.PermissionPrompt) string {
	if strings.TrimSpace(prompt.ToolCallID) == "" {
		return string(permissionOptionDeny)
	}

	conn := s.agent.connection()
	if conn == nil {
		return string(permissionOptionDeny)
	}

	dialogCtx, finishDialog := s.registerDialog(ctx, request.ID)
	defer finishDialog()

	title := prompt.ToolName
	kind := toolKindForName(prompt.ToolName)
	status := acp.ToolCallStatusPending

	toolCall := acp.ToolCallUpdate{
		ToolCallId: acp.ToolCallId(prompt.ToolCallID),
		Title:      &title,
		Kind:       &kind,
		Status:     &status,
	}
	if len(prompt.Input) > 0 {
		toolCall.RawInput = prompt.Input
	}

	startOpts := []acp.ToolCallStartOpt{
		acp.WithStartKind(kind),
		acp.WithStartStatus(acp.ToolCallStatusPending),
	}
	if len(prompt.Input) > 0 {
		startOpts = append(startOpts, acp.WithStartRawInput(prompt.Input))
	}

	// Pi raises the bridge dialog before it emits tool_execution_start. The
	// ACP client must already know this exact native tool-call id before it
	// can authorize it, so publish the pending call before requesting
	// permission. The later native start transitions this same call to
	// in_progress instead of publishing a second start.
	if err := s.emitUpdates(dialogCtx, []acp.SessionUpdate{
		acp.StartToolCall(acp.ToolCallId(prompt.ToolCallID), prompt.ToolName, startOpts...),
	}); err != nil {
		s.agent.log.DebugContext(ctx, "publish pending permission tool call failed closed",
			slog.String(acpFieldSessionID, string(s.id)),
			slog.String(jsonFieldError, err.Error()),
		)

		return string(permissionOptionDeny)
	}

	s.mu.Lock()
	if s.permissionTools == nil {
		s.permissionTools = make(map[string]struct{})
	}

	s.permissionTools[prompt.ToolCallID] = struct{}{}
	s.mu.Unlock()

	resp, err := conn.RequestPermission(dialogCtx, acp.RequestPermissionRequest{
		SessionId: s.id,
		ToolCall:  toolCall,
		Options: []acp.PermissionOption{
			{OptionId: permissionOptionAllow, Name: "Allow", Kind: acp.PermissionOptionKindAllowOnce},
			{OptionId: permissionOptionDeny, Name: "Deny", Kind: acp.PermissionOptionKindRejectOnce},
		},
	})
	if err != nil {
		s.agent.log.DebugContext(ctx, "permission request failed closed",
			slog.String(acpFieldSessionID, string(s.id)),
			slog.String(jsonFieldError, err.Error()),
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

// beginPermissionTool reports whether a bridge dialog already published the
// pending ACP tool call for this native id. Each marker is consumed exactly
// once by tool_execution_start; turn teardown clears any marker left behind
// when cancellation or transport failure prevents the native start event.
func (s *agentSession) beginPermissionTool(toolCallID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.permissionTools[toolCallID]; !ok {
		return false
	}

	delete(s.permissionTools, toolCallID)

	return true
}

// respondUIDialog writes one dialog answer back to pi; a write failure is
// logged, never fatal (the transport failure surfaces on the prompt path).
func (s *agentSession) respondUIDialog(ctx context.Context, response pi.UIResponse) {
	client := s.currentClient()
	if client == nil {
		return
	}

	if err := client.RespondUI(response); err != nil {
		s.agent.log.DebugContext(ctx, "respond to pi UI dialog failed",
			slog.String(acpFieldSessionID, string(s.id)),
			slog.String(jsonFieldError, err.Error()),
		)
	}
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
