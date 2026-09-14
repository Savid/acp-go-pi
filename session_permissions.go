package piacp

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"strings"

	"github.com/coder/acp-go-sdk"

	"github.com/savid/acp-go-core/lifecycle"
	"github.com/savid/acp-go-core/observer"
	"github.com/savid/acp-go-pi/internal/pi"
)

const (
	permissionOptionAllow acp.PermissionOptionId = "allow"
	permissionOptionDeny  acp.PermissionOptionId = "deny"

	uiMethodSelect  = "select"
	uiMethodConfirm = "confirm"
	uiMethodInput   = "input"
	uiMethodEditor  = "editor"

	elicitationFieldChoice    = "choice"
	elicitationFieldConfirmed = "confirmed"
	elicitationFieldValue     = "value"

	schemaFieldType  = "type"
	schemaFieldTitle = "title"
)

// handleUIRequest routes one extension UI request. Dialogs run on their own
// goroutine so the pump keeps draining; fire-and-forget methods are ignored.
func (s *session) handleUIRequest(rt *runtime, request pi.UIRequest) {
	if !request.IsDialog() {
		return
	}

	s.handleDialog(rt, request)
}

// handleDialog answers one blocking dialog. A select whose title carries the
// permission marker is a tool-call permission; every other dialog is a
// question relayed as elicitation. Exactly one response reaches pi.
func (s *session) handleDialog(rt *runtime, request pi.UIRequest) {
	var c *cycle

	s.mu.Lock()
	t := s.turn

	switch {
	case s.turn != nil:
		c = &s.turn.cycle
	case s.cycle != nil:
		c = s.cycle
	}

	closing := s.closing
	s.mu.Unlock()

	ctx, cancel := context.WithCancelCause(context.Background())
	unregister := s.registerDialog(request.ID, cancel)

	if c == nil || closing || ctx.Err() != nil {
		cancel(nil)
		unregister()
		s.respond(rt, pi.UICancelResponse(request.ID))

		return
	}

	if t != nil {
		s.acceptTurn(ctx, t)
	}

	if strings.HasPrefix(request.Title, pi.PermissionTitleMarker) {
		prompt, ok := pi.ParsePermissionTitle(request.Title)
		if request.Method != uiMethodSelect || !ok || s.publishPendingTool(ctx, &c.state, prompt) != nil {
			cancel(nil)
			unregister()
			s.respond(rt, pi.UICancelResponse(request.ID))

			return
		}
	}

	go func() {
		defer cancel(nil)
		defer unregister()

		if strings.HasPrefix(request.Title, pi.PermissionTitleMarker) {
			prompt, _ := pi.ParsePermissionTitle(request.Title)
			s.respond(rt, s.permissionDialog(ctx, c, request, prompt))

			return
		}

		s.respond(rt, s.elicitationDialog(ctx, c, request))
	}()
}

func (s *session) respond(rt *runtime, response pi.UIResponse) {
	if err := rt.client.RespondUI(response); err != nil {
		s.agent.log.DebugContext(context.Background(), "respond to pi dialog failed", slog.String("session_id", string(s.id)))
	}
}

// permissionDialog maps one bridge permission dialog to session/request_permission.
// Deny, a cancelled dialog, and every failure fail closed.
func (s *session) permissionDialog(ctx context.Context, c *cycle, request pi.UIRequest, prompt pi.PermissionPrompt) pi.UIResponse {
	deny := pi.UIValueResponse(request.ID, pi.PermissionOptionDeny)

	ctx, finish := s.agent.observe.StartPermission(ctx, prompt.ToolName, s.permissionMode())

	answer := s.requestPermission(ctx, c, prompt)
	finish(observer.PermissionResult{Behavior: string(answer), Mode: s.permissionMode(), ToolName: prompt.ToolName})

	if answer == permissionOptionAllow {
		return pi.UIValueResponse(request.ID, pi.PermissionOptionAllow)
	}

	return deny
}

func (s *session) requestPermission(ctx context.Context, c *cycle, prompt pi.PermissionPrompt) acp.PermissionOptionId {
	conn := s.agent.connection()
	if conn == nil {
		return permissionOptionDeny
	}

	kind := toolKindForName(prompt.ToolName)
	status := acp.ToolCallStatusPending
	title := prompt.ToolName

	toolCall := acp.ToolCallUpdate{
		ToolCallId: acp.ToolCallId(prompt.ToolCallID),
		Title:      &title,
		Kind:       &kind,
		Status:     &status,
	}
	if len(prompt.Input) > 0 {
		toolCall.RawInput = prompt.Input
	}

	resp, err := announcedRequest(ctx, s, c, lifecycle.ActionPermission,
		func(requestCtx context.Context, meta map[string]any) (acp.RequestPermissionResponse, error) {
			return conn.RequestPermission(requestCtx, acp.RequestPermissionRequest{
				Meta:      meta,
				SessionId: s.id,
				ToolCall:  toolCall,
				Options: []acp.PermissionOption{
					{OptionId: permissionOptionAllow, Name: "Allow", Kind: acp.PermissionOptionKindAllowOnce},
					{OptionId: permissionOptionDeny, Name: "Deny", Kind: acp.PermissionOptionKindRejectOnce},
				},
			})
		},
		func(resp acp.RequestPermissionResponse, err error) lifecycle.ActionState {
			switch {
			case err != nil:
				return lifecycle.ActionFailed
			case resp.Outcome.Selected == nil:
				return lifecycle.ActionCancelled
			case resp.Outcome.Selected.OptionId == permissionOptionAllow:
				return lifecycle.ActionAccepted
			default:
				return lifecycle.ActionDeclined
			}
		})
	if err != nil || resp.Outcome.Selected == nil || resp.Outcome.Selected.OptionId != permissionOptionAllow {
		return permissionOptionDeny
	}

	return permissionOptionAllow
}

// elicitationDialog relays one non-permission dialog as a form elicitation.
// Without form support on the client the dialog is cancelled natively.
func (s *session) elicitationDialog(ctx context.Context, c *cycle, request pi.UIRequest) pi.UIResponse {
	conn := s.agent.connection()
	if conn == nil || !s.agent.clientSupportsFormElicitation() {
		return pi.UICancelResponse(request.ID)
	}

	ctx, finish := s.agent.observe.StartElicitation(ctx)

	resp, err := announcedRequest(ctx, s, c, lifecycle.ActionElicitation,
		func(requestCtx context.Context, meta map[string]any) (acp.UnstableCreateElicitationResponse, error) {
			return conn.UnstableCreateElicitation(requestCtx, acp.UnstableCreateElicitationRequest{
				Form: &acp.UnstableCreateElicitationForm{
					Meta:            meta,
					Message:         elicitationMessage(request),
					Mode:            "form",
					RequestedSchema: elicitationSchema(request),
				},
			})
		},
		func(resp acp.UnstableCreateElicitationResponse, err error) lifecycle.ActionState {
			switch {
			case err != nil:
				return lifecycle.ActionFailed
			case resp.Accept != nil:
				return lifecycle.ActionAccepted
			case resp.Decline != nil:
				return lifecycle.ActionDeclined
			default:
				return lifecycle.ActionCancelled
			}
		})

	finish(observer.ElicitationResult{Accepted: err == nil && resp.Accept != nil, Err: err})

	if err != nil || resp.Accept == nil {
		return pi.UICancelResponse(request.ID)
	}

	return dialogAnswer(request, resp.Accept.Content)
}

// announcedRequest sends one client request that holds native work, announces
// the action it answers once the request is on the wire, and resolves that
// action exactly once.
func announcedRequest[T any](
	ctx context.Context,
	s *session,
	c *cycle,
	kind lifecycle.ActionKind,
	send func(context.Context, map[string]any) (T, error),
	resolved func(T, error) lifecycle.ActionState,
) (T, error) {
	var zero T

	releaseCall, err := s.agent.acquireClientCall()
	if err != nil {
		return zero, err
	}
	defer releaseCall()

	actionID, err := s.reserveAction(c)
	if err != nil {
		return zero, err
	}

	if actionID == "" {
		return send(ctx, nil)
	}

	type answer struct {
		value T
		err   error
	}

	answers := make(chan answer, 1)

	var written <-chan struct{}
	if t := s.agent.transportRef(); t != nil {
		written = t.AwaitRequestWrite(actionID)
	}

	go func() {
		value, err := send(ctx, s.actionCorrelation(c, actionID))
		answers <- answer{value: value, err: err}
	}()

	if written != nil {
		select {
		case <-written:
		case result := <-answers:
			answers <- result
		}
	}

	if err := s.lcActionPendingWithID(ctx, c, actionID, kind); err != nil {
		s.agent.log.ErrorContext(ctx, "announce lifecycle action failed",
			slog.String("session_id", string(s.id)), slog.String("reason", err.Error()))
	}

	result := <-answers
	state := resolved(result.value, result.err)

	if result.err != nil && errors.Is(context.Cause(ctx), errDialogCancelled) {
		state = lifecycle.ActionCancelled
	}

	if err := s.lcActionResolved(context.WithoutCancel(ctx), c, actionID, state); err != nil {
		s.agent.log.ErrorContext(ctx, "resolve lifecycle action failed",
			slog.String("session_id", string(s.id)), slog.String("reason", err.Error()))
	}

	return result.value, result.err
}

func (s *session) reserveAction(c *cycle) (string, error) {
	s.lcMu.Lock()
	defer s.lcMu.Unlock()

	if s.lc.stream == nil || s.lc.stream.Fenced() || c.turnID == "" {
		return "", nil
	}

	return s.nextLifecycleID("action"), nil
}

// dialogAnswer converts an accepted elicitation form into the native dialog
// response; an answer that does not fit the dialog dismisses it.
func dialogAnswer(request pi.UIRequest, content map[string]any) pi.UIResponse {
	switch request.Method {
	case uiMethodSelect:
		choice, _ := content[elicitationFieldChoice].(string)
		if choice == "" || !slices.Contains(request.Options, choice) {
			return pi.UICancelResponse(request.ID)
		}

		return pi.UIValueResponse(request.ID, choice)
	case uiMethodConfirm:
		confirmed, ok := content[elicitationFieldConfirmed].(bool)
		if !ok {
			return pi.UICancelResponse(request.ID)
		}

		return pi.UIConfirmResponse(request.ID, confirmed)
	case uiMethodInput, uiMethodEditor:
		value, ok := content[elicitationFieldValue].(string)
		if !ok {
			return pi.UICancelResponse(request.ID)
		}

		return pi.UIValueResponse(request.ID, value)
	default:
		return pi.UICancelResponse(request.ID)
	}
}

func elicitationMessage(request pi.UIRequest) string {
	parts := make([]string, 0, 2)
	if strings.TrimSpace(request.Title) != "" {
		parts = append(parts, request.Title)
	}

	if strings.TrimSpace(request.Message) != "" {
		parts = append(parts, request.Message)
	}

	if len(parts) == 0 {
		return "pi needs more input."
	}

	return strings.Join(parts, "\n\n")
}

func elicitationSchema(request pi.UIRequest) acp.UnstableElicitationSchema {
	schema := acp.UnstableElicitationSchema{Properties: map[string]any{}, Type: acp.UnstableElicitationSchemaTypeObject}

	switch request.Method {
	case uiMethodSelect:
		oneOf := make([]map[string]any, 0, len(request.Options))
		for _, option := range request.Options {
			oneOf = append(oneOf, map[string]any{"const": option, schemaFieldTitle: option})
		}

		schema.Properties[elicitationFieldChoice] = map[string]any{schemaFieldType: "string", "oneOf": oneOf}
		schema.Required = []string{elicitationFieldChoice}
	case uiMethodConfirm:
		schema.Properties[elicitationFieldConfirmed] = map[string]any{schemaFieldType: "boolean"}
		schema.Required = []string{elicitationFieldConfirmed}
	default:
		property := map[string]any{schemaFieldType: "string"}
		if request.Placeholder != "" {
			property["description"] = request.Placeholder
		}

		if request.Prefill != "" {
			property["default"] = request.Prefill
		}

		schema.Properties[elicitationFieldValue] = property
		schema.Required = []string{elicitationFieldValue}
	}

	return schema
}

func toolKindForName(toolName string) acp.ToolKind {
	switch toolName {
	case "read":
		return acp.ToolKindRead
	case "edit", "write":
		return acp.ToolKindEdit
	case "bash":
		return acp.ToolKindExecute
	case "grep", "find", "glob", "ls":
		return acp.ToolKindSearch
	case "fetch", "web_fetch":
		return acp.ToolKindFetch
	default:
		return acp.ToolKindOther
	}
}
