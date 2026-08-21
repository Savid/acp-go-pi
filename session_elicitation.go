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
	elicitationFieldChoice    = "choice"
	elicitationFieldConfirmed = "confirmed"
	elicitationFieldValue     = "value"

	elicitationModeForm = "form"
	elicitationModeURL  = "url"

	jsonSchemaTypeString  = "string"
	jsonSchemaTypeBoolean = "boolean"

	schemaFieldType = "type"
)

// handleNativeElicitationDialog relays one non-permission extension dialog
// (select without the permission marker, confirm, input, editor) as an ACP
// form elicitation. When the client does not advertise form elicitation the
// dialog fails closed with a deterministic native cancel.
func (s *agentSession) handleNativeElicitationDialog(ctx context.Context, dialog *nativeDialog) {
	request := dialog.request

	conn := s.agent.connection()
	if conn == nil || !s.agent.clientSupportsFormElicitation() {
		dialog.answer(ctx, s, pi.UICancelResponse(request.ID))

		return
	}

	ctx, finish := s.agent.observe.StartElicitation(ctx)

	response, accepted := s.createBoundDialogElicitation(ctx, conn, dialog)
	finish(observer.ElicitationResult{Accepted: accepted})

	dialog.answer(ctx, s, response)
}

func (s *agentSession) createBoundDialogElicitation(
	ctx context.Context,
	conn agentClient,
	dialog *nativeDialog,
) (pi.UIResponse, bool) {
	request := dialog.request

	dialogCtx, finishDialog := s.registerDialog(ctx, request.ID)
	defer finishDialog()

	resp, err := s.requestAnnouncedElicitation(dialogCtx, conn, acp.UnstableCreateElicitationRequest{
		Form: &acp.UnstableCreateElicitationForm{
			Message:         elicitationMessage(request),
			Mode:            elicitationModeForm,
			RequestedSchema: elicitationSchemaForDialog(request),
		},
	}, elicitationScope{SessionID: s.id, TurnNonce: turnNonceFromContext(ctx), RequestID: request.ID}, dialogActionBinding{
		outbox: dialog.outbox,
		failNative: func() {
			dialog.answer(context.WithoutCancel(ctx), s, pi.UICancelResponse(request.ID))
		},
	})
	if err != nil {
		s.agent.log.DebugContext(ctx, "elicitation request failed closed",
			slog.String(acpFieldSessionID, string(s.id)),
		)

		return pi.UICancelResponse(request.ID), false
	}

	if resp.Accept == nil {
		return pi.UICancelResponse(request.ID), false
	}

	return dialogAnswer(request, resp.Accept.Content)
}

// dialogAnswer converts an accepted elicitation form into the native dialog
// response; an answer that does not fit the dialog dismisses it.
func dialogAnswer(request pi.UIRequest, content map[string]any) (pi.UIResponse, bool) {
	switch request.Method {
	case uiMethodSelect:
		choice, _ := content[elicitationFieldChoice].(string)
		if choice == "" || !containsOption(request.Options, choice) {
			return pi.UICancelResponse(request.ID), false
		}

		return pi.UIValueResponse(request.ID, choice), true
	case uiMethodConfirm:
		confirmed, ok := content[elicitationFieldConfirmed].(bool)
		if !ok {
			return pi.UICancelResponse(request.ID), false
		}

		return pi.UIConfirmResponse(request.ID, confirmed), true
	case uiMethodInput, uiMethodEditor:
		value, ok := content[elicitationFieldValue].(string)
		if !ok {
			return pi.UICancelResponse(request.ID), false
		}

		return pi.UIValueResponse(request.ID, value), true
	default:
		return pi.UICancelResponse(request.ID), false
	}
}

func containsOption(options []string, value string) bool {
	for _, option := range options {
		if option == value {
			return true
		}
	}

	return false
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

func elicitationSchemaForDialog(request pi.UIRequest) acp.UnstableElicitationSchema {
	schema := acp.UnstableElicitationSchema{
		Properties: map[string]any{},
		Type:       acp.UnstableElicitationSchemaTypeObject,
	}

	switch request.Method {
	case uiMethodSelect:
		oneOf := make([]map[string]any, 0, len(request.Options))
		for _, option := range request.Options {
			oneOf = append(oneOf, map[string]any{
				"const": option,
				"title": option,
			})
		}

		schema.Properties[elicitationFieldChoice] = map[string]any{
			schemaFieldType: jsonSchemaTypeString,
			"oneOf":         oneOf,
		}
		schema.Required = []string{elicitationFieldChoice}
	case uiMethodConfirm:
		schema.Properties[elicitationFieldConfirmed] = map[string]any{
			schemaFieldType: jsonSchemaTypeBoolean,
		}
		schema.Required = []string{elicitationFieldConfirmed}
	default:
		property := map[string]any{
			schemaFieldType: jsonSchemaTypeString,
		}
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
