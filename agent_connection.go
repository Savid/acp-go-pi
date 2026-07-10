package piacp

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/coder/acp-go-sdk"
)

type agentClient interface {
	Done() <-chan struct{}
	CreateElicitation(context.Context, acp.UnstableCreateElicitationRequest, elicitationScope) (acp.UnstableCreateElicitationResponse, error)
	RequestPermission(context.Context, acp.RequestPermissionRequest) (acp.RequestPermissionResponse, error)
	SessionUpdate(context.Context, acp.SessionNotification) error
	NotifyExtension(context.Context, string, any) error
}

type elicitationScope struct {
	SessionID  acp.SessionId
	ToolCallID acp.ToolCallId
}

func scopedElicitationParams(
	params acp.UnstableCreateElicitationRequest,
	scope elicitationScope,
) (json.RawMessage, error) {
	if params.Form == nil {
		return nil, errors.New("elicitation request must include form")
	}

	payload := map[string]any{
		jsonFieldMessage:  params.Form.Message,
		"mode":            params.Form.Mode,
		"requestedSchema": params.Form.RequestedSchema,
	}
	if len(params.Form.Meta) > 0 {
		payload["_meta"] = params.Form.Meta
	}

	if scope.SessionID != "" {
		payload[acpFieldSessionID] = scope.SessionID
	}

	if scope.ToolCallID != "" {
		payload["toolCallId"] = scope.ToolCallID
	}

	return json.Marshal(payload)
}

func requestError(err error) *acp.RequestError {
	if err == nil {
		return nil
	}

	var reqErr *acp.RequestError
	if errors.As(err, &reqErr) {
		return reqErr
	}

	if errors.Is(err, context.Canceled) {
		return acp.NewRequestCancelled(map[string]any{jsonFieldError: err.Error()})
	}

	return acp.NewInternalError(map[string]any{jsonFieldError: err.Error()})
}

func lifecycleMetaError(err error) error {
	var reqErr *acp.RequestError
	if errors.As(err, &reqErr) {
		return reqErr
	}

	return acp.NewInvalidParams(map[string]any{jsonFieldError: err.Error()})
}
