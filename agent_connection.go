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
	TurnNonce  string
	ToolCallID acp.ToolCallId
	RequestID  string
}

func scopedElicitationParams(
	params acp.UnstableCreateElicitationRequest,
	scope elicitationScope,
) (json.RawMessage, error) {
	var (
		payload map[string]any
		meta    map[string]any
	)

	switch {
	case params.Form != nil:
		payload = map[string]any{
			jsonFieldMessage:  params.Form.Message,
			jsonFieldMode:     params.Form.Mode,
			"requestedSchema": params.Form.RequestedSchema,
		}
		meta = params.Form.Meta
	case params.Url != nil:
		payload = map[string]any{
			"elicitationId":  params.Url.ElicitationId,
			jsonFieldMessage: params.Url.Message,
			jsonFieldMode:    params.Url.Mode,
			jsonFieldURL:     params.Url.Url,
		}
		meta = params.Url.Meta
	default:
		return nil, errors.New("elicitation request must include form or url")
	}

	stamped, err := stampRouteMeta(meta, scope)
	if err != nil {
		return nil, err
	}

	payload["_meta"] = stamped

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
