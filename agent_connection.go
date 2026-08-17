package piacp

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"

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

// requestError maps a handler error onto the wire. An honored $/cancel_request
// is the only thing that cancels a request context with cause
// context.Canceled: connection teardown cancels the parent with the transport
// error, and an adapter deadline yields context.DeadlineExceeded. So that
// cause — not errors.Is on the returned error — is what identifies a cancel,
// and it is answered before any embedded RequestError, because a cancelled
// request must not serialize as whatever error it happened to be carrying.
// Everything else that is not already a contracted error is an adapter fault:
// the client gets the closed internal error and the operator gets the detail
// in the log.
func requestError(ctx context.Context, log *slog.Logger, err error) *acp.RequestError {
	if err == nil {
		return nil
	}

	if context.Cause(ctx) == context.Canceled {
		return acp.NewRequestCancelled(nil)
	}

	var reqErr *acp.RequestError
	if errors.As(err, &reqErr) {
		return reqErr
	}

	log.ErrorContext(ctx, "acp request failed", slog.Any("error", err))

	return acp.NewInternalError(nil)
}
