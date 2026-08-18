package piacp

import (
	"context"
	"errors"

	"github.com/coder/acp-go-sdk"

	"github.com/savid/acp-go-pi/internal/lifecycle"
)

type actionRequestWriteAckKey struct{}

func actionRequestWriteAck(ctx context.Context) func(error) {
	ack, _ := ctx.Value(actionRequestWriteAckKey{}).(func(error))

	return ack
}

func acknowledgeActionRequestWrite(ctx context.Context, err error) {
	if ack := actionRequestWriteAck(ctx); ack != nil {
		ack(err)
	}
}

// announcedActionRequest sends one client request that holds native work until
// it is answered, announces the ordered action that request answers, and
// resolves that action exactly once.
//
// The ordering is contract, not convenience: the request carrying the action
// identity is on the wire before the `action_update` that announces it, so a
// host can never see an action id it cannot yet answer. Where the extension is
// not negotiated, or no turn owns the request, the request is sent plainly and
// no action exists to announce.
func announcedActionRequest[T any](
	ctx context.Context,
	session *agentSession,
	kind lifecycle.ActionKind,
	send func(context.Context, map[string]any) (T, error),
	resolved func(T, error) lifecycle.ActionState,
) (T, error) {
	action, announceable, prepareErr := session.prepareLifecycleAction()
	if prepareErr != nil {
		var zero T

		return zero, prepareErr
	}

	if !announceable {
		return send(ctx, nil)
	}

	type answer struct {
		value T
		err   error
	}

	answers := make(chan answer, 1)
	written := make(chan error, 1)
	requestCtx := context.WithValue(ctx, actionRequestWriteAckKey{}, func(err error) {
		select {
		case written <- err:
		default:
		}
	})

	go func() {
		value, err := send(requestCtx, lifecycleActionMeta(action.streamID, action.actionID, action.owner))
		acknowledgeActionRequestWrite(requestCtx, err)

		answers <- answer{value: value, err: err}
	}()

	if writeErr := <-written; writeErr != nil {
		answered := <-answers

		return answered.value, errors.Join(writeErr, answered.err)
	}

	announceErr := session.announceLifecycleAction(ctx, action, kind)
	answered := <-answers

	if announceErr != nil {
		return answered.value, errors.Join(announceErr, answered.err)
	}

	resolveErr := session.lifecycleResolveAction(ctx, action.actionID, resolved(answered.value, answered.err))

	return answered.value, errors.Join(answered.err, resolveErr)
}

// permissionActionState records how a permission request was answered. A
// connection failure is a failed action rather than a declined one: nobody
// declined it.
func permissionActionState(resp acp.RequestPermissionResponse, err error) lifecycle.ActionState {
	switch {
	case err != nil:
		return lifecycle.ActionFailed
	case resp.Outcome.Cancelled != nil:
		return lifecycle.ActionCancelled
	case resp.Outcome.Selected == nil:
		return lifecycle.ActionCancelled
	case resp.Outcome.Selected.OptionId == permissionOptionAllow:
		return lifecycle.ActionAccepted
	default:
		return lifecycle.ActionDeclined
	}
}

// elicitationActionState records how an elicitation was answered, keeping the
// three ACP actions distinct rather than collapsing them into one refusal.
func elicitationActionState(resp acp.UnstableCreateElicitationResponse, err error) lifecycle.ActionState {
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
}

// requestAnnouncedPermission asks the client for a tool-call permission as an
// ordered action.
func (s *agentSession) requestAnnouncedPermission(
	ctx context.Context,
	conn agentClient,
	request acp.RequestPermissionRequest,
) (acp.RequestPermissionResponse, error) {
	return announcedActionRequest(ctx, s, lifecycle.ActionPermission, func(requestCtx context.Context, meta map[string]any) (acp.RequestPermissionResponse, error) {
		request.Meta = meta

		return conn.RequestPermission(requestCtx, request)
	}, permissionActionState)
}

// requestAnnouncedElicitation asks the client a non-permission question as an
// ordered action. The turn-scoped route envelope stays the routing and
// authentication envelope; the action correlation adds a stable lifecycle name
// for the same pending request without replacing it.
func (s *agentSession) requestAnnouncedElicitation(
	ctx context.Context,
	conn agentClient,
	request acp.UnstableCreateElicitationRequest,
	scope elicitationScope,
) (acp.UnstableCreateElicitationResponse, error) {
	return announcedActionRequest(ctx, s, lifecycle.ActionElicitation, func(requestCtx context.Context, meta map[string]any) (acp.UnstableCreateElicitationResponse, error) {
		if meta != nil {
			switch {
			case request.Form != nil:
				request.Form.Meta = meta
			case request.Url != nil:
				request.Url.Meta = meta
			}
		}

		return conn.CreateElicitation(requestCtx, request, scope)
	}, elicitationActionState)
}
