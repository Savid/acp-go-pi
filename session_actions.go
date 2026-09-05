package piacp

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/coder/acp-go-sdk"

	"github.com/savid/acp-go-pi/internal/lifecycle"
)

type actionRequestWriteAckKey struct{}

var sessionActionRequestTimeout = 30 * time.Second

var errLifecycleActionRequest = errors.New("lifecycle action request failed")

var errSessionInteractionClosed = errors.New("session interaction boundary closed")

type dialogActionBinding struct {
	outbox     *sessionOutbox
	failNative func()
}

func actionRequestWriteAck(ctx context.Context) func(error) {
	ack, _ := ctx.Value(actionRequestWriteAckKey{}).(func(error))

	return ack
}

func acknowledgeActionRequestWrite(ctx context.Context, err error) {
	if ack := actionRequestWriteAck(ctx); ack != nil {
		ack(err)
	}
}

// announcedBoundActionRequest sends one client request that holds native work until
// it is answered, announces the ordered action that request answers, and
// resolves that action exactly once.
//
// The ordering is contract, not convenience: the request carrying the action
// identity is on the wire before the `action_update` that announces it, so a
// host can never see an action id it cannot yet answer. Where the extension is
// not negotiated, the request is sent plainly and no action exists to announce.
//
//nolint:gocyclo // The request/write/announce/answer ladder is deliberately linear and fail-closed.
func announcedBoundActionRequest[T any](
	ctx context.Context,
	session *agentSession,
	kind lifecycle.ActionKind,
	send func(context.Context, map[string]any) (T, error),
	resolved func(T, error) lifecycle.ActionState,
	outbox *sessionOutbox,
	failNative func(),
) (T, error) {
	action, announceable, prepareErr := session.prepareLifecycleActionFor(outbox)
	if prepareErr != nil {
		if failNative != nil {
			failNative()
		}

		if outbox != nil {
			prepareErr = errors.Join(prepareErr, session.containGenerationSync(
				ctx, outbox, poisoned(poisonCauseActionRequest, "the lifecycle action request had no live owner"),
			))
		}

		var zero T

		return zero, prepareErr
	}

	if !announceable {
		return send(ctx, nil)
	}

	if err := ctx.Err(); err != nil {
		if failNative != nil {
			failNative()
		}

		var zero T

		return zero, errLifecycleActionRequest
	}

	type answer struct {
		value T
		err   error
	}

	answers := make(chan answer, 2)
	written := make(chan error, 1)
	requestCtx, cancelRequest := context.WithTimeout(ctx, sessionActionRequestTimeout)

	var acknowledgeOnce sync.Once

	acknowledge := func(err error) {
		acknowledgeOnce.Do(func() {
			select {
			case written <- err:
			default:
			}
		})
	}
	requestCtx = context.WithValue(requestCtx, actionRequestWriteAckKey{}, func(err error) {
		acknowledge(err)
	})

	defer cancelRequest()

	// One token covers the coordinator itself and one covers the transport
	// sender. Close therefore cannot begin its action join between a sender's
	// return and the coordinator's failure containment.
	if action.outbox == nil {
		if failNative != nil {
			failNative()
		}

		var zero T

		return zero, errLifecycleActionUnowned
	}

	coordinatorDone, senderDone, admitted := session.admitActionHandlers(action.outbox)
	if !admitted {
		if failNative != nil {
			failNative()
		}

		var zero T

		return zero, errLifecycleActionRequest
	}

	defer coordinatorDone()

	go func() {
		defer senderDone()

		answered := answer{}

		defer func() {
			if recover() != nil {
				answered.err = errLifecycleActionRequest
			}

			acknowledge(answered.err)

			answers <- answered
		}()

		answered.value, answered.err = send(
			requestCtx,
			lifecycleActionMeta(action.streamID, action.actionID, action.owner),
		)
	}()

	fail := func() (T, error) {
		cancelRequest()
		acknowledge(errLifecycleActionRequest)

		select {
		case answers <- answer{err: errLifecycleActionRequest}:
		default:
		}

		if failNative != nil {
			failNative()
		}

		if errors.Is(context.Cause(ctx), errSessionInteractionClosed) {
			resolveErr := session.lifecycleResolveCapturedAction(
				context.WithoutCancel(ctx), action.outbox, action.actionID, lifecycle.ActionCancelled,
			)

			var zero T

			return zero, errors.Join(errLifecycleActionRequest, errSessionInteractionClosed, resolveErr)
		}

		containmentErr := session.containGenerationSync(
			context.WithoutCancel(ctx),
			action.outbox,
			poisoned(poisonCauseActionRequest, "the lifecycle action request failed"),
		)

		var zero T

		return zero, errors.Join(errLifecycleActionRequest, containmentErr)
	}

	var writeErr error
	select {
	case writeErr = <-written:
	case <-requestCtx.Done():
		return fail()
	}

	if writeErr != nil {
		select {
		case answered := <-answers:
			if errors.Is(writeErr, errLifecycleActionRequest) || errors.Is(answered.err, errLifecycleActionRequest) {
				return fail()
			}

			return answered.value, errors.Join(writeErr, answered.err)
		case <-requestCtx.Done():
			return fail()
		}
	}

	announceErr := runLifecycleActionAnnouncement(requestCtx, session, action, kind)
	if announceErr != nil {
		// The host request is already addressable on the wire. Revoke that exact
		// request immediately, retire any blocker inserted by a partial
		// announcement, and contain the generation that owned it. Waiting for a
		// held host answer here would wedge both the native dialog and teardown.
		cancelRequest()

		if failNative != nil {
			failNative()
		}

		if quarantineErr := session.lifecycleGenerationQuarantine(action.generation); quarantineErr != nil {
			var zero T

			return zero, errors.Join(errLifecycleActionAnnouncement, announceErr, quarantineErr)
		}

		containmentErr := session.containGenerationSync(
			context.WithoutCancel(ctx),
			action.outbox,
			poisoned(poisonCauseActionRequest, "the lifecycle action announcement failed"),
		)
		// Only a returned (therefore no longer lcMu-owning) announcement may be
		// fenced here. A timeout or panic leaves the hook quarantined; trying to
		// acquire lcMu behind it would make close unbounded. Incomplete native
		// containment likewise permits no lifecycle mutation.
		if nativeContainmentComplete(containmentErr) &&
			!errors.Is(announceErr, ErrContainmentIncomplete) {
			session.revokeLifecycleAction(action)
		}

		var zero T

		return zero, errors.Join(errLifecycleActionAnnouncement, announceErr, containmentErr)
	}

	var answered answer
	select {
	case answered = <-answers:
	case <-requestCtx.Done():
		return fail()
	}

	if errors.Is(answered.err, errLifecycleActionRequest) {
		return fail()
	}

	resolveErr := session.lifecycleResolveCapturedAction(
		ctx,
		action.outbox,
		action.actionID,
		resolved(answered.value, answered.err),
	)

	return answered.value, errors.Join(answered.err, resolveErr)
}

func runLifecycleActionAnnouncement(
	ctx context.Context,
	session *agentSession,
	action pendingAction,
	kind lifecycle.ActionKind,
) (err error) {
	defer func() {
		if recover() != nil {
			err = generationContainmentPanicError("lifecycle action announcement")
		}
	}()

	return session.announceLifecycleAction(ctx, action, kind)
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
	binding dialogActionBinding,
) (acp.RequestPermissionResponse, error) {
	return announcedBoundActionRequest(ctx, s, lifecycle.ActionPermission, func(requestCtx context.Context, meta map[string]any) (acp.RequestPermissionResponse, error) {
		request.Meta = meta

		return conn.RequestPermission(requestCtx, request)
	}, permissionActionState, binding.outbox, binding.failNative)
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
	binding dialogActionBinding,
) (acp.UnstableCreateElicitationResponse, error) {
	return announcedBoundActionRequest(ctx, s, lifecycle.ActionElicitation, func(requestCtx context.Context, meta map[string]any) (acp.UnstableCreateElicitationResponse, error) {
		if meta != nil {
			switch {
			case request.Form != nil:
				request.Form.Meta = meta
			case request.Url != nil:
				request.Url.Meta = meta
			}
		}

		return conn.CreateElicitation(requestCtx, request, scope)
	}, elicitationActionState, binding.outbox, binding.failNative)
}
