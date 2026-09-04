package piacp

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"

	"github.com/savid/acp-go-pi/internal/lifecycle"
	"github.com/savid/acp-go-pi/internal/pi"
)

func actionLifecycleSession(t *testing.T, authoritative bool) (*agentSession, *directAgentClient) {
	t.Helper()

	session, client := lifecycleSession(t, authoritative)
	session.outbox = newTestSessionOutbox(1)

	return session, client
}

type lifecycleActionWireWriter struct {
	lines          chan []byte
	blockMethod    string
	requestWrite   chan struct{}
	releaseRequest chan struct{}
}

func (w *lifecycleActionWireWriter) Write(data []byte) (int, error) {
	w.lines <- append([]byte(nil), data...)

	var message lifecycleActionWireMessage
	if json.Unmarshal(data, &message) == nil && message.Method == w.blockMethod {
		close(w.requestWrite)
		<-w.releaseRequest
	}

	return len(data), nil
}

type lifecycleActionWireMessage struct {
	ID     *json.RawMessage `json:"id"`
	Method string           `json:"method"`
	Params json.RawMessage  `json:"params"`
}

type heldActionClient struct {
	*recordingClient
	kind      lifecycle.ActionKind
	started   chan struct{}
	cancelled chan struct{}
}

type faultingActionClient struct {
	*directAgentClient
	mode    string
	entered chan struct{}
	release chan struct{}
}

type blockingActionAnnouncementClient struct {
	*heldActionClient
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (c *blockingActionAnnouncementClient) SessionUpdate(ctx context.Context, notification acp.SessionNotification) error {
	envelope, _ := notification.Meta[lifecycleMetaKey].(map[string]any)
	event, _ := envelope["event"].(map[string]any)
	action, _ := event["action"].(map[string]any)
	if event["type"] == string(lifecycle.EventActionUpdate) && action["state"] == string(lifecycle.ActionPending) {
		c.once.Do(func() { close(c.entered) })
		<-c.release
	}

	return c.recordingClient.SessionUpdate(ctx, notification)
}

func (c *faultingActionClient) CreateElicitation(
	ctx context.Context,
	_ acp.UnstableCreateElicitationRequest,
	_ elicitationScope,
) (acp.UnstableCreateElicitationResponse, error) {
	switch c.mode {
	case "panic before write":
		panic("action writer secret before write")
	case "panic after write":
		acknowledgeActionRequestWrite(ctx, nil)
		panic("action writer secret after write")
	case "ignore cancellation":
		acknowledgeActionRequestWrite(ctx, nil)
		close(c.entered)
		<-c.release
	}

	return acp.UnstableCreateElicitationResponse{}, nil
}

func actionFailureSession(t *testing.T, conn agentClient) (*agentSession, *stubProcess, *stubPiClient) {
	t.Helper()

	agent := NewAgent(testContainmentOption(), WithLogger(slog.New(slog.DiscardHandler)))
	agent.conn = conn
	agent.lifecycle = lifecycle.Negotiated{Version: 1, UpdatesOutsidePrompt: true}
	agent.clientCapabilities.Elicitation = &acp.ElicitationCapabilities{Form: &acp.ElicitationFormCapabilities{}}
	process := newStubProcess(false)
	native := newStubPiClient()
	session := &agentSession{agent: agent, id: "action-failure", proc: process, client: native}
	outbox := newTestSessionOutbox(1)
	bindTestRuntime(outbox, process, native, nil, nil, nil)
	session.outbox = outbox
	session.pumpGeneration = 1
	require.NoError(t, session.openLifecycleStream(t.Context(), 1))
	require.NoError(t, session.lifecycleAcceptTurn(t.Context(), testSubmission()))

	return session, process, native
}

func (c *heldActionClient) RequestPermission(
	ctx context.Context,
	_ acp.RequestPermissionRequest,
) (acp.RequestPermissionResponse, error) {
	if c.kind != lifecycle.ActionPermission {
		return acp.RequestPermissionResponse{}, errors.New("unexpected permission request")
	}

	acknowledgeActionRequestWrite(ctx, nil)
	close(c.started)
	<-ctx.Done()
	close(c.cancelled)

	return acp.RequestPermissionResponse{}, ctx.Err()
}

func (c *heldActionClient) CreateElicitation(
	ctx context.Context,
	_ acp.UnstableCreateElicitationRequest,
	_ elicitationScope,
) (acp.UnstableCreateElicitationResponse, error) {
	if c.kind != lifecycle.ActionElicitation {
		return acp.UnstableCreateElicitationResponse{}, errors.New("unexpected elicitation request")
	}

	acknowledgeActionRequestWrite(ctx, nil)
	close(c.started)
	<-ctx.Done()
	close(c.cancelled)

	return acp.UnstableCreateElicitationResponse{}, ctx.Err()
}

func nextLifecycleActionWireMessage(t *testing.T, lines <-chan []byte) lifecycleActionWireMessage {
	t.Helper()

	select {
	case line := <-lines:
		var message lifecycleActionWireMessage
		require.NoError(t, json.Unmarshal(line, &message))

		return message
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for outbound ACP message")

		return lifecycleActionWireMessage{}
	}
}

func TestActionAnswerClassificationAndPlainRequest(t *testing.T) {
	errBoom := errors.New("boom")
	require.Equal(t, lifecycle.ActionFailed, permissionActionState(acp.RequestPermissionResponse{}, errBoom))
	require.Equal(t, lifecycle.ActionCancelled, permissionActionState(acp.RequestPermissionResponse{Outcome: acp.NewRequestPermissionOutcomeCancelled()}, nil))
	require.Equal(t, lifecycle.ActionCancelled, permissionActionState(acp.RequestPermissionResponse{}, nil))
	require.Equal(t, lifecycle.ActionAccepted, permissionActionState(acp.RequestPermissionResponse{Outcome: acp.NewRequestPermissionOutcomeSelected(permissionOptionAllow)}, nil))
	require.Equal(t, lifecycle.ActionDeclined, permissionActionState(acp.RequestPermissionResponse{Outcome: acp.NewRequestPermissionOutcomeSelected("deny_once")}, nil))
	require.Equal(t, lifecycle.ActionFailed, elicitationActionState(acp.UnstableCreateElicitationResponse{}, errBoom))
	require.Equal(t, lifecycle.ActionAccepted, elicitationActionState(acp.UnstableCreateElicitationResponse{Accept: &acp.UnstableCreateElicitationAccept{}}, nil))
	require.Equal(t, lifecycle.ActionDeclined, elicitationActionState(acp.UnstableCreateElicitationResponse{Decline: &acp.UnstableCreateElicitationDecline{}}, nil))
	require.Equal(t, lifecycle.ActionCancelled, elicitationActionState(acp.UnstableCreateElicitationResponse{}, nil))

	s := &agentSession{}
	value, err := announcedActionRequest(t.Context(), s, lifecycle.ActionPermission,
		func(_ context.Context, meta map[string]any) (string, error) {
			require.Nil(t, meta)

			return "plain", nil
		},
		func(string, error) lifecycle.ActionState { return lifecycle.ActionAccepted })
	require.NoError(t, err)
	require.Equal(t, "plain", value)
}

func TestActionWriterPanicFailsClosedAndAnswersNativeDialogOnce(t *testing.T) {
	for _, mode := range []string{"panic before write", "panic after write"} {
		t.Run(mode, func(t *testing.T) {
			client := &faultingActionClient{directAgentClient: newDirectAgentClient(), mode: mode}
			session, process, native := actionFailureSession(t, client)

			session.handleElicitationDialog(t.Context(), pi.UIRequest{ID: "dialog", Method: uiMethodInput})

			require.NoError(t, session.awaitPoisonContainment())
			require.Equal(t, 1, process.shutdownCalls)
			require.Equal(t, 1, process.closeCalls)
			native.mu.Lock()
			require.Len(t, native.responses, 1)
			require.Equal(t, "dialog", native.responses[0].ID)
			native.mu.Unlock()

			_, err := session.acquireTurn(t.Context())
			require.Error(t, err)
			require.NotContains(t, err.Error(), "action writer secret")
		})
	}
}

func TestActionFailureDeniesExactEmittingClientBeforeContainingGeneration(t *testing.T) {
	host := &faultingActionClient{directAgentClient: newDirectAgentClient(), mode: "panic after write"}
	session, oldProcess, oldClient := actionFailureSession(t, host)
	oldOutbox := session.outbox
	dialog := &nativeDialog{
		request: pi.UIRequest{ID: "old-dialog", Method: uiMethodInput},
		outbox:  oldOutbox,
		client:  oldClient,
	}
	trace := make([]string, 0, 3)
	oldClient.respondFunc = func(pi.UIResponse) { trace = append(trace, "deny") }
	oldProcess.shutdownFunc = func(context.Context) error {
		trace = append(trace, "shutdown")

		return nil
	}
	oldProcess.closeFunc = func() error {
		trace = append(trace, "close")

		return nil
	}

	successorProcess := newStubProcess(false)
	successorClient := newStubPiClient()
	successor := newTestSessionOutbox(2)
	bindTestRuntime(successor, successorProcess, successorClient, nil, nil, nil)
	session.mu.Lock()
	session.proc = successorProcess
	session.client = successorClient
	session.outbox = successor
	session.mu.Unlock()

	session.handleNativeElicitationDialog(t.Context(), dialog)
	require.NoError(t, session.awaitPoisonContainment())

	oldClient.mu.Lock()
	require.Len(t, oldClient.responses, 1)
	require.Equal(t, "old-dialog", oldClient.responses[0].ID)
	oldClient.mu.Unlock()
	successorClient.mu.Lock()
	require.Empty(t, successorClient.responses)
	successorClient.mu.Unlock()
	require.Equal(t, 1, oldProcess.shutdownCalls)
	require.Equal(t, []string{"deny", "shutdown", "close"}, trace)
	require.Zero(t, successorProcess.shutdownCalls)
	require.Zero(t, successorProcess.closeCalls)
}

func TestActionWriterIgnoringCancellationCannotVetoNativeCloseAndIsRetained(t *testing.T) {
	originalActionTimeout := sessionActionRequestTimeout
	originalCloseWait := sessionCloseTurnWaitContext
	sessionActionRequestTimeout = 20 * time.Millisecond
	sessionCloseTurnWaitContext = func(ctx context.Context) (context.Context, context.CancelFunc) {
		return context.WithTimeout(ctx, 20*time.Millisecond)
	}
	t.Cleanup(func() {
		sessionActionRequestTimeout = originalActionTimeout
		sessionCloseTurnWaitContext = originalCloseWait
	})

	client := &faultingActionClient{
		directAgentClient: newDirectAgentClient(),
		mode:              "ignore cancellation",
		entered:           make(chan struct{}),
		release:           make(chan struct{}),
	}
	session, process, native := actionFailureSession(t, client)
	agent := session.agent
	agent.sessions[session.id] = session

	dialogDone := make(chan struct{})
	go func() {
		defer close(dialogDone)
		session.handleElicitationDialog(context.Background(), pi.UIRequest{ID: "held", Method: uiMethodInput})
	}()
	<-client.entered

	closeErr := agent.Close()
	require.ErrorIs(t, closeErr, ErrContainmentIncomplete)
	require.Equal(t, 1, process.shutdownCalls, "bounded host delivery cannot veto native shutdown")
	require.Equal(t, 1, process.closeCalls, "bounded host delivery cannot veto native close")
	native.mu.Lock()
	require.Len(t, native.responses, 1)
	native.mu.Unlock()
	agent.mu.Lock()
	require.Same(t, session, agent.sessions[session.id])
	agent.mu.Unlock()

	close(client.release)
	<-dialogDone
	require.ErrorIs(t, agent.Close(), ErrContainmentIncomplete)
}

func TestActionRequestCancellationAtEachWriteBarrierContainsExactGeneration(t *testing.T) {
	for _, testCase := range []struct {
		name        string
		acknowledge func(context.Context)
	}{
		{name: "before write acknowledgement"},
		{
			name: "after failed write acknowledgement",
			acknowledge: func(ctx context.Context) {
				acknowledgeActionRequestWrite(ctx, errors.New("write failed"))
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			session, process, _ := actionFailureSession(t, newDirectAgentClient())
			outbox := session.outbox
			entered := make(chan struct{})
			release := make(chan struct{})
			requestCtx, cancelRequest := context.WithCancel(context.Background())

			type result struct {
				value string
				err   error
			}
			resultCh := make(chan result, 1)
			failedNative := 0
			go func() {
				value, err := announcedBoundActionRequest(
					requestCtx,
					session,
					lifecycle.ActionPermission,
					func(ctx context.Context, _ map[string]any) (string, error) {
						if testCase.acknowledge != nil {
							testCase.acknowledge(ctx)
						}
						close(entered)
						<-release

						return "late", nil
					},
					func(string, error) lifecycle.ActionState { return lifecycle.ActionAccepted },
					outbox,
					func() { failedNative++ },
				)
				resultCh <- result{value: value, err: err}
			}()

			<-entered
			cancelRequest()
			got := <-resultCh
			require.Empty(t, got.value)
			require.ErrorIs(t, got.err, errLifecycleActionRequest)
			require.Equal(t, 1, failedNative)
			require.Equal(t, 1, process.shutdownCalls)
			require.Equal(t, 1, process.closeCalls)

			close(release)
			require.NoError(t, outbox.producers.waitChildren(t.Context()))
			require.NoError(t, outbox.interactions.waitChildren(t.Context()))
		})
	}
}

func TestCloseCancelsAndJoinsHostActionBeforeNativeInterrupt(t *testing.T) {
	trace := make([]string, 0, 4)
	var traceMu sync.Mutex
	record := func(value string) {
		traceMu.Lock()
		trace = append(trace, value)
		traceMu.Unlock()
	}

	host := &heldActionClient{
		recordingClient: newRecordingClient(),
		kind:            lifecycle.ActionElicitation,
		started:         make(chan struct{}),
		cancelled:       make(chan struct{}),
	}
	session, process, native := actionFailureSession(t, host)
	native.respondFunc = func(pi.UIResponse) { record("deny") }
	native.abortFunc = func(context.Context) error {
		record("abort")

		return nil
	}
	process.shutdownFunc = func(context.Context) error {
		record("shutdown")

		return nil
	}
	process.closeFunc = func() error {
		record("close")

		return nil
	}

	releaseDialog, admitted := session.admitDialogHandler(session.outbox)
	require.True(t, admitted)
	dialog := &nativeDialog{
		request: pi.UIRequest{ID: "barrier", Method: uiMethodInput},
		outbox:  session.outbox,
		client:  native,
		done:    releaseDialog,
	}
	dialogDone := make(chan struct{})
	go func() {
		defer close(dialogDone)
		defer dialog.complete()
		session.handleNativeElicitationDialog(context.Background(), dialog)
	}()
	<-host.started

	require.NoError(t, session.Close(t.Context()))
	<-dialogDone
	<-host.cancelled
	traceMu.Lock()
	got := append([]string(nil), trace...)
	traceMu.Unlock()
	require.Equal(t, []string{"deny", "abort", "shutdown", "close"}, got)
}

func TestTimedOutActionAnnouncementRetainsImmutableGeneration(t *testing.T) {
	originalWait := sessionCloseTurnWaitContext
	sessionCloseTurnWaitContext = func(ctx context.Context) (context.Context, context.CancelFunc) {
		bounded, cancel := context.WithCancel(ctx)
		cancel()

		return bounded, cancel
	}
	t.Cleanup(func() { sessionCloseTurnWaitContext = originalWait })

	host := &blockingActionAnnouncementClient{
		heldActionClient: &heldActionClient{
			recordingClient: newRecordingClient(),
			kind:            lifecycle.ActionElicitation,
			started:         make(chan struct{}),
			cancelled:       make(chan struct{}),
		},
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	session, process, native := actionFailureSession(t, host)
	releaseDialog, admitted := session.admitDialogHandler(session.outbox)
	require.True(t, admitted)
	dialog := &nativeDialog{
		request: pi.UIRequest{ID: "blocked-announcement", Method: uiMethodInput},
		outbox:  session.outbox,
		client:  native,
		done:    releaseDialog,
	}
	dialogDone := make(chan struct{})
	go func() {
		defer close(dialogDone)
		defer dialog.complete()
		session.handleNativeElicitationDialog(context.Background(), dialog)
	}()
	<-host.entered
	require.True(t, session.lcMu.TryLock(), "blocked lifecycle delivery retained lcMu")
	session.lcMu.Unlock()

	closed := make(chan error, 1)
	go func() { closed <- session.Close(context.Background()) }()
	require.ErrorIs(t, <-closed, ErrContainmentIncomplete)
	require.Equal(t, 1, process.shutdownCalls)
	require.Equal(t, 1, process.closeCalls)

	session.lcMu.Lock()
	sequence := session.lc.stream.Sequence()
	require.Empty(t, session.lc.blockers)
	session.lcMu.Unlock()
	close(host.release)
	<-dialogDone

	session.lcMu.Lock()
	require.Equal(t, sequence, session.lc.stream.Sequence())
	require.Empty(t, session.lc.blockers)
	require.ErrorIs(t, session.lc.quarantineErr, ErrContainmentIncomplete)
	session.lcMu.Unlock()
	require.Equal(t, 1, process.shutdownCalls, "late announcement release repeated containment")
	require.Equal(t, 1, process.closeCalls, "late announcement release repeated cleanup")
}

// TestAnnouncedActionRequestLifecycle pins the ordered action contract: the
// request carries the action identity on the wire, the announcement follows
// it, and the resolution lands exactly once with the answer's classification.
func TestAnnouncedActionRequestLifecycle(t *testing.T) {
	s, client := actionLifecycleSession(t, false)
	require.NoError(t, s.openLifecycleStream(t.Context(), 1))
	require.NoError(t, s.lifecycleAcceptTurn(t.Context(), lifecycle.Submission{SubmissionID: "s", ClientNonce: "n"}))

	var sentMeta map[string]any
	value, err := announcedActionRequest(t.Context(), s, lifecycle.ActionPermission,
		func(ctx context.Context, meta map[string]any) (string, error) {
			sentMeta = meta
			acknowledgeActionRequestWrite(ctx, nil)

			return "answer", nil
		},
		func(string, error) lifecycle.ActionState { return lifecycle.ActionAccepted })
	require.NoError(t, err)
	require.Equal(t, "answer", value)

	action := anyMap(t, anyMap(t, sentMeta[lifecycleMetaKey])["action"])
	actionID, _ := action["actionId"].(string)
	require.NotEmpty(t, actionID)
	require.Equal(t, s.lc.stream.ID(), anyMap(t, sentMeta[lifecycleMetaKey])["streamId"])

	var announced, resolved bool
	for _, notification := range client.notifications {
		envelope := anyMap(t, notification.Meta[lifecycleMetaKey])
		event := anyMap(t, envelope["event"])
		if event["type"] != "action_update" {
			continue
		}

		update := anyMap(t, event["action"])
		if update["actionId"] != actionID {
			continue
		}

		switch update["state"] {
		case "pending":
			announced = true
			require.Equal(t, "permission", update["kind"])
		case "accepted":
			resolved = true
		}
	}
	require.True(t, announced, "the announced action names the request's identity")
	require.True(t, resolved, "the resolved action names the request's identity")

	sentErr := errors.New("send")
	notificationsBeforeFailure := len(client.notifications)
	_, err = announcedActionRequest(t.Context(), s, lifecycle.ActionElicitation,
		func(context.Context, map[string]any) (string, error) { return "", sentErr },
		func(_ string, err error) lifecycle.ActionState {
			require.ErrorIs(t, err, sentErr)

			return lifecycle.ActionFailed
		})
	require.ErrorIs(t, err, sentErr)
	require.Len(t, client.notifications, notificationsBeforeFailure,
		"a request that did not cross the write barrier announced an action")

	client.updateErr = errors.New("announce delivery")
	_, err = announcedActionRequest(t.Context(), s, lifecycle.ActionPermission,
		func(ctx context.Context, _ map[string]any) (string, error) {
			acknowledgeActionRequestWrite(ctx, nil)

			return "unpublished", nil
		},
		func(string, error) lifecycle.ActionState { return lifecycle.ActionAccepted })
	require.ErrorIs(t, err, errLifecycleActionAnnouncement)

	fenced, _ := actionLifecycleSession(t, false)
	require.NoError(t, fenced.openLifecycleStream(t.Context(), 1))
	require.NoError(t, fenced.lifecycleAcceptTurn(t.Context(), testSubmission()))
	original := lifecycleRandRead
	lifecycleRandRead = func([]byte) (int, error) { return 0, errors.New("entropy") }
	t.Cleanup(func() { lifecycleRandRead = original })
	_, err = announcedActionRequest(t.Context(), fenced, lifecycle.ActionPermission,
		func(context.Context, map[string]any) (string, error) { return "never sent", nil },
		func(string, error) lifecycle.ActionState { return lifecycle.ActionAccepted })
	require.ErrorContains(t, err, "entropy")
}

// TestAnnouncementFailureRevokesHeldHostAction exercises both action methods
// at both delivery seams. The host deliberately never answers; cancellation of
// its exact request is therefore the only way the call can return.
func TestAnnouncementFailureRevokesHeldHostAction(t *testing.T) {
	for _, kind := range []lifecycle.ActionKind{lifecycle.ActionPermission, lifecycle.ActionElicitation} {
		for _, failAfter := range []int{3, 4} {
			stage := "pending"
			if failAfter == 4 {
				stage = "requires_action"
			}
			t.Run(string(kind)+"/"+stage, func(t *testing.T) {
				logs := &strings.Builder{}
				recording := newRecordingClient()
				recording.lifecycleFailAfter = failAfter
				recording.failureErr = errors.New("provider announcement sentinel")
				client := &heldActionClient{
					recordingClient: recording,
					kind:            kind,
					started:         make(chan struct{}),
					cancelled:       make(chan struct{}),
				}
				agent := NewAgent(testContainmentOption(), WithLogger(slog.New(slog.NewTextHandler(logs, nil))))
				agent.conn = client
				agent.lifecycle = lifecycle.Negotiated{Version: 1, UpdatesOutsidePrompt: true}
				process := newStubProcess(false)
				native := newStubPiClient()
				session := &agentSession{agent: agent, id: "action", proc: process, client: native}
				outbox := newTestSessionOutbox(1)
				bindTestRuntime(outbox, process, native, nil, nil, nil)
				session.outbox = outbox
				session.pumpGeneration = 1
				require.NoError(t, session.openLifecycleStream(t.Context(), 1))
				require.NoError(t, session.lifecycleAcceptTurn(t.Context(), testSubmission()))

				result := make(chan error, 1)
				go func() {
					switch kind {
					case lifecycle.ActionPermission:
						_, err := session.requestAnnouncedPermission(context.Background(), client, acp.RequestPermissionRequest{SessionId: session.id}, dialogActionBinding{outbox: outbox})
						result <- err
					case lifecycle.ActionElicitation:
						_, err := session.requestAnnouncedElicitation(context.Background(), client, acp.UnstableCreateElicitationRequest{
							Form: &acp.UnstableCreateElicitationForm{},
						}, elicitationScope{SessionID: session.id}, dialogActionBinding{outbox: outbox})
						result <- err
					}
				}()

				<-client.started
				require.ErrorIs(t, <-result, errLifecycleActionAnnouncement)
				<-client.cancelled
				require.NoError(t, session.awaitPoisonContainment())

				session.lcMu.Lock()
				require.Empty(t, session.lc.blockers)
				require.True(t, session.lc.fenced)
				session.lcMu.Unlock()
				require.Equal(t, 1, process.shutdownCalls)
				require.Equal(t, 1, process.closeCalls)
				require.NotContains(t, logs.String(), "provider announcement sentinel")
			})
		}
	}
}

func TestActionAdmissionFailureMatrixCancelsExactNativeOwner(t *testing.T) {
	session, _ := actionLifecycleSession(t, false)
	require.NoError(t, session.openLifecycleStream(t.Context(), 1))
	require.NoError(t, session.lifecycleAcceptTurn(t.Context(), testSubmission()))

	for _, testCase := range []struct {
		name  string
		ctx   context.Context //nolint:containedctx // The table selects exact cancelled/live admission contexts.
		setup func(*sessionOutbox)
	}{
		{
			name: "caller already cancelled",
			ctx: func() context.Context {
				ctx, cancel := context.WithCancel(t.Context())
				cancel()

				return ctx
			}(),
		},
		{
			name: "generation handler admission closed",
			ctx:  t.Context(),
			setup: func(outbox *sessionOutbox) {
				outbox.interactions.releaseRoot()
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			outbox := newTestSessionOutbox(1)
			if testCase.setup != nil {
				testCase.setup(outbox)
			}
			cancelled := 0
			_, err := announcedBoundActionRequest(testCase.ctx, session, lifecycle.ActionPermission,
				func(context.Context, map[string]any) (string, error) {
					t.Fatal("failed admission reached host request")

					return "", nil
				},
				func(string, error) lifecycle.ActionState { return lifecycle.ActionFailed },
				outbox,
				func() { cancelled++ },
			)
			require.Error(t, err)
			require.Equal(t, 1, cancelled)
		})
	}

	cancelled := 0
	session.outbox = nil
	_, err := announcedBoundActionRequest(t.Context(), session, lifecycle.ActionPermission,
		func(context.Context, map[string]any) (string, error) {
			t.Fatal("unowned action reached host request")

			return "", nil
		},
		func(string, error) lifecycle.ActionState { return lifecycle.ActionFailed },
		nil,
		func() { cancelled++ },
	)
	require.ErrorIs(t, err, errLifecycleActionUnowned)
	require.Equal(t, 1, cancelled)
}

func TestLifecycleActionAnnouncementPanicIsContained(t *testing.T) {
	err := runLifecycleActionAnnouncement(t.Context(), nil, pendingAction{}, lifecycle.ActionPermission)
	require.ErrorIs(t, err, ErrContainmentIncomplete)
}

func TestStaleStreamAfterActionWriteIsRevokedWithoutAnswer(t *testing.T) {
	s, _ := actionLifecycleSession(t, false)
	process := newStubProcess(false)
	native := newStubPiClient()
	s.proc = process
	s.client = native
	outbox := newTestSessionOutbox(1)
	bindTestRuntime(outbox, process, native, nil, nil, nil)
	s.outbox = outbox
	s.pumpGeneration = 1
	require.NoError(t, s.openLifecycleStream(t.Context(), 1))
	require.NoError(t, s.lifecycleAcceptTurn(t.Context(), testSubmission()))

	cancelled := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		_, err := announcedActionRequest(context.Background(), s, lifecycle.ActionPermission,
			func(ctx context.Context, _ map[string]any) (string, error) {
				// The request's full write has completed. Fence the stream before
				// reporting that write to the announcement coordinator.
				s.fenceLifecycleStream()
				acknowledgeActionRequestWrite(ctx, nil)
				<-ctx.Done()
				close(cancelled)

				return "", ctx.Err()
			},
			func(string, error) lifecycle.ActionState { return lifecycle.ActionFailed })
		result <- err
	}()

	require.ErrorIs(t, <-result, errLifecycleActionAnnouncement)
	<-cancelled
	require.NoError(t, s.awaitPoisonContainment())
	require.Equal(t, 1, process.shutdownCalls)
	require.Equal(t, 1, process.closeCalls)
}

// TestAnnouncedPermissionCrossesTheRequestWriteBarrier pins the transport
// boundary itself: the pending response is registered and the request write has
// completed before the lifecycle notification can announce its action id.
func TestAnnouncedPermissionCrossesTheRequestWriteBarrier(t *testing.T) {
	input, respond := io.Pipe()
	wire := &lifecycleActionWireWriter{
		lines:          make(chan []byte, 16),
		blockMethod:    acp.ClientMethodSessionRequestPermission,
		requestWrite:   make(chan struct{}),
		releaseRequest: make(chan struct{}),
	}
	agent := NewAgent(testContainmentOption())
	agent.lifecycle = lifecycle.Negotiated{Version: 1, ActivityKinds: []lifecycle.ActivityKind{}}
	connection := newLocalAgentConnection(agent, wire, input)
	agent.setConnection(connection)
	t.Cleanup(func() {
		select {
		case <-wire.releaseRequest:
		default:
			close(wire.releaseRequest)
		}
		require.NoError(t, respond.Close())

		select {
		case <-connection.Done():
		case <-time.After(time.Second):
			t.Error("ACP connection did not stop")
		}
	})

	session := &agentSession{agent: agent, id: "session", outbox: newTestSessionOutbox(1)}
	require.NoError(t, session.openLifecycleStream(t.Context(), 1))
	require.NoError(t, session.lifecycleAcceptTurn(t.Context(), lifecycle.Submission{
		SubmissionID: "submission", ClientNonce: "nonce",
	}))

	for range 3 {
		nextLifecycleActionWireMessage(t, wire.lines)
	}
	session.lcMu.Lock()
	sequenceBeforeRequest := session.lc.stream.Sequence()
	session.lcMu.Unlock()

	type result struct {
		response acp.RequestPermissionResponse
		err      error
	}
	done := make(chan result, 1)
	go func() {
		response, err := session.requestAnnouncedPermission(t.Context(), connection, acp.RequestPermissionRequest{
			SessionId: session.id,
		}, dialogActionBinding{outbox: session.outbox})
		done <- result{response: response, err: err}
	}()

	request := nextLifecycleActionWireMessage(t, wire.lines)
	require.Equal(t, acp.ClientMethodSessionRequestPermission, request.Method)
	require.NotNil(t, request.ID)
	<-wire.requestWrite

	session.lcMu.Lock()
	require.Equal(t, sequenceBeforeRequest, session.lc.stream.Sequence(),
		"action was claimed before the request write completed")
	session.lcMu.Unlock()

	// The SDK response registration precedes its transport write: route a
	// response while that write is still blocked, then let the write complete.
	encodedResult, err := json.Marshal(acp.RequestPermissionResponse{
		Outcome: acp.NewRequestPermissionOutcomeSelected(permissionOptionAllow),
	})
	require.NoError(t, err)
	response, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      *request.ID,
		"result":  json.RawMessage(encodedResult),
	})
	require.NoError(t, err)
	_, err = respond.Write(append(response, '\n'))
	require.NoError(t, err)
	close(wire.releaseRequest)

	announcement := nextLifecycleActionWireMessage(t, wire.lines)
	require.Equal(t, acp.ClientMethodSessionUpdate, announcement.Method)
	var notification acp.SessionNotification
	require.NoError(t, json.Unmarshal(announcement.Params, &notification))
	envelope := anyMap(t, notification.Meta[lifecycleMetaKey])
	event := anyMap(t, envelope["event"])
	require.Equal(t, "action_update", event["type"])
	require.Equal(t, "pending", anyMap(t, event["action"])["state"])

	select {
	case result := <-done:
		require.NoError(t, result.err)
		require.Equal(t, permissionOptionAllow, result.response.Outcome.Selected.OptionId)
	case <-time.After(time.Second):
		t.Fatal("permission response was not routed to the registered request")
	}
}

// TestAnnouncedElicitationStampsActionMeta pins that the action correlation
// rides either elicitation variant's own _meta when a turn owns the request.
func TestAnnouncedElicitationStampsActionMeta(t *testing.T) {
	s, _ := actionLifecycleSession(t, false)
	require.NoError(t, s.openLifecycleStream(t.Context(), 1))
	require.NoError(t, s.lifecycleAcceptTurn(t.Context(), lifecycle.Submission{SubmissionID: "s", ClientNonce: "n"}))

	dialog := newDialogStubClient()
	dialog.elicitationResponse = acp.UnstableCreateElicitationResponse{
		Accept: &acp.UnstableCreateElicitationAccept{Action: "accept", Content: map[string]any{elicitationFieldValue: "v"}},
	}
	_, accepted := s.createDialogElicitation(t.Context(), dialog, pi.UIRequest{ID: "dialog", Method: uiMethodInput})
	require.True(t, accepted)
	require.Len(t, dialog.elicitationRequests, 1)
	require.Contains(t, dialog.elicitationRequests[0].Form.Meta, lifecycleMetaKey)

	dialog.elicitationResponse = acp.UnstableCreateElicitationResponse{
		Decline: &acp.UnstableCreateElicitationDecline{Action: "decline"},
	}
	_, err := s.requestAnnouncedElicitation(t.Context(), dialog, acp.UnstableCreateElicitationRequest{
		Url: &acp.UnstableCreateElicitationUrl{
			ElicitationId: "url", Message: "open", Mode: elicitationModeURL, Url: "https://example.test",
		},
	}, elicitationScope{}, dialogActionBinding{outbox: s.outbox})
	require.NoError(t, err)
	require.Len(t, dialog.elicitationRequests, 2)
	require.Contains(t, dialog.elicitationRequests[1].Url.Meta, lifecycleMetaKey)
}

// TestUnownedLifecycleActionIsRefusedRatherThanSentBare pins the states
// prepareLifecycleAction distinguishes. While version 1 is negotiated every
// permission and elicitation carries the correlation, so a fenced stream or a
// live incarnation with no owner refuses the request. Only an unnegotiated
// session sends plainly.
func TestUnownedLifecycleActionIsRefusedRatherThanSentBare(t *testing.T) {
	t.Run("unnegotiated sends plainly", func(t *testing.T) {
		s := &agentSession{agent: NewAgent()}

		action, announceable, err := s.prepareLifecycleAction()
		require.NoError(t, err)
		require.False(t, announceable)
		require.Zero(t, action)
	})

	t.Run("a fenced incarnation refuses", func(t *testing.T) {
		s, _ := actionLifecycleSession(t, false)
		require.NoError(t, s.openLifecycleStream(t.Context(), 1))
		require.NoError(t, s.lifecycleAcceptTurn(t.Context(), testSubmission()))
		s.fenceLifecycleStream()

		_, announceable, err := s.prepareLifecycleAction()
		require.ErrorIs(t, err, errLifecycleStreamFenced)
		require.False(t, announceable)
	})

	t.Run("a stale callback sends no uncorrelated request", func(t *testing.T) {
		s, _ := actionLifecycleSession(t, false)
		require.NoError(t, s.openLifecycleStream(t.Context(), 1))
		require.NoError(t, s.lifecycleAcceptTurn(t.Context(), testSubmission()))

		callbackEntered := make(chan struct{})
		releaseCallback := make(chan struct{})
		sent := make(chan struct{}, 1)
		result := make(chan error, 1)
		go func() {
			close(callbackEntered)
			<-releaseCallback
			_, err := announcedActionRequest(context.Background(), s, lifecycle.ActionPermission,
				func(context.Context, map[string]any) (int, error) {
					sent <- struct{}{}

					return 0, nil
				},
				func(int, error) lifecycle.ActionState { return lifecycle.ActionCancelled },
			)
			result <- err
		}()

		<-callbackEntered
		s.fenceLifecycleStream()
		close(releaseCallback)
		require.ErrorIs(t, <-result, errLifecycleStreamFenced)
		select {
		case <-sent:
			t.Fatal("a stale negotiated callback sent a bare host request")
		default:
		}
	})

	t.Run("a live incarnation with no open turn refuses", func(t *testing.T) {
		s, _ := actionLifecycleSession(t, false)
		require.NoError(t, s.openLifecycleStream(t.Context(), 1))

		_, announceable, err := s.prepareLifecycleAction()
		require.ErrorIs(t, err, errLifecycleActionUnowned)
		require.False(t, announceable)

		// A turn that has already settled is the same state: the dialog
		// handler outlived the turn it was spawned for.
		require.NoError(t, s.lifecycleAcceptTurn(t.Context(), testSubmission()))
		require.NoError(t, s.lifecycleSettleTurn(t.Context(),
			lifecycle.StopReasonEndTurn, lifecycle.OutcomeSuccess))

		_, _, err = s.prepareLifecycleAction()
		require.ErrorIs(t, err, errLifecycleActionUnowned)
	})

	t.Run("an open turn always owns its action", func(t *testing.T) {
		s, _ := actionLifecycleSession(t, false)
		require.NoError(t, s.openLifecycleStream(t.Context(), 1))
		require.NoError(t, s.lifecycleAcceptTurn(t.Context(), testSubmission()))

		action, announceable, err := s.prepareLifecycleAction()
		require.NoError(t, err)
		require.True(t, announceable)
		require.NotEmpty(t, action.actionID)
		require.Equal(t, s.lc.stream.ID(), action.streamID)
		require.Equal(t, lifecycle.Owner{Type: lifecycle.OwnerTurn, ID: s.lc.turnID}, action.owner)
	})

	// The refusal reaches the caller as a failed request, which both dialog
	// legs already answer with a deterministic native cancel.
	t.Run("the announced request refuses with it", func(t *testing.T) {
		s, _ := actionLifecycleSession(t, false)
		require.NoError(t, s.openLifecycleStream(t.Context(), 1))

		_, err := announcedActionRequest(t.Context(), s, lifecycle.ActionPermission,
			func(context.Context, map[string]any) (int, error) {
				t.Fatal("an unowned action never reaches the wire")

				return 0, nil
			},
			func(int, error) lifecycle.ActionState { return lifecycle.ActionFailed })
		require.ErrorIs(t, err, errLifecycleActionUnowned)
	})
}
