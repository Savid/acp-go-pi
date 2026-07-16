package piacp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"

	"github.com/savid/acp-go-pi/internal/pi"
)

type orderedPermissionClient struct {
	*dialogStubClient
	updatesBeforePermission []acp.SessionUpdate
}

// strictPermissionClient models the host invariant that a permission request
// is valid only while its exact tool-call id is already published and
// nonterminal. It also rejects duplicate starts, making both channel-order
// races observable instead of relying on notification timing.
type strictPermissionClient struct {
	*directAgentClient

	mu                 sync.Mutex
	tools              map[acp.ToolCallId]acp.ToolCallStatus
	updates            []acp.SessionUpdate
	permissionRequests []acp.RequestPermissionRequest
	gateToolCallID     acp.ToolCallId
	gateStatus         acp.ToolCallStatus
	gateEntered        chan struct{}
	gateRelease        chan struct{}
	gateOnce           sync.Once
}

func newStrictPermissionClient() *strictPermissionClient {
	return &strictPermissionClient{
		directAgentClient: newDirectAgentClient(),
		tools:             make(map[acp.ToolCallId]acp.ToolCallStatus),
	}
}

func (c *strictPermissionClient) gate(
	toolCallID acp.ToolCallId,
	status acp.ToolCallStatus,
) (<-chan struct{}, chan<- struct{}) {
	c.gateToolCallID = toolCallID
	c.gateStatus = status
	c.gateEntered = make(chan struct{})
	c.gateRelease = make(chan struct{})

	return c.gateEntered, c.gateRelease
}

func (c *strictPermissionClient) SessionUpdate(_ context.Context, notification acp.SessionNotification) error {
	update := notification.Update
	toolCallID := acp.ToolCallId("")
	status := acp.ToolCallStatus("")

	c.mu.Lock()
	switch {
	case update.ToolCall != nil:
		id := update.ToolCall.ToolCallId
		toolCallID = id
		if _, exists := c.tools[id]; exists {
			c.mu.Unlock()

			return fmt.Errorf("duplicate tool start for %s", id)
		}

		status = update.ToolCall.Status
		c.tools[id] = status
	case update.ToolCallUpdate != nil:
		id := update.ToolCallUpdate.ToolCallId
		toolCallID = id
		if _, exists := c.tools[id]; !exists {
			c.mu.Unlock()

			return fmt.Errorf("tool update for unknown id %s", id)
		}

		if update.ToolCallUpdate.Status != nil {
			status = *update.ToolCallUpdate.Status
			c.tools[id] = status
		}
	}
	c.updates = append(c.updates, update)
	shouldGate := toolCallID == c.gateToolCallID && status != "" &&
		status == c.gateStatus && c.gateEntered != nil
	entered := c.gateEntered
	release := c.gateRelease
	c.mu.Unlock()

	if shouldGate {
		c.gateOnce.Do(func() { close(entered) })
		<-release
	}

	return nil
}

func (c *strictPermissionClient) RequestPermission(
	_ context.Context,
	request acp.RequestPermissionRequest,
) (acp.RequestPermissionResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	status, exists := c.tools[request.ToolCall.ToolCallId]
	if !exists || status == acp.ToolCallStatusCompleted || status == acp.ToolCallStatusFailed {
		return acp.RequestPermissionResponse{}, fmt.Errorf(
			"permission tool %s is not currently nonterminal",
			request.ToolCall.ToolCallId,
		)
	}

	c.permissionRequests = append(c.permissionRequests, request)

	return acp.RequestPermissionResponse{
		Outcome: acp.NewRequestPermissionOutcomeSelected(permissionOptionAllow),
	}, nil
}

func (c *strictPermissionClient) snapshot() ([]acp.SessionUpdate, []acp.RequestPermissionRequest) {
	c.mu.Lock()
	defer c.mu.Unlock()

	return append([]acp.SessionUpdate(nil), c.updates...),
		append([]acp.RequestPermissionRequest(nil), c.permissionRequests...)
}

func (c *orderedPermissionClient) RequestPermission(
	ctx context.Context,
	request acp.RequestPermissionRequest,
) (acp.RequestPermissionResponse, error) {
	c.updatesBeforePermission = append(c.updatesBeforePermission, c.updates...)

	return c.dialogStubClient.RequestPermission(ctx, request)
}

func TestToolKindForNameMapping(t *testing.T) {
	for name, kind := range map[string]acp.ToolKind{
		"read": acp.ToolKindRead, "edit": acp.ToolKindEdit, "write": acp.ToolKindEdit,
		"bash": acp.ToolKindExecute, "grep": acp.ToolKindSearch, "find": acp.ToolKindSearch,
		"glob": acp.ToolKindSearch, "ls": acp.ToolKindSearch, "fetch": acp.ToolKindFetch,
		"web_fetch": acp.ToolKindFetch, "other": acp.ToolKindOther,
	} {
		require.Equal(t, kind, toolKindForName(name))
	}
}

func TestRegisterDialogCancelledTurn(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	session := &agentSession{agent: agent, id: "session"}

	session.turnCancelled = true
	dialogCtx, finishDialog := session.registerDialog(t.Context(), "cancelled")
	require.ErrorIs(t, dialogCtx.Err(), context.Canceled)
	finishDialog()
	require.Empty(t, session.pendingDialogs)
}

func TestRequestPermissionAnswerFailsClosed(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	session := &agentSession{agent: agent, id: "session"}

	require.Equal(
		t,
		string(permissionOptionDeny),
		session.requestPermissionAnswer(t.Context(), pi.UIRequest{ID: "none"}, pi.PermissionPrompt{}),
	)
	require.Equal(
		t,
		string(permissionOptionDeny),
		session.requestPermissionAnswer(
			t.Context(),
			pi.UIRequest{ID: "no-connection"},
			pi.PermissionPrompt{ToolCallID: "native-call-no-connection", ToolName: "bash"},
		),
	)

	permissionClient := newDialogStubClient()
	permissionClient.updateErr = errors.New("publish failed")
	agent.setConnection(permissionClient)
	require.Equal(
		t,
		string(permissionOptionDeny),
		session.requestPermissionAnswer(
			t.Context(),
			pi.UIRequest{ID: "publish-error"},
			pi.PermissionPrompt{ToolCallID: "native-call-publish-error", ToolName: "bash"},
		),
	)
	require.Empty(t, permissionClient.permissionRequests)
	require.False(t, session.turnTools["native-call-publish-error"].published)

	permissionClient.updateErr = nil
	permissionClient.permissionErr = errors.New("permission failed")
	require.Equal(
		t,
		string(permissionOptionDeny),
		session.requestPermissionAnswer(
			t.Context(),
			pi.UIRequest{ID: "error"},
			pi.PermissionPrompt{
				ToolCallID: "native-call-error",
				ToolName:   "surface_tool",
				Input:      json.RawMessage(`{"value":true}`),
			},
		),
	)
}

func TestRequestPermissionUsesExactNativeToolCallID(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	permissionClient := newDialogStubClient()
	permissionClient.permissionResponse = acp.RequestPermissionResponse{
		Outcome: acp.NewRequestPermissionOutcomeSelected(permissionOptionAllow),
	}
	agent.setConnection(permissionClient)
	session := &agentSession{agent: agent, id: "session"}

	answer := session.requestPermissionAnswer(
		t.Context(),
		pi.UIRequest{ID: "permission-1"},
		pi.PermissionPrompt{
			ToolCallID: "native-call-42",
			ToolName:   "bash",
			Input:      json.RawMessage(`{"command":"echo hi"}`),
		},
	)

	require.Equal(t, string(permissionOptionAllow), answer)
	require.Len(t, permissionClient.permissionRequests, 1)
	request := permissionClient.permissionRequests[0]
	require.Equal(t, acp.SessionId("session"), request.SessionId)
	require.Equal(t, acp.ToolCallId("native-call-42"), request.ToolCall.ToolCallId)
	require.NotEqual(t, acp.ToolCallId("bash"), request.ToolCall.ToolCallId)
	require.Equal(t, "bash", *request.ToolCall.Title)
	rawInput, ok := request.ToolCall.RawInput.(json.RawMessage)
	require.True(t, ok)
	require.JSONEq(t, `{"command":"echo hi"}`, string(rawInput))
	require.Equal(t, string(permissionOptionDeny), session.requestPermissionAnswer(
		t.Context(),
		pi.UIRequest{ID: "permission-duplicate"},
		pi.PermissionPrompt{ToolCallID: "native-call-42", ToolName: "bash"},
	))
	require.Len(t, permissionClient.permissionRequests, 1)
}

func TestPermissionPublishesPendingCallBeforeRequestAndNativeStartUpdatesIt(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	client := &orderedPermissionClient{dialogStubClient: newDialogStubClient()}
	client.permissionResponse = acp.RequestPermissionResponse{
		Outcome: acp.NewRequestPermissionOutcomeSelected(permissionOptionAllow),
	}
	agent.setConnection(client)
	native := newStubPiClient()
	session := &agentSession{agent: agent, id: "session", client: native}

	session.handleUIDialog(t.Context(), pi.UIRequest{
		ID:     "permission-1",
		Method: uiMethodSelect,
		Title: pi.PermissionTitleMarker +
			`{"toolCallId":"native-call-42","toolName":"bash","input":{"command":"echo hi"}}`,
	})

	require.Len(t, client.updatesBeforePermission, 1)
	pending := client.updatesBeforePermission[0].ToolCall
	require.NotNil(t, pending)
	require.Equal(t, acp.ToolCallId("native-call-42"), pending.ToolCallId)
	require.Equal(t, acp.ToolCallStatusPending, pending.Status)
	require.Len(t, client.permissionRequests, 1)
	require.Equal(t, pending.ToolCallId, client.permissionRequests[0].ToolCall.ToolCallId)

	_, err := session.handleTurnEvent(t.Context(), pi.ToolExecutionStartEvent{
		ToolCallID: "native-call-42",
		ToolName:   "bash",
		Args:       json.RawMessage(`{"command":"echo hi"}`),
	}, &promptTurnState{})
	require.NoError(t, err)

	starts := 0
	progressUpdates := 0
	for _, update := range client.updates {
		if update.ToolCall != nil {
			starts++
		}
		if update.ToolCallUpdate != nil && update.ToolCallUpdate.Status != nil &&
			*update.ToolCallUpdate.Status == acp.ToolCallStatusInProgress {
			progressUpdates++
			require.Equal(t, pending.ToolCallId, update.ToolCallUpdate.ToolCallId)
		}
	}
	require.Equal(t, 1, starts, "native start must not duplicate the pending tool call")
	require.Equal(t, 1, progressUpdates)
	state := session.turnTools["native-call-42"]
	require.NotNil(t, state)
	require.True(t, state.published)
	require.True(t, state.nativeStartPublished)
	require.False(t, state.terminalPublished)
}

func TestPermissionSerializationUIFirstDuringPendingPublication(t *testing.T) {
	previousProcs := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(previousProcs)

	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	client := newStrictPermissionClient()
	entered, release := client.gate("native-call", acp.ToolCallStatusPending)
	agent.setConnection(client)
	session := &agentSession{agent: agent, id: "session"}
	prompt := pi.PermissionPrompt{ToolCallID: "native-call", ToolName: "bash"}

	answerDone := make(chan string, 1)
	go func() {
		answerDone <- session.requestPermissionAnswer(
			t.Context(), pi.UIRequest{ID: "permission"}, prompt,
		)
	}()
	requireSignal(t, entered)

	startDone := make(chan error, 1)
	go func() {
		_, err := session.handleTurnEvent(t.Context(), pi.ToolExecutionStartEvent{
			ToolCallID: prompt.ToolCallID,
			ToolName:   prompt.ToolName,
		}, &promptTurnState{})
		startDone <- err
	}()
	runtime.Gosched()
	close(release)

	require.Equal(t, string(permissionOptionAllow), requireValue(t, answerDone))
	require.NoError(t, requireValue(t, startDone))
	requireStrictPermissionLifecycle(t, client, prompt.ToolCallID, 1, 1, 0)
}

func TestPermissionSerializationDoesNotBlockUnrelatedToolID(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	client := newStrictPermissionClient()
	entered, release := client.gate("permission-call", acp.ToolCallStatusPending)
	agent.setConnection(client)
	session := &agentSession{agent: agent, id: "session"}

	answerDone := make(chan string, 1)
	go func() {
		answerDone <- session.requestPermissionAnswer(
			t.Context(),
			pi.UIRequest{ID: "permission"},
			pi.PermissionPrompt{ToolCallID: "permission-call", ToolName: "bash"},
		)
	}()
	requireSignal(t, entered)

	startDone := make(chan error, 1)
	go func() {
		_, err := session.handleTurnEvent(t.Context(), pi.ToolExecutionStartEvent{
			ToolCallID: "unrelated-call",
			ToolName:   "read",
		}, &promptTurnState{})
		startDone <- err
	}()
	require.NoError(t, requireValue(t, startDone))
	close(release)
	require.Equal(t, string(permissionOptionAllow), requireValue(t, answerDone))
	requireStrictPermissionLifecycle(t, client, "permission-call", 1, 1, 0)
	requireStrictPermissionLifecycle(t, client, "unrelated-call", 1, 0, 0)
}

func TestPermissionSerializationNativeStartFirst(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	client := newStrictPermissionClient()
	agent.setConnection(client)
	session := &agentSession{agent: agent, id: "session"}
	prompt := pi.PermissionPrompt{ToolCallID: "native-call", ToolName: "bash"}

	_, err := session.handleTurnEvent(t.Context(), pi.ToolExecutionStartEvent{
		ToolCallID: prompt.ToolCallID,
		ToolName:   prompt.ToolName,
	}, &promptTurnState{})
	require.NoError(t, err)
	require.Equal(t, string(permissionOptionAllow), session.requestPermissionAnswer(
		t.Context(), pi.UIRequest{ID: "permission"}, prompt,
	))

	_, err = session.handleTurnEvent(t.Context(), pi.ToolExecutionStartEvent{
		ToolCallID: prompt.ToolCallID,
		ToolName:   prompt.ToolName,
	}, &promptTurnState{})
	require.NoError(t, err)
	requireStrictPermissionLifecycle(t, client, prompt.ToolCallID, 1, 1, 0)
}

func TestPermissionSerializationTerminalCannotOvertakeAdmission(t *testing.T) {
	previousProcs := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(previousProcs)

	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	client := newStrictPermissionClient()
	entered, release := client.gate("native-call", acp.ToolCallStatusPending)
	agent.setConnection(client)
	session := &agentSession{agent: agent, id: "session"}
	prompt := pi.PermissionPrompt{ToolCallID: "native-call", ToolName: "bash"}

	answerDone := make(chan string, 1)
	go func() {
		answerDone <- session.requestPermissionAnswer(
			t.Context(), pi.UIRequest{ID: "permission"}, prompt,
		)
	}()
	requireSignal(t, entered)

	terminalDone := make(chan error, 1)
	go func() {
		_, err := session.handleTurnEvent(t.Context(), pi.ToolExecutionEndEvent{
			ToolCallID: prompt.ToolCallID,
		}, &promptTurnState{})
		terminalDone <- err
	}()
	runtime.Gosched()
	close(release)

	require.Equal(t, string(permissionOptionAllow), requireValue(t, answerDone))
	require.NoError(t, requireValue(t, terminalDone))

	_, err := session.handleTurnEvent(t.Context(), pi.ToolExecutionEndEvent{
		ToolCallID: prompt.ToolCallID,
	}, &promptTurnState{})
	require.NoError(t, err)
	requireStrictPermissionLifecycle(t, client, prompt.ToolCallID, 1, 1, 1)
}

func TestPermissionSerializationConcurrentDeliveryStress(t *testing.T) {
	for iteration := range 128 {
		agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
		client := newStrictPermissionClient()
		agent.setConnection(client)
		session := &agentSession{agent: agent, id: "session"}
		prompt := pi.PermissionPrompt{ToolCallID: fmt.Sprintf("native-call-%d", iteration), ToolName: "bash"}
		start := make(chan struct{})
		answerDone := make(chan string, 1)
		startDone := make(chan error, 1)

		go func() {
			<-start
			answerDone <- session.requestPermissionAnswer(
				t.Context(), pi.UIRequest{ID: fmt.Sprintf("permission-%d", iteration)}, prompt,
			)
		}()
		go func() {
			<-start
			_, err := session.handleTurnEvent(t.Context(), pi.ToolExecutionStartEvent{
				ToolCallID: prompt.ToolCallID,
				ToolName:   prompt.ToolName,
			}, &promptTurnState{})
			startDone <- err
		}()
		close(start)

		require.Equal(t, string(permissionOptionAllow), requireValue(t, answerDone), "iteration %d", iteration)
		require.NoError(t, requireValue(t, startDone), "iteration %d", iteration)
		requireStrictPermissionLifecycle(t, client, prompt.ToolCallID, 1, 1, 0)
	}
}

func TestPermissionToolLifecycleEmitFailuresAndLateEvents(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	client := newDialogStubClient()
	client.permissionResponse = acp.RequestPermissionResponse{
		Outcome: acp.NewRequestPermissionOutcomeSelected(permissionOptionAllow),
	}
	agent.setConnection(client)
	session := &agentSession{agent: agent, id: "session"}
	prompt := pi.PermissionPrompt{ToolCallID: "native-call", ToolName: "bash"}

	require.Equal(t, string(permissionOptionAllow), session.requestPermissionAnswer(
		t.Context(), pi.UIRequest{ID: "permission"}, prompt,
	))
	client.updateErr = errors.New("in-progress update failed")
	_, err := session.handleTurnEvent(t.Context(), pi.ToolExecutionStartEvent{
		ToolCallID: prompt.ToolCallID,
		ToolName:   prompt.ToolName,
	}, &promptTurnState{})
	require.ErrorContains(t, err, "in-progress update failed")

	client.updateErr = nil
	_, err = session.handleTurnEvent(t.Context(), pi.ToolExecutionStartEvent{
		ToolCallID: prompt.ToolCallID,
		ToolName:   prompt.ToolName,
	}, &promptTurnState{})
	require.NoError(t, err)
	_, err = session.handleTurnEvent(t.Context(), pi.ToolExecutionEndEvent{
		ToolCallID: prompt.ToolCallID,
	}, &promptTurnState{})
	require.NoError(t, err)
	updatesBeforeLatePartial := len(client.updates)
	_, err = session.handleTurnEvent(t.Context(), pi.ToolExecutionUpdateEvent{
		ToolCallID:    prompt.ToolCallID,
		PartialResult: &pi.ToolResult{},
	}, &promptTurnState{})
	require.NoError(t, err)
	require.Len(t, client.updates, updatesBeforeLatePartial)

	terminalAgent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	terminalClient := newDirectAgentClient()
	terminalClient.updateErr = errors.New("terminal update failed")
	terminalAgent.setConnection(terminalClient)
	terminalSession := &agentSession{agent: terminalAgent, id: "session"}
	_, err = terminalSession.handleTurnEvent(t.Context(), pi.ToolExecutionEndEvent{
		ToolCallID: "terminal-before-start",
	}, &promptTurnState{})
	require.ErrorContains(t, err, "terminal update failed")

	terminalClient.updateErr = nil
	_, err = terminalSession.handleTurnEvent(t.Context(), pi.ToolExecutionEndEvent{
		ToolCallID: "terminal-before-start",
	}, &promptTurnState{})
	require.NoError(t, err)
	require.Equal(t, string(permissionOptionDeny), terminalSession.requestPermissionAnswer(
		t.Context(),
		pi.UIRequest{ID: "permission-after-terminal"},
		pi.PermissionPrompt{ToolCallID: "terminal-before-start", ToolName: "bash"},
	))
}

func requireStrictPermissionLifecycle(
	t *testing.T,
	client *strictPermissionClient,
	toolCallID string,
	wantStarts int,
	wantPermissions int,
	wantTerminals int,
) {
	t.Helper()

	updates, permissions := client.snapshot()
	starts := 0
	terminals := 0
	matchingPermissions := 0
	for _, update := range updates {
		if update.ToolCall != nil && update.ToolCall.ToolCallId == acp.ToolCallId(toolCallID) {
			starts++
		}

		if update.ToolCallUpdate != nil && update.ToolCallUpdate.ToolCallId == acp.ToolCallId(toolCallID) &&
			update.ToolCallUpdate.Status != nil &&
			(*update.ToolCallUpdate.Status == acp.ToolCallStatusCompleted ||
				*update.ToolCallUpdate.Status == acp.ToolCallStatusFailed) {
			terminals++
		}
	}

	for _, permission := range permissions {
		if permission.ToolCall.ToolCallId == acp.ToolCallId(toolCallID) {
			matchingPermissions++
		}
	}

	require.Equal(t, wantStarts, starts)
	require.Equal(t, wantPermissions, matchingPermissions)
	require.Equal(t, wantTerminals, terminals)
}

func requireSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()

	select {
	case <-signal:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for synchronization signal")
	}
}

func requireValue[T any](t *testing.T, values <-chan T) T {
	t.Helper()

	select {
	case value := <-values:
		return value
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for concurrent result")

		var zero T

		return zero
	}
}

func TestMalformedPermissionMarkerFailsClosed(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	permissionClient := newDialogStubClient()
	agent.setConnection(permissionClient)
	native := newStubPiClient()
	session := &agentSession{agent: agent, id: "session", client: native}

	requests := []pi.UIRequest{
		{ID: "missing", Method: uiMethodSelect, Title: pi.PermissionTitleMarker + `{"toolName":"bash"}`},
		{ID: "malformed", Method: uiMethodSelect, Title: pi.PermissionTitleMarker + `{"toolCallId":7,"toolName":"bash"}`},
		{
			ID:     "wrong-method",
			Method: uiMethodConfirm,
			Title:  pi.PermissionTitleMarker + `{"toolCallId":"native-call-42","toolName":"bash"}`,
		},
	}
	for _, request := range requests {
		session.handleUIDialog(t.Context(), request)
	}

	require.Empty(t, permissionClient.permissionRequests, "malformed markers must not reach ACP permissions")
	require.Empty(t, permissionClient.elicitationRequests, "malformed markers must not be reclassified as elicitation")
	require.Len(t, native.responses, len(requests))
	for index, request := range requests {
		require.Equal(t, pi.UICancelResponse(request.ID), native.responses[index])
	}
}

func TestRespondUIDialogFailures(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	session := &agentSession{agent: agent, id: "session"}

	session.respondUIDialog(t.Context(), pi.UICancelResponse("no-client"))
	native := newStubPiClient()
	native.respondErr = errors.New("respond failed")
	session.client = native
	session.respondUIDialog(t.Context(), pi.UICancelResponse("error"))
}
