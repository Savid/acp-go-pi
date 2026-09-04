//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	piacp "github.com/savid/acp-go-pi"
	"github.com/stretchr/testify/require"
)

type failingAppendStore struct {
	*piacp.InMemorySessionStore
	err error
}

func (s *failingAppendStore) Append(
	context.Context, piacp.SessionKey, []piacp.SessionStoreEntry,
) error {
	return s.err
}

// requireTurnFailure asserts the uniform native-turn-failure wire shape: a
// JSON-RPC -32603 error carrying data.error "pi_turn_failed" and the given
// cause — never a stop reason.
func requireTurnFailure(t *testing.T, err error, cause string) {
	t.Helper()

	var reqErr *acp.RequestError
	require.ErrorAs(t, err, &reqErr)
	require.Equal(t, -32603, reqErr.Code)

	encoded, marshalErr := json.Marshal(reqErr.Data)
	require.NoError(t, marshalErr)

	var data struct {
		Error string `json:"error"`
		Cause string `json:"cause"`
	}
	require.NoError(t, json.Unmarshal(encoded, &data))
	require.Equal(t, "pi_turn_failed", data.Error)
	require.Equal(t, cause, data.Cause)
}

func newFakeSession(
	t *testing.T,
	ctx context.Context,
	conn *acp.ClientSideConnection,
	opts ...piacp.SessionRequestOption,
) acp.SessionId {
	t.Helper()

	session, err := conn.NewSession(ctx, piacp.NewSessionRequest(integrationWorkspaceDir(t), opts...))
	require.NoError(t, err)
	require.NotEmpty(t, session.SessionId)

	return session.SessionId
}

func TestAgentFakePromptTurn(t *testing.T) {
	requireRunIntegration(t)
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	client := &recordingClient{}
	conn := connectFakeAgentForTest(t, ctx, client, fakeTurnScenario())
	sessionID := newFakeSession(t, ctx, conn)

	resp, err := conn.Prompt(ctx, piacp.TextPromptRequest(sessionID, "test-turn", "hello"))
	require.NoError(t, err)
	require.Equal(t, acp.StopReasonEndTurn, resp.StopReason)
	piMeta, ok := resp.Meta["pi"].(map[string]any)
	require.True(t, ok)
	require.Regexp(t, `^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`, piMeta["messageId"])
	require.Contains(t, client.text(), fakeDefaultReply)
	require.Positive(t, client.usageUpdateCount(),
		"a settled turn with harness-reported usage must emit a usage update")

	_, err = conn.CloseSession(ctx, acp.CloseSessionRequest{SessionId: sessionID})
	require.NoError(t, err)
}

func TestAgentFakeCancelDuringStream(t *testing.T) {
	requireRunIntegration(t)
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	scenario := fakeTurnScenario()
	scenario.DeltaTexts = []string{"one ", "two ", "three ", "four ", "five ", "six "}
	scenario.StreamDelayMs = 250

	client := &recordingClient{}
	store := piacp.NewInMemorySessionStore()
	conn := connectFakeAgentForTest(t, ctx, client, scenario, piacp.WithSessionStore(store))
	cwd := integrationWorkspaceDir(t)
	session, err := conn.NewSession(ctx, piacp.NewSessionRequest(cwd))
	require.NoError(t, err)
	sessionID := session.SessionId

	promptDone := make(chan acp.PromptResponse, 1)
	promptErr := make(chan error, 1)
	go func() {
		resp, err := conn.Prompt(ctx, piacp.TextPromptRequest(sessionID, "test-turn", "count slowly"))
		if err != nil {
			promptErr <- err

			return
		}
		promptDone <- resp
	}()

	require.Eventually(t, func() bool { return client.text() != "" },
		30*time.Second, 20*time.Millisecond, "no streamed chunk before cancel")

	require.NoError(t, conn.Cancel(ctx, piacp.CancelRequest(sessionID, "test-turn")))

	select {
	case resp := <-promptDone:
		require.Equal(t, acp.StopReasonCancelled, resp.StopReason,
			"user cancel maps to the cancelled stop reason, never a turn failure")
	case err := <-promptErr:
		t.Fatalf("cancelled prompt failed instead of stopping: %v", err)
	case <-time.After(30 * time.Second):
		t.Fatal("prompt did not return after cancel")
	}

	entries, err := store.Load(ctx, piacp.SessionKey{SessionID: string(sessionID)})
	require.NoError(t, err)
	require.NotEmpty(t, entries, "a cancelled turn is mirrored only after its native settle fence")
	require.Contains(t, string(entries[len(entries)-1]), `"stopReason":"aborted"`,
		"the committed generation must include pi's durable aborted assistant row")

	_, err = conn.CloseSession(ctx, acp.CloseSessionRequest{SessionId: sessionID})
	require.NoError(t, err)

	resumeClient := &recordingClient{}
	resumeConn := connectFakeAgentForTest(t, ctx, resumeClient, scenario, piacp.WithSessionStore(store))
	_, err = resumeConn.ResumeSession(ctx, piacp.ResumeSessionRequest(sessionID, cwd))
	require.NoError(t, err, "a first-turn cancel must leave a resumable native session")

	resp, err := resumeConn.Prompt(ctx, piacp.TextPromptRequest(sessionID, "test-turn-2", "continue after cancel"))
	require.NoError(t, err)
	require.Equal(t, acp.StopReasonEndTurn, resp.StopReason)
}

func TestAgentFakeSettledCancelMirrorFailureFailsPrompt(t *testing.T) {
	requireRunIntegration(t)
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	scenario := fakeTurnScenario()
	scenario.DeltaTexts = []string{"one ", "two ", "three ", "four "}
	scenario.StreamDelayMs = 250
	client := &recordingClient{}
	store := &failingAppendStore{
		InMemorySessionStore: piacp.NewInMemorySessionStore(),
		err:                  errors.New("cancel mirror unavailable"),
	}
	conn := connectFakeAgentForTest(t, ctx, client, scenario, piacp.WithSessionStore(store))
	sessionID := newFakeSession(t, ctx, conn)

	promptDone := make(chan error, 1)
	go func() {
		_, err := conn.Prompt(ctx, piacp.TextPromptRequest(sessionID, "test-turn", "count slowly"))
		promptDone <- err
	}()

	require.Eventually(t, func() bool { return client.text() != "" },
		30*time.Second, 20*time.Millisecond, "no streamed chunk before cancel")
	require.NoError(t, conn.Cancel(ctx, piacp.CancelRequest(sessionID, "test-turn")))

	// The failed durability fence fails the prompt rather than reporting a
	// cancelled success. Why the store refused is the operator's business, so
	// the client gets the closed internal error and never the store's own text.
	var reqErr *acp.RequestError
	err := <-promptDone
	require.ErrorAs(t, err, &reqErr,
		"a cancelled response must not hide its failed durability fence")
	require.Equal(t, -32603, reqErr.Code)
	require.Nil(t, reqErr.Data)
	require.NotContains(t, err.Error(), "cancel mirror unavailable")
}

func TestAgentFakeProviderErrorTurnFailure(t *testing.T) {
	requireRunIntegration(t)
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	scenario := fakeTurnScenario()
	scenario.PromptBehavior = fakeBehaviorProviderError
	scenario.ProviderError = "429 rate limited by fake provider"

	client := &recordingClient{}
	store := piacp.NewInMemorySessionStore()
	conn := connectFakeAgentForTest(t, ctx, client, scenario, piacp.WithSessionStore(store))
	sessionID := newFakeSession(t, ctx, conn)

	_, err := conn.Prompt(ctx, piacp.TextPromptRequest(sessionID, "test-turn", "fail please"))
	requireTurnFailure(t, err, "provider")
	entries, loadErr := store.Load(ctx, piacp.SessionKey{SessionID: string(sessionID)})
	require.NoError(t, loadErr)
	require.NotEmpty(t, entries, "a provider error is mirrored only after its native settle fence")

	// A provider failure leaves the session addressable.
	_, err = conn.Prompt(ctx, piacp.TextPromptRequest(sessionID, "test-turn", "again"))
	requireTurnFailure(t, err, "provider")
}

func TestAgentFakeProcessDeathTurnFailure(t *testing.T) {
	requireRunIntegration(t)
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	scenario := fakeTurnScenario()
	scenario.PromptBehavior = fakeBehaviorDie

	client := &recordingClient{}
	store := piacp.NewInMemorySessionStore()
	conn := connectFakeAgentForTest(t, ctx, client, scenario, piacp.WithSessionStore(store))
	sessionID := newFakeSession(t, ctx, conn)

	_, err := conn.Prompt(ctx, piacp.TextPromptRequest(sessionID, "test-turn", "die mid turn"))
	requireTurnFailure(t, err, "process_exit")
	entries, loadErr := store.Load(ctx, piacp.SessionKey{SessionID: string(sessionID)})
	require.NoError(t, loadErr)
	require.Empty(t, entries, "a transport/process failure before agent_settled must never be mirrored")
}

func TestAgentFakeTurnTimeout(t *testing.T) {
	requireRunIntegration(t)
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	scenario := fakeTurnScenario()
	scenario.PromptBehavior = fakeBehaviorHang

	client := &recordingClient{}
	conn := connectFakeAgentForTest(t, ctx, client, scenario, piacp.WithTurnTimeout(3*time.Second))
	sessionID := newFakeSession(t, ctx, conn)

	_, err := conn.Prompt(ctx, piacp.TextPromptRequest(sessionID, "test-turn", "hang forever"))
	requireTurnFailure(t, err, "timeout")
}

func TestAgentFakeGarbageBurstTerminalizesTheTurn(t *testing.T) {
	requireRunIntegration(t)
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	scenario := fakeTurnScenario()
	scenario.PromptBehavior = fakeBehaviorGarbageBurst
	scenario.DeltaTexts = []string{"first ", "second ", "third"}
	scenario.GarbageLines = 5

	client := &recordingClient{}
	conn := connectFakeAgentForTest(t, ctx, client, scenario)
	sessionID := newFakeSession(t, ctx, conn)

	// Native stdout is strict JSONL, so the first malformed record mid-turn is
	// the transport's terminal fact: the turn fails on it and no later record
	// is read. Chunks the client already received stand.
	_, err := conn.Prompt(ctx, piacp.TextPromptRequest(sessionID, "test-turn", "burst"))
	requireTurnFailure(t, err, "transport")
	require.Contains(t, client.text(), "first ")
	require.NotContains(t, client.text(), "third")
}

func TestAgentFakePermissionOutcomes(t *testing.T) {
	requireRunIntegration(t)
	t.Parallel()

	outcomes := []struct {
		name   string
		choice string
	}{
		{"allow", permissionChoiceAllow},
		{"deny", permissionChoiceDeny},
		{"cancel", permissionChoiceCancel},
	}

	for _, testCase := range outcomes {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()

			scenario := fakeTurnScenario()
			scenario.ToolName = "bash"
			scenario.ToolArgs = map[string]any{"command": "echo permission-probe"}

			client := &recordingClient{permissionChoice: testCase.choice}
			conn := connectFakeAgentForTest(t, ctx, client, scenario)
			sessionID := newFakeSession(t, ctx, conn)

			// A denied or cancelled dialog fails the tool call closed; the
			// turn itself still completes.
			resp, err := conn.Prompt(ctx, piacp.TextPromptRequest(sessionID, "test-turn", "use the tool"))
			require.NoError(t, err)
			require.Equal(t, acp.StopReasonEndTurn, resp.StopReason)
			require.Equal(t, 1, client.permissionCount())

			request := client.permissionSnapshot()[0]
			require.Equal(t, sessionID, request.SessionId)
			require.Equal(t, acp.ToolCallId("call_1"), request.ToolCall.ToolCallId)
			require.NotEqual(t, acp.ToolCallId("bash"), request.ToolCall.ToolCallId)
			require.NotEmpty(t, request.Options)

			var publishedToolCallID acp.ToolCallId
			for _, notification := range client.notificationSnapshot() {
				if notification.Update.ToolCall != nil {
					publishedToolCallID = notification.Update.ToolCall.ToolCallId
					break
				}
			}
			require.Equal(t, request.ToolCall.ToolCallId, publishedToolCallID,
				"permission request and published tool update must share the native id")
		})
	}
}

func TestAgentFakeMalformedPermissionMarkerFailsClosed(t *testing.T) {
	requireRunIntegration(t)
	t.Parallel()

	for _, testCase := range []struct {
		name    string
		payload string
	}{
		{name: "missing native id", payload: `{"toolName":"bash","input":{"command":"do not run"}}`},
		{name: "malformed native id", payload: `{"toolCallId":7,"toolName":"bash"}`},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()

			scenario := fakeTurnScenario()
			scenario.ToolName = "bash"
			scenario.ToolArgs = map[string]any{"command": "do not run"}
			scenario.ToolOutput = "MALFORMED_PERMISSION_EXECUTED"
			scenario.PermissionPayload = testCase.payload

			client := &recordingClient{permissionChoice: permissionChoiceAllow}
			conn := connectFakeAgentForTest(t, ctx, client, scenario)
			sessionID := newFakeSession(t, ctx, conn)

			resp, err := conn.Prompt(ctx, piacp.TextPromptRequest(sessionID, "test-turn", "use the tool"))
			require.NoError(t, err)
			require.Equal(t, acp.StopReasonEndTurn, resp.StopReason)
			require.Zero(t, client.permissionCount(), "malformed marker must not reach ACP permissions")
			require.Empty(t, client.elicitationSnapshot(), "malformed marker must not become elicitation")

			encoded, err := json.Marshal(client.notificationSnapshot())
			require.NoError(t, err)
			require.NotContains(t, string(encoded), scenario.ToolOutput,
				"native tool implementation must not execute after a malformed marker")
			require.Contains(t, string(encoded), "Denied by ACP client")
		})
	}
}

func TestAgentFakeElicitationRelay(t *testing.T) {
	requireRunIntegration(t)
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	scenario := fakeTurnScenario()
	scenario.ElicitMethod = "input"
	scenario.ElicitTitle = "Pick a code word"

	client := &recordingClient{elicitationValue: "ELICIT_VALUE_SENTINEL"}
	conn := connectAgentWithInitForTest(t, ctx, client, formElicitationInit(),
		piacp.WithExecutablePath(fakePiExecutable(t, scenario)),
		piacp.WithScratchDir(integrationScratchDir(t)),
	)
	sessionID := newFakeSession(t, ctx, conn)

	// The dialog answer travels back to pi and shapes the reply, pinning the
	// full relay round trip.
	resp, err := conn.Prompt(ctx, piacp.TextPromptRequest(sessionID, "test-turn", "ask me something"))
	require.NoError(t, err)
	require.Equal(t, acp.StopReasonEndTurn, resp.StopReason)
	require.Contains(t, client.text(), "ELICIT_VALUE_SENTINEL")

	elicitations := client.elicitationSnapshot()
	require.Len(t, elicitations, 1)
	require.NotNil(t, elicitations[0].Form)
	require.Contains(t, elicitations[0].Form.Message, "Pick a code word")
	require.Equal(t, []string{"value"}, elicitations[0].Form.RequestedSchema.Required)
}

func TestAgentFakeElicitationWithoutCapabilityFailsClosed(t *testing.T) {
	requireRunIntegration(t)
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	scenario := fakeTurnScenario()
	scenario.ElicitMethod = "input"
	scenario.ElicitTitle = "Pick a code word"

	client := &recordingClient{elicitationValue: "MUST_NOT_APPEAR"}
	conn := connectFakeAgentForTest(t, ctx, client, scenario)
	sessionID := newFakeSession(t, ctx, conn)

	// Without the form elicitation capability the dialog is auto-cancelled;
	// the turn itself still completes.
	resp, err := conn.Prompt(ctx, piacp.TextPromptRequest(sessionID, "test-turn", "ask me something"))
	require.NoError(t, err)
	require.Equal(t, acp.StopReasonEndTurn, resp.StopReason)
	require.Empty(t, client.elicitationSnapshot())
	require.Contains(t, client.text(), fakeElicitCancelledReply)
}

func TestAgentFakeRawEventsOptIn(t *testing.T) {
	requireRunIntegration(t)
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	client := &recordingClient{}
	conn := connectFakeAgentForTest(t, ctx, client, fakeTurnScenario())
	sessionID := newFakeSession(t, ctx, conn, piacp.WithSessionRawEvents(true))

	resp, err := conn.Prompt(ctx, piacp.TextPromptRequest(sessionID, "test-turn", "raw events please"))
	require.NoError(t, err)
	require.Equal(t, acp.StopReasonEndTurn, resp.StopReason)
	require.Positive(t, client.rawEventCount(), "raw-event opt-in must forward native event lines")
}

func TestAgentFakeMCPValidation(t *testing.T) {
	requireRunIntegration(t)
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	client := &recordingClient{}
	conn := connectFakeAgentForTest(t, ctx, client, fakeTurnScenario())

	sseServer := acp.McpServer{
		Sse: &acp.McpServerSseInline{
			Type:    "sse",
			Name:    "sse-server",
			Url:     "http://127.0.0.1:1/sse",
			Headers: []acp.HttpHeader{},
		},
	}
	_, err := conn.NewSession(ctx, piacp.NewSessionRequest(integrationWorkspaceDir(t),
		piacp.WithSessionMCPServers(sseServer)))
	require.Error(t, err, "SSE MCP transport is rejected at session start")

	_, err = conn.NewSession(ctx, piacp.NewSessionRequest(integrationWorkspaceDir(t),
		piacp.WithSessionMCPServers(
			piacp.StdioMCPServer("dup", "/bin/true", nil, nil),
			piacp.StdioMCPServer("dup", "/bin/true", nil, nil),
		)))
	require.Error(t, err, "duplicate MCP server names are rejected")

	_, err = conn.NewSession(ctx, piacp.NewSessionRequest(integrationWorkspaceDir(t),
		piacp.WithSessionMCPServers(piacp.StdioMCPServer("", "/bin/true", nil, nil))))
	require.Error(t, err, "empty MCP server names are rejected")

	sessionID := newFakeSession(t, ctx, conn,
		piacp.WithSessionMCPServers(
			piacp.StdioMCPServer("calc", "/bin/cat", nil, map[string]string{"MCP_TEST_ENV": "1"}),
			piacp.HTTPMCPServer("hq", "http://127.0.0.1:1/mcp", map[string]string{"Authorization": "Bearer x"}),
		))

	_, err = conn.CloseSession(ctx, acp.CloseSessionRequest{SessionId: sessionID})
	require.NoError(t, err)
}

func TestAgentFakeMCPConnectFailureFailsSessionStart(t *testing.T) {
	requireRunIntegration(t)
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	scenario := fakeTurnScenario()
	scenario.MCPStartupFailure = "MCP HTTP 401"

	client := &recordingClient{}
	conn := connectFakeAgentForTest(t, ctx, client, scenario)

	// A failing MCP server connection makes pi exit loudly at startup; the
	// wrapper maps that to a structured session/new failure.
	_, err := conn.NewSession(ctx, piacp.NewSessionRequest(integrationWorkspaceDir(t),
		piacp.WithSessionMCPServers(piacp.HTTPMCPServer("broken", "http://127.0.0.1:1/mcp", nil))))
	require.Error(t, err)
}

func TestAgentFakeCloseAndDeleteDeterministic(t *testing.T) {
	requireRunIntegration(t)
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	client := &recordingClient{}
	store := piacp.NewInMemorySessionStore()
	conn := connectFakeAgentForTest(t, ctx, client, fakeTurnScenario(), piacp.WithSessionStore(store))
	sessionID := newFakeSession(t, ctx, conn)

	_, err := conn.Prompt(ctx, piacp.TextPromptRequest(sessionID, "test-turn", "persist me"))
	require.NoError(t, err)

	_, err = conn.CloseSession(ctx, acp.CloseSessionRequest{SessionId: sessionID})
	require.NoError(t, err)

	_, err = conn.UnstableDeleteSession(ctx, piacp.DeleteSessionRequest(sessionID))
	require.NoError(t, err)

	// A deleted session is tombstoned: session-scoped requests return the
	// uniform invalid-params error.
	_, err = conn.Prompt(ctx, piacp.TextPromptRequest(sessionID, "test-turn", "gone"))
	var reqErr *acp.RequestError
	require.ErrorAs(t, err, &reqErr)
	require.Equal(t, -32602, reqErr.Code)

	list, err := conn.ListSessions(ctx, piacp.ListSessionsRequest())
	require.NoError(t, err)
	for _, summary := range list.Sessions {
		require.NotEqual(t, sessionID, summary.SessionId, "deleted sessions must not be listed")
	}
}
