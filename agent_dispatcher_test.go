package piacp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"

	"github.com/savid/acp-go-pi/internal/lifecycle"
	"github.com/savid/acp-go-pi/internal/pi"
)

type dispatcherParams struct {
	Value string `json:"value"`
}

type actionWriteFailureWriter struct {
	err   error
	short bool
}

type actionPanicWriter struct {
	written *[]byte
}

func (w actionPanicWriter) Write(data []byte) (int, error) {
	if w.written != nil {
		*w.written = append(*w.written, data...)
	}

	panic("transport writer secret")
}

func (w actionWriteFailureWriter) Write(data []byte) (int, error) {
	if w.short {
		return len(data) - 1, nil
	}

	return 0, w.err
}

func (p *dispatcherParams) Validate() error {
	if p.Value == "" {
		return errors.New("value is required")
	}

	return nil
}

type dispatcherClient struct {
	done         chan struct{}
	updateErr    error
	updateCtxErr error
}

type blockingOpeningClient struct {
	*directAgentClient
	entered  chan struct{}
	release  chan struct{}
	returned chan struct{}
	once     sync.Once
}

func (c *blockingOpeningClient) SessionUpdate(ctx context.Context, notification acp.SessionNotification) error {
	blocked := false
	c.once.Do(func() {
		blocked = true
		close(c.entered)
	})
	if blocked {
		<-c.release
		close(c.returned)
	}
	if err := context.Cause(ctx); err != nil {
		return err
	}

	return c.directAgentClient.SessionUpdate(ctx, notification)
}

func (c *dispatcherClient) Done() <-chan struct{} {
	return c.done
}

func (*dispatcherClient) CreateElicitation(
	ctx context.Context,
	_ acp.UnstableCreateElicitationRequest,
	_ elicitationScope,
) (acp.UnstableCreateElicitationResponse, error) {
	acknowledgeActionRequestWrite(ctx, nil)

	return acp.UnstableCreateElicitationResponse{}, nil
}

func (*dispatcherClient) RequestPermission(
	ctx context.Context,
	_ acp.RequestPermissionRequest,
) (acp.RequestPermissionResponse, error) {
	acknowledgeActionRequestWrite(ctx, nil)

	return acp.RequestPermissionResponse{}, nil
}

func (c *dispatcherClient) SessionUpdate(ctx context.Context, _ acp.SessionNotification) error {
	c.updateCtxErr = ctx.Err()

	return c.updateErr
}

func (*dispatcherClient) NotifyExtension(context.Context, string, any) error {
	return nil
}

func TestLocalAgentConnectionDispatchValidation(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	conn := &localAgentConnection{agent: agent, hooks: &postResponseHooks{}}

	result, reqErr := conn.handle(t.Context(), acp.AgentMethodLogout, json.RawMessage(`{}`))
	require.Nil(t, result)
	require.Equal(t, -32600, reqErr.Code)

	result, reqErr = conn.handle(t.Context(), acp.AgentMethodInitialize, json.RawMessage(`{`))
	require.Nil(t, result)
	require.Equal(t, -32602, reqErr.Code)
	require.False(t, conn.initialized.Load())

	params, err := json.Marshal(defaultInitializeRequest())
	require.NoError(t, err)
	result, reqErr = conn.handle(t.Context(), acp.AgentMethodInitialize, params)
	require.Nil(t, reqErr)
	require.IsType(t, acp.InitializeResponse{}, result)
	require.True(t, conn.initialized.Load())

	result, reqErr = conn.handle(t.Context(), "unknown/method", json.RawMessage(`{}`))
	require.Nil(t, result)
	require.Equal(t, -32601, reqErr.Code)
}

func TestLocalAgentConnectionDispatchFailsClosedAfterAgentClose(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	conn := &localAgentConnection{agent: agent, hooks: &postResponseHooks{}}
	conn.initialized.Store(true)

	agent.mu.Lock()
	agent.closed = true
	agent.mu.Unlock()

	for _, test := range []struct {
		method string
		params json.RawMessage
	}{
		{method: "unknown/method", params: json.RawMessage(`{}`)},
		{method: acp.AgentMethodSessionPrompt, params: json.RawMessage(`{`)},
		{method: "_pi/unknown", params: json.RawMessage(`{`)},
	} {
		result, reqErr := conn.handle(t.Context(), test.method, test.params)
		require.Nil(t, result)
		require.Equal(t, -32600, reqErr.Code)
	}
}

func TestLocalAgentHandlersDecodeAndCallErrors(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	response := localResponse(func(_ *Agent, _ context.Context, params dispatcherParams) (string, error) {
		if params.Value == "fail" {
			return "", errors.New("response failed")
		}

		return params.Value, nil
	})

	result, reqErr := response(t.Context(), agent, json.RawMessage(`{`))
	require.Nil(t, result)
	require.Equal(t, -32602, reqErr.Code)

	result, reqErr = response(t.Context(), agent, json.RawMessage(`{}`))
	require.Nil(t, result)
	require.Equal(t, -32602, reqErr.Code)

	result, reqErr = response(t.Context(), agent, json.RawMessage(`{"value":"fail"}`))
	require.Nil(t, result)
	require.Equal(t, -32603, reqErr.Code)

	result, reqErr = response(t.Context(), agent, json.RawMessage(`{"value":"ok"}`))
	require.Equal(t, "ok", result)
	require.Nil(t, reqErr)

	notification := localNotification(func(_ *Agent, _ context.Context, params dispatcherParams) error {
		if params.Value == "fail" {
			return errors.New("notification failed")
		}

		return nil
	})

	result, reqErr = notification(t.Context(), agent, json.RawMessage(`{}`))
	require.Nil(t, result)
	require.Equal(t, -32602, reqErr.Code)

	result, reqErr = notification(t.Context(), agent, json.RawMessage(`{"value":"fail"}`))
	require.Nil(t, result)
	require.Equal(t, -32603, reqErr.Code)

	result, reqErr = notification(t.Context(), agent, json.RawMessage(`{"value":"ok"}`))
	require.Nil(t, result)
	require.Nil(t, reqErr)
}

func TestLifecycleCommandSessionIDEdges(t *testing.T) {
	tests := []struct {
		name   string
		method string
		params json.RawMessage
		result any
		wantID acp.SessionId
		wantOK bool
	}{
		{name: "new response", method: acp.AgentMethodSessionNew, result: acp.NewSessionResponse{SessionId: "new"}, wantID: "new", wantOK: true},
		{name: "new wrong response", method: acp.AgentMethodSessionNew, result: struct{}{}},
		{name: "new empty response", method: acp.AgentMethodSessionNew, result: acp.NewSessionResponse{}},
		{name: "load request", method: acp.AgentMethodSessionLoad, params: json.RawMessage(`{"sessionId":"load"}`), wantID: "load", wantOK: true},
		{name: "load malformed", method: acp.AgentMethodSessionLoad, params: json.RawMessage(`{`)},
		{name: "load empty", method: acp.AgentMethodSessionLoad, params: json.RawMessage(`{}`)},
		{name: "resume request", method: acp.AgentMethodSessionResume, params: json.RawMessage(`{"sessionId":"resume"}`), wantID: "resume", wantOK: true},
		{name: "resume malformed", method: acp.AgentMethodSessionResume, params: json.RawMessage(`{`)},
		{name: "resume empty", method: acp.AgentMethodSessionResume, params: json.RawMessage(`{}`)},
		{name: "fork response", method: ForkSessionMethod, result: acp.UnstableForkSessionResponse{SessionId: "fork"}, wantID: "fork", wantOK: true},
		{name: "fork wrong response", method: ForkSessionMethod, result: struct{}{}},
		{name: "fork empty response", method: ForkSessionMethod, result: acp.UnstableForkSessionResponse{}},
		{name: "unrelated", method: acp.AgentMethodSessionPrompt},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			gotID, gotOK := lifecycleCommandSessionID(test.method, test.params, test.result)
			require.Equal(t, test.wantID, gotID)
			require.Equal(t, test.wantOK, gotOK)
		})
	}
}

func TestPostResponseHookRequestReaderBuffersFinalLine(t *testing.T) {
	input := `{"jsonrpc":"2.0","id":"request-1","method":"session/new","params":{}}`
	reader := newPostResponseHookRequestReader(strings.NewReader(input))

	var output bytes.Buffer
	buffer := make([]byte, 3)
	_, err := io.CopyBuffer(&output, reader, buffer)
	require.NoError(t, err)
	require.NotContains(t, output.String(), "\n")

	var message struct {
		Params map[string]string `json:"params"`
	}
	require.NoError(t, json.Unmarshal(output.Bytes(), &message))
	require.Equal(t, `"request-1"`, message.Params[postResponseHookIDParam])

	n, err := reader.Read(buffer)
	require.Zero(t, n)
	require.ErrorIs(t, err, io.EOF)
}

func TestTagPostResponseHookRequestEdges(t *testing.T) {
	unchanged := [][]byte{
		[]byte(`{`),
		[]byte(`{"jsonrpc":"2.0","method":"session/new","params":{}}`),
		[]byte(`{"jsonrpc":"2.0","id":1,"method":"session/prompt","params":{}}`),
		[]byte(`{"jsonrpc":"2.0","id":1,"method":"session/new"}`),
		[]byte(`{"jsonrpc":"2.0","id":1,"method":"session/new","params":[]}`),
		[]byte(`{"jsonrpc":"2.0","id":1,"method":"session/new","params":null}`),
	}
	for _, input := range unchanged {
		require.Equal(t, input, tagPostResponseHookRequest(input))
	}

	for _, input := range [][]byte{
		[]byte("{\"jsonrpc\":\"2.0\",\"id\":7,\"method\":\"session/load\",\"params\":{}}"),
		[]byte("{\"jsonrpc\":\"2.0\",\"id\":7,\"method\":\"session/resume\",\"params\":{}}\n"),
		[]byte("{\"jsonrpc\":\"2.0\",\"id\":7,\"method\":\"_pi/session/fork\",\"params\":{}}\n"),
	} {
		tagged := tagPostResponseHookRequest(input)
		require.Equal(t, bytes.HasSuffix(input, []byte("\n")), bytes.HasSuffix(tagged, []byte("\n")))

		var message struct {
			Params map[string]string `json:"params"`
		}
		require.NoError(t, json.Unmarshal(tagged, &message))
		require.Equal(t, "7", message.Params[postResponseHookIDParam])
	}
}

// TestPostResponseHookRequestIDSurvivesARealRequestsParams pins the tag against
// the params an establishing request actually carries. mcpServers is mandatory
// and _meta is an object, so a reader that expected every value to be a string
// recovered no tag at all — and a session whose hook never ran emitted no
// opening snapshot, leaving its host waiting on an incarnation nobody would
// ever name.
func TestPostResponseHookRequestIDSurvivesARealRequestsParams(t *testing.T) {
	tagged := tagPostResponseHookRequest([]byte(
		`{"jsonrpc":"2.0","id":9,"method":"session/new","params":` +
			`{"cwd":"/tmp/work","mcpServers":[],"_meta":{"lifecycle":{"version":1}}}}`,
	))

	var msg struct {
		Params json.RawMessage `json:"params"`
	}
	require.NoError(t, json.Unmarshal(tagged, &msg))
	require.Equal(t, "9", postResponseHookRequestID(msg.Params))

	require.Empty(t, postResponseHookRequestID(json.RawMessage(`{"cwd":"/tmp/work"}`)))
	require.Empty(t, postResponseHookRequestID(json.RawMessage(
		`{"`+postResponseHookIDParam+`":7}`,
	)))
	require.Empty(t, postResponseHookRequestID(json.RawMessage(`[]`)))
}

func TestPostResponseHooksMatchSuccessfulResponses(t *testing.T) {
	hooks := &postResponseHooks{log: slog.New(slog.DiscardHandler)}
	hooks.runAfterResponseWrite([]byte(`{`))
	(&postResponseHooks{}).runAfterResponseWrite([]byte(`{`))

	firstRan := make(chan struct{})
	secondRan := make(chan struct{})
	hooks.enqueue("1", func() (func(), bool) { return func() { close(firstRan) }, true })
	hooks.enqueue("2", func() (func(), bool) { return func() { close(secondRan) }, true })

	hooks.runAfterResponseWrite([]byte(`{"jsonrpc":"2.0","id":2,"error":{"code":-32603}}`))
	hooks.runAfterResponseWrite([]byte(`{"jsonrpc":"2.0","result":{}}`))
	hooks.runAfterResponseWrite([]byte(`{"jsonrpc":"2.0","id":3,"result":{}}`))
	hooks.runAfterResponseWrite([]byte(`{"jsonrpc":"2.0","id":2,"result":{}}`))

	select {
	case <-secondRan:
	case <-time.After(time.Second):
		t.Fatal("second hook did not run")
	}
	select {
	case <-firstRan:
		t.Fatal("non-matching hook ran")
	default:
	}

	hooks.mu.Lock()
	require.Len(t, hooks.all, 1)
	require.Equal(t, "1", hooks.all[0].responseID)
	hooks.mu.Unlock()

	hooks.runAfterResponseWrite([]byte(`{"jsonrpc":"2.0","id":1,"result":{}}`))
	select {
	case <-firstRan:
	case <-time.After(time.Second):
		t.Fatal("first hook did not run")
	}

	hooks.enqueue("4", func() (func(), bool) { return nil, false })
	hooks.runAfterResponseWrite([]byte(`{"jsonrpc":"2.0","id":4,"result":{}}`))
	hooks.mu.Lock()
	require.Empty(t, hooks.all)
	hooks.mu.Unlock()
}

func TestLifecycleCommandHookErrors(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)

	missingAgent := NewAgent(WithLogger(logger))
	missing := &localAgentConnection{agent: missingAgent, hooks: &postResponseHooks{}}
	missing.enqueueLifecycleCommandHook(t.Context(), acp.AgentMethodSessionLoad, json.RawMessage(
		`{"sessionId":"missing","_acp_go_pi_post_response_hook_id":"response"}`,
	), nil)
	require.Len(t, missing.hooks.all, 1)
	_, admitted := missing.hooks.all[0].admit()
	require.False(t, admitted)

	untagged := &localAgentConnection{agent: missingAgent, hooks: &postResponseHooks{}}
	untagged.enqueueLifecycleCommandHook(t.Context(), acp.AgentMethodSessionLoad, json.RawMessage(
		`{"sessionId":"untagged"}`,
	), nil)
	require.Empty(t, untagged.hooks.all)

	updateErr := errors.New("update failed")
	client := &dispatcherClient{done: make(chan struct{}), updateErr: updateErr}
	agent := NewAgent(WithLogger(logger))
	agent.conn = client
	session := &agentSession{
		agent: agent,
		id:    "session",
		availableCommands: []pi.SlashCommand{{
			Name:        "help",
			Description: "Show help",
		}},
	}
	session.outbox = newTestSessionOutbox(1)
	session.outbox.generationDone = t.Context().Done()
	agent.sessions["session"] = session
	conn := &localAgentConnection{agent: agent, hooks: &postResponseHooks{}}
	canceledCtx, cancel := context.WithCancel(t.Context())
	cancel()
	conn.enqueueLifecycleCommandHook(canceledCtx, acp.AgentMethodSessionLoad, json.RawMessage(
		`{"sessionId":"session","_acp_go_pi_post_response_hook_id":"response"}`,
	), nil)
	require.Len(t, conn.hooks.all, 1)
	run, admitted := conn.hooks.all[0].admit()
	require.True(t, admitted)
	run()
	require.NoError(t, client.updateCtxErr)

	closedSession := &agentSession{agent: agent, id: "closed-session", closing: true}
	closedSession.outbox = newTestSessionOutbox(2)
	agent.sessions[closedSession.id] = closedSession
	closedConn := &localAgentConnection{agent: agent, hooks: &postResponseHooks{}}
	closedConn.enqueueLifecycleCommandHook(t.Context(), acp.AgentMethodSessionLoad, json.RawMessage(
		`{"sessionId":"closed-session","_acp_go_pi_post_response_hook_id":"closed"}`,
	), nil)
	require.Len(t, closedConn.hooks.all, 1)
	_, admitted = closedConn.hooks.all[0].admit()
	require.False(t, admitted)
}

func TestPostResponseOpenRetiredBeforeHookRuns(t *testing.T) {
	logs := &lockedLogBuffer{}
	agent := NewAgent(WithLogger(slog.New(slog.NewTextHandler(logs, nil))))
	generationCtx, cancelGeneration := context.WithCancel(t.Context())
	defer cancelGeneration()
	session := &agentSession{agent: agent, id: "session", outbox: newTestSessionOutbox(1)}
	session.outbox.generationDone = generationCtx.Done()
	agent.sessions[session.id] = session
	conn := &localAgentConnection{agent: agent, hooks: &postResponseHooks{}}
	conn.enqueueLifecycleCommandHook(t.Context(), acp.AgentMethodSessionLoad, json.RawMessage(
		`{"sessionId":"session","_acp_go_pi_post_response_hook_id":"response"}`,
	), nil)
	cancelGeneration()
	run, admitted := conn.hooks.all[0].admit()
	require.True(t, admitted)
	run()
	require.Empty(t, logs.String())
	require.False(t, session.opened)
	require.NoError(t, session.outbox.producers.waitChildren(t.Context()))
}

func TestBlockedPostResponseHookCannotVetoCloseOrContinueAfterRelease(t *testing.T) {
	originalWait := sessionCloseTurnWaitContext
	sessionCloseTurnWaitContext = func(ctx context.Context) (context.Context, context.CancelFunc) {
		bounded, cancel := context.WithCancel(ctx)
		cancel()

		return bounded, func() {}
	}
	t.Cleanup(func() { sessionCloseTurnWaitContext = originalWait })

	host := &blockingOpeningClient{
		directAgentClient: newDirectAgentClient(),
		entered:           make(chan struct{}),
		release:           make(chan struct{}),
		returned:          make(chan struct{}),
	}
	agent := NewAgent(testContainmentOption(), WithLogger(slog.New(slog.DiscardHandler)))
	agent.conn = host
	process := newStubProcess(false)
	native := newStubPiClient()
	session := &agentSession{agent: agent, id: "session", proc: process, client: native}
	outbox := bindTestEstablishingOutbox(session, 1, process, native)
	generationCtx, cancelGeneration := context.WithCancel(context.Background())
	outbox.generationDone = generationCtx.Done()
	outbox.pumpCancel = cancelGeneration
	agent.sessions[session.id] = session

	conn := &localAgentConnection{agent: agent, hooks: &postResponseHooks{log: agent.log}}
	conn.enqueueLifecycleCommandHook(t.Context(), acp.AgentMethodSessionLoad, json.RawMessage(
		`{"sessionId":"session","_acp_go_pi_post_response_hook_id":"1"}`,
	), nil)
	conn.hooks.runAfterResponseWrite([]byte(`{"jsonrpc":"2.0","id":1,"result":{}}`))
	<-host.entered

	require.ErrorIs(t, session.Close(t.Context()), ErrContainmentIncomplete)
	require.Equal(t, 1, process.shutdownCalls)
	require.Equal(t, 1, process.closeCalls)
	require.Nil(t, session.lc.stream)
	close(host.release)
	<-host.returned
	require.NoError(t, outbox.producers.waitChildren(t.Context()))
	require.Nil(t, session.lc.stream, "late hook release opened lifecycle state")
}

func TestLocalAgentConnectionClientCallErrors(t *testing.T) {
	agent := NewAgent(
		WithLogger(slog.New(slog.DiscardHandler)),
		WithConcurrencyLimits(ConcurrencyLimits{MaxConcurrentClientCalls: 1}),
	)
	conn := &localAgentConnection{agent: agent}

	_, err := conn.CreateElicitation(t.Context(), acp.UnstableCreateElicitationRequest{}, elicitationScope{})
	require.Error(t, err)

	agent.clientCalls = make(chan struct{}, 1)
	agent.clientCalls <- struct{}{}
	form := &acp.UnstableCreateElicitationForm{
		Message: "message",
		Mode:    elicitationModeForm,
	}
	_, err = conn.CreateElicitation(t.Context(), acp.UnstableCreateElicitationRequest{Form: form}, elicitationScope{
		SessionID: "session", TurnNonce: "turn", RequestID: "req",
	})
	require.Error(t, err)
	_, err = conn.RequestPermission(t.Context(), acp.RequestPermissionRequest{})
	require.Error(t, err)
	require.Error(t, conn.SessionUpdate(t.Context(), acp.SessionNotification{}))
	require.Error(t, conn.NotifyExtension(t.Context(), "_pi/test", map[string]any{}))
	require.Error(t, conn.NotifyExtension(t.Context(), "", nil))
	require.Error(t, conn.NotifyExtension(t.Context(), "pi/test", nil))
	<-agent.clientCalls

	peerInput, peerOutput := io.Pipe()
	var output bytes.Buffer
	conn.conn = acp.NewConnection(
		func(context.Context, string, json.RawMessage) (any, *acp.RequestError) { return nil, nil },
		&output,
		peerInput,
	)
	conn.conn.SetLogger(slog.New(slog.DiscardHandler))
	t.Cleanup(func() {
		require.NoError(t, peerOutput.Close())
		select {
		case <-conn.conn.Done():
		case <-time.After(time.Second):
			t.Fatal("connection did not stop")
		}
	})

	require.NoError(t, conn.NotifyExtension(t.Context(), "_pi/test", map[string]any{"ok": true}))
	require.Contains(t, output.String(), `"method":"_pi/test"`)
}

func TestActionRequestWriteTracking(t *testing.T) {
	meta := map[string]any{lifecycleMetaKey: map[string]any{
		"action": map[string]any{"actionId": "action"},
	}}
	require.Equal(t, "action", lifecycleActionID(meta))
	require.Empty(t, lifecycleActionID(nil))
	require.Empty(t, lifecycleActionID(map[string]any{lifecycleMetaKey: func() {}}))
	require.Empty(t, lifecycleActionIDFromRaw(json.RawMessage(`not-json`)))
	require.Equal(t, meta, elicitationMeta(acp.UnstableCreateElicitationRequest{
		Form: &acp.UnstableCreateElicitationForm{Meta: meta},
	}))
	require.Equal(t, meta, elicitationMeta(acp.UnstableCreateElicitationRequest{
		Url: &acp.UnstableCreateElicitationUrl{Meta: meta},
	}))
	require.Nil(t, elicitationMeta(acp.UnstableCreateElicitationRequest{}))

	requests := newActionRequestWrites()
	ack := make(chan error, 1)
	require.NoError(t, requests.register("action", func(err error) { ack <- err }))
	require.Error(t, requests.register("action", func(error) {}))

	payload, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  acp.ClientMethodSessionRequestPermission,
		"params":  map[string]any{"_meta": meta},
	})
	require.NoError(t, err)
	writer := &actionRequestWriteWriter{
		writer:   actionWriteFailureWriter{short: true},
		requests: requests,
	}
	_, err = writer.Write(payload)
	require.ErrorIs(t, err, io.ErrShortWrite)
	require.ErrorIs(t, <-ack, io.ErrShortWrite)
	requests.resolve("unknown", nil)

	for _, test := range []struct {
		name        string
		fullWrite   bool
		wantWritten bool
	}{
		{name: "panic before request bytes"},
		{name: "panic after full request", fullWrite: true, wantWritten: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			requests := newActionRequestWrites()
			acked := make(chan error, 1)
			require.NoError(t, requests.register("action", func(err error) { acked <- err }))
			var written []byte
			var sink *[]byte
			if test.fullWrite {
				sink = &written
			}
			writer := &actionRequestWriteWriter{writer: actionPanicWriter{written: sink}, requests: requests}

			n, panicErr := writer.Write(payload)
			require.Zero(t, n)
			require.ErrorIs(t, panicErr, errLifecycleActionRequest)
			require.ErrorIs(t, <-acked, errLifecycleActionRequest)
			require.NotContains(t, panicErr.Error(), "transport writer secret")
			if test.wantWritten {
				require.Equal(t, payload, written)
			} else {
				require.Empty(t, written)
			}
		})
	}

	require.Empty(t, outboundLifecycleActionID(json.RawMessage(`not-json`)))
	require.Empty(t, outboundLifecycleActionID(json.RawMessage(`{"id":1,"method":"other"}`)))
}

func TestLocalActionRequestsFailClosedAtTheWriteBarrier(t *testing.T) {
	meta := map[string]any{lifecycleMetaKey: map[string]any{
		"action": map[string]any{"actionId": "action"},
	}}

	t.Run("invalid registrations", func(t *testing.T) {
		acked := make(chan error, 3)
		ctx := context.WithValue(t.Context(), actionRequestWriteAckKey{}, func(err error) { acked <- err })

		connection := &localAgentConnection{agent: NewAgent(), requestWrites: newActionRequestWrites()}
		_, err := connection.RequestPermission(ctx, acp.RequestPermissionRequest{})
		require.ErrorContains(t, err, "missing its lifecycle action id")
		require.ErrorContains(t, <-acked, "missing its lifecycle action id")

		require.NoError(t, connection.requestWrites.register("action", func(error) {}))
		_, err = connection.RequestPermission(ctx, acp.RequestPermissionRequest{Meta: meta})
		require.ErrorContains(t, err, "already awaits a write")
		require.ErrorContains(t, <-acked, "already awaits a write")

		_, err = connection.CreateElicitation(ctx, acp.UnstableCreateElicitationRequest{
			Form: &acp.UnstableCreateElicitationForm{Meta: meta, Mode: elicitationModeForm},
		}, elicitationScope{SessionID: "session", TurnNonce: "turn"})
		require.ErrorContains(t, err, "already awaits a write")
		require.ErrorContains(t, <-acked, "already awaits a write")
	})

	for name, call := range map[string]func(context.Context, *localAgentConnection) error{
		"permission": func(ctx context.Context, connection *localAgentConnection) error {
			_, err := connection.RequestPermission(ctx, acp.RequestPermissionRequest{Meta: meta})

			return err
		},
		"elicitation": func(ctx context.Context, connection *localAgentConnection) error {
			_, err := connection.CreateElicitation(ctx, acp.UnstableCreateElicitationRequest{
				Form: &acp.UnstableCreateElicitationForm{Meta: meta, Mode: elicitationModeForm},
			}, elicitationScope{SessionID: "session", TurnNonce: "turn", RequestID: "request"})

			return err
		},
	} {
		t.Run(name+" write failure", func(t *testing.T) {
			input, peer := io.Pipe()
			writeErr := errors.New("write failed")
			connection := newLocalAgentConnection(NewAgent(), actionWriteFailureWriter{err: writeErr}, input)
			t.Cleanup(func() {
				require.NoError(t, peer.Close())
				select {
				case <-connection.Done():
				case <-time.After(time.Second):
					t.Fatal("connection did not stop")
				}
			})

			acked := make(chan error, 1)
			ctx := context.WithValue(t.Context(), actionRequestWriteAckKey{}, func(err error) { acked <- err })
			require.Error(t, call(ctx, connection))
			require.ErrorIs(t, <-acked, writeErr)
		})
	}
}

// lifecycleWireConnection exercises Serve and the SDK decoder over pipes. The
// addressable session has no native process; every case stops at validation.
func lifecycleWireConnection(t *testing.T, opts ...Option) *acp.Connection {
	t.Helper()
	previous := newServeAgent
	t.Cleanup(func() { newServeAgent = previous })
	var starts atomic.Int32
	newServeAgent = func(options ...Option) *Agent {
		agent := NewAgent(options...)
		agent.sessions["session"] = &agentSession{agent: agent, id: "session"}
		agent.startPiProcess = func(context.Context, pi.LaunchSpec) (piProcess, piClient, error) {
			starts.Add(1)

			return nil, nil, errAgentClosed
		}

		return agent
	}

	input, write := io.Pipe()
	read, output := io.Pipe()
	ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 5*time.Second)
	done := make(chan error, 1)
	options := append([]Option{WithLogger(slog.New(slog.DiscardHandler)), WithScratchDir(t.TempDir())}, opts...)
	go func() { done <- Serve(ctx, input, output, options...) }()
	connection := acp.NewConnection(func(context.Context, string, json.RawMessage) (any, *acp.RequestError) {
		return nil, nil
	}, write, read)
	connection.SetLogger(slog.New(slog.DiscardHandler))
	t.Cleanup(func() {
		_ = write.Close()
		_ = input.Close()
		_ = output.Close()
		_ = read.Close()
		select {
		case <-done:
		case <-ctx.Done():
			t.Error("Serve did not stop")
		}
		cancel()
		require.Zero(t, starts.Load(), "validation launched a native process")
	})

	return connection
}

func TestServeLifecycleOfferRetainsWireValues(t *testing.T) {
	for _, test := range []struct {
		name    string
		members string
		field   string
		offered bool
	}{
		{"valid", `"_meta":{"acp-go.dev/lifecycle":{"version":1}}`, "", true},
		{"absent", `"_meta":{"foreign":{"version":2,"version":1}}`, "", false},
		{"foreign duplicate envelopes", `"_meta":{"foreign":1},"_meta":{"foreign":2}`, "", false},
		{"foreign duplicate beside offer", `"_meta":{"foreign":1,"foreign":2,"acp-go.dev/lifecycle":{"version":1}}`, "", true},
		{"duplicate version", `"_meta":{"acp-go.dev/lifecycle":{"version":2,"version":1}}`, lifecycle.MetaPath + ".version", false},
		{"rounded fraction", `"_meta":{"acp-go.dev/lifecycle":{"version":1.0000000000000001}}`, lifecycle.MetaPath + ".version", false},
		{"overflow", `"_meta":{"acp-go.dev/lifecycle":{"version":1e400}}`, lifecycle.MetaPath + ".version", false},
		{"SDK case alias", `"_META":{"acp-go.dev/lifecycle":{"version":2,"version":1}}`, lifecycle.MetaPath + ".version", false},
		{"mixed case erased envelope", `"_META":{"acp-go.dev/lifecycle":{"version":1}},"_meta":null`, lifecycle.MetaPath, false},
		{"decimal", `"_meta":{"acp-go.dev/lifecycle":{"version":1.0}}`, lifecycle.MetaPath + ".version", false},
		{"exponent", `"_meta":{"acp-go.dev/lifecycle":{"version":1e0}}`, lifecycle.MetaPath + ".version", false},
		{"unknown member", `"_meta":{"acp-go.dev/lifecycle":{"version":1,"extra":true}}`, lifecycle.MetaPath + ".extra", false},
		{"nonobject", `"_meta":{"acp-go.dev/lifecycle":null}`, lifecycle.MetaPath, false},
		{"duplicate namespace", `"_meta":{"acp-go.dev/lifecycle":{"version":2},"acp-go.dev/lifecycle":{"version":1}}`, lifecycle.MetaPath, false},
		{"duplicate envelopes", `"_meta":{"acp-go.dev/lifecycle":{"version":2}},"_meta":{"acp-go.dev/lifecycle":{"version":1}}`, lifecycle.MetaPath, false},
		{"erased envelope", `"_meta":{"acp-go.dev/lifecycle":{"version":1}},"_meta":null`, lifecycle.MetaPath, false},
		{"offer after foreign envelope", `"_meta":{"foreign":1},"_meta":{"acp-go.dev/lifecycle":{"version":1}}`, lifecycle.MetaPath, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			connection := lifecycleWireConnection(t)
			response, err := acp.SendRequest[acp.InitializeResponse](connection, t.Context(), acp.AgentMethodInitialize,
				json.RawMessage(`{"protocolVersion":1,`+test.members+`}`))
			if test.field != "" {
				requireUnsupportedField(t, err, test.field)

				return
			}
			require.NoError(t, err)
			_, offered := response.Meta[lifecycleMetaKey]
			require.Equal(t, test.offered, offered)
		})
	}
}

func TestServeLifecyclePromptRetainsWireValues(t *testing.T) {
	const route = `"acp-go.dev/route":{"version":1,"turnNonce":"turn"}`
	const submission = `"submission":{"submissionId":"submission","clientNonce":"nonce"}`
	for _, test := range []struct {
		name    string
		members string
		field   string
	}{
		{"valid reaches prompt validation", `"_meta":{` + route + `,"acp-go.dev/lifecycle":{"version":1,` + submission + `}}`, fieldPrompt},
		{"rounded fraction", `"_meta":{` + route + `,"acp-go.dev/lifecycle":{"version":1.0000000000000001,` + submission + `}}`, lifecycle.MetaPath + ".version"},
		{"duplicate version", `"_meta":{` + route + `,"acp-go.dev/lifecycle":{"version":2,"version":1,` + submission + `}}`, lifecycle.MetaPath + ".version"},
		{"duplicate submission", `"_meta":{` + route + `,"acp-go.dev/lifecycle":{"version":1,"submission":null,` + submission + `}}`, lifecycle.MetaPath + ".submission"},
		{"duplicate identifier", `"_meta":{` + route + `,"acp-go.dev/lifecycle":{"version":1,"submission":{"submissionId":"first","submissionId":"second","clientNonce":"nonce"}}}`, lifecycle.MetaPath + ".submission.submissionId"},
		{"duplicate namespace", `"_meta":{` + route + `,"acp-go.dev/lifecycle":null,"acp-go.dev/lifecycle":{"version":1,` + submission + `}}`, lifecycle.MetaPath},
		{"erased envelope", `"_meta":{"acp-go.dev/lifecycle":{"version":1,` + submission + `}},"_meta":null,"_meta":{` + route + `}`, lifecycle.MetaPath},
		{"route wins", `"_meta":{"acp-go.dev/route":{"version":2,"turnNonce":"turn"},"acp-go.dev/lifecycle":{"version":2,"version":1}}`, routeMetaPath + ".version"},
		{"route wins over overflow", `"_meta":{"acp-go.dev/route":{"version":2,"turnNonce":"turn"},"acp-go.dev/lifecycle":{"version":1e400}}`, routeMetaPath + ".version"},
		{"foreign duplicate", `"_meta":{` + route + `,"foreign":{"x":1,"x":2},"acp-go.dev/lifecycle":{"version":1,` + submission + `}}`, fieldPrompt},
	} {
		t.Run(test.name, func(t *testing.T) {
			connection := lifecycleWireConnection(t)
			_, err := acp.SendRequest[acp.InitializeResponse](connection, t.Context(), acp.AgentMethodInitialize, lifecycleInitializeRequest())
			require.NoError(t, err)
			_, err = acp.SendRequest[acp.PromptResponse](connection, t.Context(), acp.AgentMethodSessionPrompt,
				json.RawMessage(`{"sessionId":"session","prompt":[],`+test.members+`}`))
			requireUnsupportedField(t, err, test.field)
		})
	}
}

func TestServeLifecycleWirePreservesConstructionPrecedence(t *testing.T) {
	connection := lifecycleWireConnection(t, WithDefaultModel("malformed"))
	_, err := acp.SendRequest[acp.InitializeResponse](connection, t.Context(), acp.AgentMethodInitialize,
		json.RawMessage(`{"protocolVersion":1,"_meta":{"acp-go.dev/lifecycle":{"version":1e400}}}`))
	requireClosedInternalError(t, err, invalidOptionsError)
}

func TestServeLifecycleCannotBeErasedOnForbiddenSurfaces(t *testing.T) {
	for _, test := range []struct {
		method string
		fields string
	}{
		{acp.AgentMethodAuthenticate, `"methodId":"provider",`},
		{acp.AgentMethodLogout, ""},
		{acp.AgentMethodSessionClose, `"sessionId":"session",`},
		{acp.AgentMethodSessionDelete, `"sessionId":"session",`},
		{acp.AgentMethodSessionList, ""},
		{acp.AgentMethodSessionNew, `"cwd":"/workspace","mcpServers":[],`},
		{acp.AgentMethodSessionLoad, `"sessionId":"session","cwd":"/workspace","mcpServers":[],`},
		{acp.AgentMethodSessionResume, `"sessionId":"session","cwd":"/workspace","mcpServers":[],`},
		{acp.AgentMethodSessionSetConfigOption, `"sessionId":"session","configId":"model","value":"provider/model",`},
		{ForkSessionMethod, `"sessionId":"session","cwd":"/workspace","mcpServers":[],`},
		{"_unknown", ""},
	} {
		t.Run(test.method, func(t *testing.T) {
			connection := lifecycleWireConnection(t)
			_, err := acp.SendRequest[acp.InitializeResponse](connection, t.Context(), acp.AgentMethodInitialize, defaultInitializeRequest())
			require.NoError(t, err)
			_, err = acp.SendRequest[json.RawMessage](connection, t.Context(), test.method,
				json.RawMessage(`{`+test.fields+`"_meta":{"acp-go.dev/lifecycle":{"version":1}},"_meta":null}`))
			requireUnsupportedField(t, err, lifecycle.MetaPath)
		})
	}
}

func TestServeRouteNumbersRemainExact(t *testing.T) {
	for _, version := range []string{"1", "1.0", "1e0", "0.1e1", "1.0000000000000001", "1e400", "-1e-400"} {
		t.Run(version, func(t *testing.T) {
			connection := lifecycleWireConnection(t)
			_, err := acp.SendRequest[acp.InitializeResponse](connection, t.Context(), acp.AgentMethodInitialize, defaultInitializeRequest())
			require.NoError(t, err)
			_, err = acp.SendRequest[acp.PromptResponse](connection, t.Context(), acp.AgentMethodSessionPrompt,
				json.RawMessage(`{"sessionId":"session","prompt":[],"_meta":{"acp-go.dev/route":{"version":`+version+`,"turnNonce":"turn"}}}`))
			field := routeMetaPath + ".version"
			if version == "1" || version == "1.0" || version == "1e0" || version == "0.1e1" {
				field = fieldPrompt
			}
			requireUnsupportedField(t, err, field)
		})
	}
}

func TestServeHandoffNumbersRemainExact(t *testing.T) {
	for _, test := range []struct {
		name    string
		version string
		size    string
		data    string
		verdict string
	}{
		{"fractional version", "1.0000000000000001", "1", "", imageErrorInvalidHandoff},
		{"overflow version", "1e400", "1", "", imageErrorInvalidHandoff},
		{"fractional size", "1", "1.0000000000000001", "", imageErrorInvalidHandoff},
		{"negative underflow", "1", "-1e-400", "", imageErrorInvalidHandoff},
		{"overflow size", "1", "1e400", "", imageErrorInvalidHandoff},
		{"decimal integer spellings", "1.0", "1.0", "", imageErrorMissingFile},
		{"exponent integer spellings", "1e0", "1e0", "", imageErrorMissingFile},
		{"duplicate versions keep SDK behavior", `2,"version":1`, "1", "", imageErrorMissingFile},
		{"duplicate sizes keep SDK behavior", "1", `2,"sizeBytes":1`, "", imageErrorMissingFile},
		{"largest integer reaches size gate", "1", "9223372036854775807", "", imageErrorTooLarge},
		{"integer beyond size representation", "1", "9223372036854775808", "", imageErrorInvalidHandoff},
		{"embedded data dominates", "1e400", "-1e-400", "!invalid!", imageErrorInvalidBase64},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			connection := lifecycleWireConnection(t, WithInputHandoffRoot(root))
			_, err := acp.SendRequest[acp.InitializeResponse](connection, t.Context(), acp.AgentMethodInitialize, defaultInitializeRequest())
			require.NoError(t, err)
			uri, err := json.Marshal(fileURIFor(filepath.Join(root, "missing.png")))
			require.NoError(t, err)
			_, err = acp.SendRequest[acp.PromptResponse](connection, t.Context(), acp.AgentMethodSessionPrompt,
				json.RawMessage(`{"sessionId":"session","_meta":{"acp-go.dev/route":{"version":1,"turnNonce":"turn"}},`+
					`"prompt":[{"type":"image","mimeType":"image/png","data":"`+test.data+`","uri":`+string(uri)+
					`,"_meta":{"acp-go.dev/handoff":{"version":`+test.version+`,"sizeBytes":`+test.size+`,"digest":"`+strings.Repeat("0", 64)+`"}}}]}`))
			requireImageParamError(t, err, test.verdict, 0)
		})
	}
}

func TestServeForeignNumericOverflowKeepsSDKRefusal(t *testing.T) {
	connection := lifecycleWireConnection(t)
	_, err := acp.SendRequest[acp.InitializeResponse](connection, t.Context(), acp.AgentMethodInitialize,
		json.RawMessage(`{"protocolVersion":1,"_meta":{"foreign":{"version":1e400}}}`))
	requireUnsupportedField(t, err, jsonFieldParams)
}
