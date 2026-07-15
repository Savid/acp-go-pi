package piacp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"

	"github.com/savid/acp-go-pi/internal/pi"
)

type dispatcherParams struct {
	Value string `json:"value"`
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

func (c *dispatcherClient) Done() <-chan struct{} {
	return c.done
}

func (*dispatcherClient) CreateElicitation(
	context.Context,
	acp.UnstableCreateElicitationRequest,
	elicitationScope,
) (acp.UnstableCreateElicitationResponse, error) {
	return acp.UnstableCreateElicitationResponse{}, nil
}

func (*dispatcherClient) RequestPermission(
	context.Context,
	acp.RequestPermissionRequest,
) (acp.RequestPermissionResponse, error) {
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

func TestPostResponseHooksMatchSuccessfulResponses(t *testing.T) {
	hooks := &postResponseHooks{log: slog.New(slog.DiscardHandler)}
	hooks.runAfterResponseWrite([]byte(`{`))
	(&postResponseHooks{}).runAfterResponseWrite([]byte(`{`))

	firstRan := make(chan struct{})
	secondRan := make(chan struct{})
	hooks.enqueue("1", func() { close(firstRan) })
	hooks.enqueue("2", func() { close(secondRan) })

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
}

func TestLifecycleCommandHookErrors(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)

	missingAgent := NewAgent(WithLogger(logger))
	missing := &localAgentConnection{agent: missingAgent, hooks: &postResponseHooks{}}
	missing.enqueueLifecycleCommandHook(t.Context(), acp.AgentMethodSessionLoad, json.RawMessage(
		`{"sessionId":"missing","_acp_go_pi_post_response_hook_id":"response"}`,
	), nil)
	require.Len(t, missing.hooks.all, 1)
	missing.hooks.all[0].run()

	updateErr := errors.New("update failed")
	client := &dispatcherClient{done: make(chan struct{}), updateErr: updateErr}
	agent := NewAgent(WithLogger(logger))
	agent.conn = client
	agent.sessions["session"] = &agentSession{
		agent: agent,
		id:    "session",
		availableCommands: []pi.SlashCommand{{
			Name:        "help",
			Description: "Show help",
		}},
	}
	conn := &localAgentConnection{agent: agent, hooks: &postResponseHooks{}}
	canceledCtx, cancel := context.WithCancel(t.Context())
	cancel()
	conn.enqueueLifecycleCommandHook(canceledCtx, acp.AgentMethodSessionLoad, json.RawMessage(
		`{"sessionId":"session","_acp_go_pi_post_response_hook_id":"response"}`,
	), nil)
	require.Len(t, conn.hooks.all, 1)
	conn.hooks.all[0].run()
	require.NoError(t, client.updateCtxErr)
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
