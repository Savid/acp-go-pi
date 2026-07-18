package piacp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/coder/acp-go-sdk"
)

const postResponseHookIDParam = "_acp_go_pi_post_response_hook_id"

type localAgentConnection struct {
	agent       *Agent
	conn        *acp.Connection
	initialized atomic.Bool
	hooks       *postResponseHooks
}

type localAgentHandler func(context.Context, *Agent, json.RawMessage) (any, *acp.RequestError)

type localAgentParams[Req any] interface {
	*Req
	Validate() error
}

var (
	_ agentClient = (*localAgentConnection)(nil)

	localAgentHandlers = map[string]localAgentHandler{
		acp.AgentMethodAuthenticate:           localResponse((*Agent).Authenticate),
		acp.AgentMethodInitialize:             localResponse((*Agent).Initialize),
		acp.AgentMethodLogout:                 localResponse((*Agent).Logout),
		acp.AgentMethodSessionCancel:          localNotification((*Agent).Cancel),
		acp.AgentMethodSessionClose:           localResponse((*Agent).CloseSession),
		acp.AgentMethodSessionDelete:          localResponse((*Agent).UnstableDeleteSession),
		acp.AgentMethodSessionList:            localResponse((*Agent).ListSessions),
		acp.AgentMethodSessionLoad:            localResponse((*Agent).LoadSession),
		acp.AgentMethodSessionNew:             localResponse((*Agent).NewSession),
		acp.AgentMethodSessionPrompt:          localResponse((*Agent).Prompt),
		acp.AgentMethodSessionResume:          localResponse((*Agent).ResumeSession),
		acp.AgentMethodSessionSetConfigOption: localResponse((*Agent).SetSessionConfigOption),
	}
)

func newLocalAgentConnection(agent *Agent, output io.Writer, input io.Reader) *localAgentConnection {
	hooks := &postResponseHooks{log: agent.log}
	conn := &localAgentConnection{agent: agent, hooks: hooks}
	inputGate := newConnectionInputGate(newPostResponseHookRequestReader(input))
	conn.conn = acp.NewConnection(conn.handle, hooks.wrap(output), inputGate)
	conn.conn.SetLogger(agent.log)
	inputGate.open()

	return conn
}

type connectionInputGate struct {
	reader io.Reader
	ready  chan struct{}
	once   sync.Once
}

// connectionInputGate blocks the SDK receive goroutine until the connection
// logger is installed. The SDK starts receiving inside NewConnection.
func newConnectionInputGate(reader io.Reader) *connectionInputGate {
	return &connectionInputGate{
		reader: reader,
		ready:  make(chan struct{}),
	}
}

func (g *connectionInputGate) open() {
	g.once.Do(func() {
		close(g.ready)
	})
}

func (g *connectionInputGate) Read(p []byte) (int, error) {
	<-g.ready

	return g.reader.Read(p)
}

func (c *localAgentConnection) Done() <-chan struct{} {
	return c.conn.Done()
}

func (c *localAgentConnection) handle(ctx context.Context, method string, params json.RawMessage) (any, *acp.RequestError) {
	if err := c.agent.ensureOpen(); err != nil {
		return nil, acp.NewInvalidRequest(map[string]any{jsonFieldError: err.Error()})
	}

	if method != acp.AgentMethodInitialize && !c.initialized.Load() {
		return nil, acp.NewInvalidRequest(map[string]any{
			jsonFieldMethod: method,
			jsonFieldError:  "initialize must be called before other ACP methods",
		})
	}

	if strings.HasPrefix(method, "_") {
		result, err := c.agent.HandleExtensionMethod(ctx, method, params)

		reqErr := requestError(err)
		if reqErr == nil {
			c.enqueueLifecycleCommandHook(ctx, method, params, result)
		}

		return result, reqErr
	}

	handler, ok := localAgentHandlers[method]
	if !ok {
		return nil, acp.NewMethodNotFound(method)
	}

	result, reqErr := handler(ctx, c.agent, params)
	if method == acp.AgentMethodInitialize && reqErr == nil {
		c.initialized.Store(true)
	}

	if reqErr == nil {
		c.enqueueLifecycleCommandHook(ctx, method, params, result)
	}

	return result, reqErr
}

// enqueueLifecycleCommandHook schedules the first available_commands_update
// for a session lifecycle method to run only after the lifecycle response has
// been written to the ACP transport.
func (c *localAgentConnection) enqueueLifecycleCommandHook(ctx context.Context, method string, params json.RawMessage, result any) {
	sessionID, ok := lifecycleCommandSessionID(method, params, result)
	if !ok || c.hooks == nil {
		return
	}

	responseID := postResponseHookRequestID(params)
	if responseID == "" {
		return
	}

	c.hooks.enqueue(responseID, func() {
		hookCtx := context.WithoutCancel(ctx)

		session, err := c.agent.session(sessionID)
		if err != nil {
			c.agent.log.ErrorContext(hookCtx, "post-response command update session lookup failed",
				slog.String(jsonFieldMethod, method),
				slog.String(acpFieldSessionID, string(sessionID)),
				slog.String(jsonFieldError, err.Error()),
			)

			return
		}

		if err := session.emitAvailableCommandsUpdate(hookCtx, true); err != nil {
			c.agent.log.ErrorContext(hookCtx, "post-response command update failed",
				slog.String(jsonFieldMethod, method),
				slog.String(acpFieldSessionID, string(sessionID)),
				slog.String(jsonFieldError, err.Error()),
			)
		}
	})
}

func lifecycleCommandSessionID(method string, params json.RawMessage, result any) (acp.SessionId, bool) {
	switch method {
	case acp.AgentMethodSessionNew:
		resp, ok := result.(acp.NewSessionResponse)

		return resp.SessionId, ok && resp.SessionId != ""
	case acp.AgentMethodSessionLoad:
		var req acp.LoadSessionRequest
		if err := json.Unmarshal(params, &req); err != nil {
			return "", false
		}

		return req.SessionId, req.SessionId != ""
	case acp.AgentMethodSessionResume:
		var req acp.ResumeSessionRequest
		if err := json.Unmarshal(params, &req); err != nil {
			return "", false
		}

		return req.SessionId, req.SessionId != ""
	case ForkSessionMethod:
		resp, ok := result.(acp.UnstableForkSessionResponse)

		return resp.SessionId, ok && resp.SessionId != ""
	default:
		return "", false
	}
}

type postResponseHookRequestReader struct {
	reader     *bufio.Reader
	pending    []byte
	pendingErr error
}

func newPostResponseHookRequestReader(reader io.Reader) *postResponseHookRequestReader {
	return &postResponseHookRequestReader{reader: bufio.NewReader(reader)}
}

func (r *postResponseHookRequestReader) Read(p []byte) (int, error) {
	if len(r.pending) == 0 {
		if r.pendingErr != nil {
			err := r.pendingErr
			r.pendingErr = nil

			return 0, err
		}

		line, err := r.reader.ReadBytes('\n')
		if len(line) == 0 {
			return 0, err
		}

		r.pending = tagPostResponseHookRequest(line)
		if err != nil {
			r.pendingErr = err
		}
	}

	n := copy(p, r.pending)
	r.pending = r.pending[n:]

	return n, nil
}

func tagPostResponseHookRequest(line []byte) []byte {
	var msg struct {
		JSONRPC string           `json:"jsonrpc,omitempty"`
		ID      *json.RawMessage `json:"id,omitempty"`
		Method  string           `json:"method,omitempty"`
		Params  json.RawMessage  `json:"params,omitempty"`
	}
	if err := json.Unmarshal(line, &msg); err != nil {
		return line
	}

	if msg.ID == nil || !lifecycleCommandMethod(msg.Method) || len(bytes.TrimSpace(msg.Params)) == 0 {
		return line
	}

	params := make(map[string]json.RawMessage)
	if err := json.Unmarshal(msg.Params, &params); err != nil {
		return line
	}

	if params == nil {
		return line
	}

	hookID, _ := json.Marshal(responseHookID(msg.ID))
	params[postResponseHookIDParam] = hookID
	msg.Params, _ = json.Marshal(params)

	tagged, _ := json.Marshal(msg)
	if bytes.HasSuffix(line, []byte("\n")) {
		tagged = append(tagged, '\n')
	}

	return tagged
}

func lifecycleCommandMethod(method string) bool {
	switch method {
	case acp.AgentMethodSessionNew, acp.AgentMethodSessionLoad, acp.AgentMethodSessionResume, ForkSessionMethod:
		return true
	default:
		return false
	}
}

func postResponseHookRequestID(params json.RawMessage) string {
	var tagged map[string]string
	if err := json.Unmarshal(params, &tagged); err != nil {
		return ""
	}

	return tagged[postResponseHookIDParam]
}

type postResponseHooks struct {
	log *slog.Logger
	mu  sync.Mutex
	all []postResponseHook
}

type postResponseHook struct {
	responseID string
	run        func()
}

func (h *postResponseHooks) wrap(writer io.Writer) io.Writer {
	return &postResponseWriter{writer: writer, hooks: h}
}

func (h *postResponseHooks) enqueue(responseID string, run func()) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.all = append(h.all, postResponseHook{
		responseID: responseID,
		run:        run,
	})
}

func (h *postResponseHooks) runAfterResponseWrite(data []byte) {
	var msg struct {
		ID    *json.RawMessage `json:"id"`
		Error *json.RawMessage `json:"error"`
	}
	if err := json.Unmarshal(bytes.TrimSpace(data), &msg); err != nil {
		if h.log != nil {
			h.log.Debug("parse response for post-response hook failed", slog.String(jsonFieldError, err.Error()))
		}

		return
	}

	if msg.ID == nil || msg.Error != nil {
		return
	}

	responseID := responseHookID(msg.ID)

	h.mu.Lock()
	for index, hook := range h.all {
		if hook.responseID != responseID {
			continue
		}

		h.all = append(h.all[:index], h.all[index+1:]...)
		h.mu.Unlock()

		go func() {
			defer recoverAgentGoroutine(context.Background(), h.log, "post-response hook")

			hook.run()
		}()

		return
	}
	h.mu.Unlock()
}

func responseHookID(id *json.RawMessage) string {
	return string(bytes.TrimSpace(*id))
}

type postResponseWriter struct {
	writer io.Writer
	hooks  *postResponseHooks
}

func (w *postResponseWriter) Write(data []byte) (int, error) {
	n, err := w.writer.Write(data)
	if err == nil && n == len(data) {
		w.hooks.runAfterResponseWrite(data)
	}

	return n, err
}

func localResponse[Req any, ReqPtr localAgentParams[Req], Resp any](
	call func(*Agent, context.Context, Req) (Resp, error),
) localAgentHandler {
	return func(ctx context.Context, agent *Agent, params json.RawMessage) (any, *acp.RequestError) {
		value, reqErr := decodeLocalAgentParams[Req, ReqPtr](params)
		if reqErr != nil {
			return nil, reqErr
		}

		resp, err := call(agent, ctx, value)
		if err != nil {
			return nil, requestError(err)
		}

		return resp, nil
	}
}

func localNotification[Req any, ReqPtr localAgentParams[Req]](
	call func(*Agent, context.Context, Req) error,
) localAgentHandler {
	return func(ctx context.Context, agent *Agent, params json.RawMessage) (any, *acp.RequestError) {
		value, reqErr := decodeLocalAgentParams[Req, ReqPtr](params)
		if reqErr != nil {
			return nil, reqErr
		}

		if err := call(agent, ctx, value); err != nil {
			return nil, requestError(err)
		}

		return nil, nil
	}
}

func decodeLocalAgentParams[Req any, ReqPtr localAgentParams[Req]](params json.RawMessage) (Req, *acp.RequestError) {
	var value Req
	if err := json.Unmarshal(params, &value); err != nil {
		return value, acp.NewInvalidParams(map[string]any{jsonFieldError: err.Error()})
	}

	if err := ReqPtr(&value).Validate(); err != nil {
		return value, acp.NewInvalidParams(map[string]any{jsonFieldError: err.Error()})
	}

	return value, nil
}

func (c *localAgentConnection) CreateElicitation(
	ctx context.Context,
	params acp.UnstableCreateElicitationRequest,
	scope elicitationScope,
) (acp.UnstableCreateElicitationResponse, error) {
	raw, err := scopedElicitationParams(params, scope)
	if err != nil {
		return acp.UnstableCreateElicitationResponse{}, err
	}

	release, err := c.agent.acquireClientCall(ctx)
	if err != nil {
		return acp.UnstableCreateElicitationResponse{}, err
	}
	defer release()

	return acp.SendRequest[acp.UnstableCreateElicitationResponse](c.conn, ctx, acp.ClientMethodElicitationCreate, raw)
}

func (c *localAgentConnection) RequestPermission(
	ctx context.Context,
	params acp.RequestPermissionRequest,
) (acp.RequestPermissionResponse, error) {
	release, err := c.agent.acquireClientCall(ctx)
	if err != nil {
		return acp.RequestPermissionResponse{}, err
	}
	defer release()

	return acp.SendRequest[acp.RequestPermissionResponse](c.conn, ctx, acp.ClientMethodSessionRequestPermission, params)
}

func (c *localAgentConnection) SessionUpdate(ctx context.Context, params acp.SessionNotification) error {
	release, err := c.agent.acquireClientCall(ctx)
	if err != nil {
		return err
	}
	defer release()

	return c.conn.SendNotification(ctx, acp.ClientMethodSessionUpdate, params)
}

func (c *localAgentConnection) NotifyExtension(ctx context.Context, method string, params any) error {
	if method == "" || !strings.HasPrefix(method, "_") {
		return fmt.Errorf("extension method name must start with '_' (got %q)", method)
	}

	release, err := c.agent.acquireClientCall(ctx)
	if err != nil {
		return err
	}
	defer release()

	return c.conn.SendNotification(ctx, method, params)
}
