package piacp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"strings"
	"sync"

	"github.com/coder/acp-go-sdk"

	"github.com/savid/acp-go-core/wire"
)

const rawKeyInitialize = "initialize"

// transport wraps the JSON-RPC streams Serve hands to the SDK. It reads every
// inbound frame to keep the raw params the lifecycle strictness needs and the
// request ids the establishing responses answer, and it watches every outbound
// frame so a session's opening publication runs only after its establishing
// response is on the wire and an action announcement only after its request.
type transport struct {
	input  io.Reader
	output io.Writer

	mu       sync.Mutex
	requests map[string]inboundRequest
	raw      map[string][]json.RawMessage
	hooks    map[acp.SessionId]func(context.Context)
	written  map[string]chan struct{}
	partial  []byte
}

type inboundRequest struct {
	method    string
	sessionID acp.SessionId
}

func newTransport(input io.Reader, output io.Writer) *transport {
	return &transport{
		input:    input,
		output:   output,
		requests: make(map[string]inboundRequest),
		raw:      make(map[string][]json.RawMessage),
		hooks:    make(map[acp.SessionId]func(context.Context)),
		written:  make(map[string]chan struct{}),
	}
}

func (t *transport) reader() io.Reader {
	return &transportReader{transport: t, lines: bufio.NewReader(t.input)}
}

func (t *transport) writer() io.Writer { return &transportWriter{transport: t} }

type transportReader struct {
	transport *transport
	lines     *bufio.Reader
	pending   []byte
}

func (r *transportReader) Read(p []byte) (int, error) {
	if len(r.pending) == 0 {
		line, err := r.lines.ReadBytes('\n')
		if len(line) > 0 {
			r.transport.observeInbound(line)
		}

		r.pending = line

		if len(line) == 0 {
			return 0, err
		}
	}

	n := copy(p, r.pending)
	r.pending = r.pending[n:]

	return n, nil
}

type transportWriter struct {
	transport *transport
}

func (w *transportWriter) Write(p []byte) (int, error) {
	n, err := w.transport.output.Write(p)
	if err == nil && n != len(p) {
		err = io.ErrShortWrite
	}

	if n > 0 {
		w.transport.observeOutbound(p[:n])
	}

	return n, err
}

type frame struct {
	ID     json.RawMessage `json:"id"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
	Result json.RawMessage `json:"result"`
	Error  json.RawMessage `json:"error"`
}

type sessionParams struct {
	SessionID acp.SessionId `json:"sessionId"`
}

// observeInbound records the request id and, for the surfaces whose owned
// metadata must be read strictly, the raw params.
func (t *transport) observeInbound(line []byte) {
	var f frame
	if err := json.Unmarshal(line, &f); err != nil || f.Method == "" || len(f.ID) == 0 {
		return
	}

	var params sessionParams

	_ = json.Unmarshal(f.Params, &params)

	t.mu.Lock()
	defer t.mu.Unlock()

	t.requests[string(f.ID)] = inboundRequest{method: f.Method, sessionID: params.SessionID}

	switch f.Method {
	case acp.AgentMethodInitialize:
		t.raw[rawKeyInitialize] = append(t.raw[rawKeyInitialize], f.Params)
	case acp.AgentMethodSessionPrompt:
		t.raw[rawKeyPrompt(params.SessionID)] = append(t.raw[rawKeyPrompt(params.SessionID)], f.Params)
	}
}

func rawKeyPrompt(sessionID acp.SessionId) string {
	return "prompt/" + string(sessionID)
}

// takeRaw returns the oldest raw params recorded under key.
func (t *transport) takeRaw(key string) json.RawMessage {
	t.mu.Lock()
	defer t.mu.Unlock()

	queue := t.raw[key]
	if len(queue) == 0 {
		return nil
	}

	t.raw[key] = queue[1:]

	return queue[0]
}

// takeRawPrompt returns the recorded raw params of the prompt whose decoded
// lifecycle value equals meta's, so concurrent prompts on one session cannot
// swap correlations.
func (t *transport) takeRawPrompt(sessionID acp.SessionId, meta map[string]any) json.RawMessage {
	t.mu.Lock()
	defer t.mu.Unlock()

	key := rawKeyPrompt(sessionID)
	queue := t.raw[key]

	for index, raw := range queue {
		var envelope struct {
			Meta map[string]any `json:"_meta"` //nolint:tagliatelle // ACP reserves this wire spelling.
		}

		_ = json.Unmarshal(raw, &envelope)

		if sameJSON(envelope.Meta[wire.LifecycleKey], meta[wire.LifecycleKey]) {
			t.raw[key] = append(queue[:index:index], queue[index+1:]...)

			return raw
		}
	}

	return nil
}

func sameJSON(left, right any) bool {
	leftBytes, leftErr := json.Marshal(left)
	rightBytes, rightErr := json.Marshal(right)

	return leftErr == nil && rightErr == nil && bytes.Equal(leftBytes, rightBytes)
}

// registerHook schedules one publication to run after the establishing
// response for sessionID is written.
func (t *transport) registerHook(sessionID acp.SessionId, hook func(context.Context)) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.hooks[sessionID] = hook
}

// awaitRequestWrite returns a channel closed once the outbound request that
// carries actionID has been written.
func (t *transport) awaitRequestWrite(actionID string) <-chan struct{} {
	t.mu.Lock()
	defer t.mu.Unlock()

	ch, ok := t.written[actionID]
	if !ok {
		ch = make(chan struct{})
		t.written[actionID] = ch
	}

	return ch
}

// observeOutbound watches complete frames. A response to a session-establishing
// request releases that session's hook; a request carrying an action
// correlation releases its announcement.
func (t *transport) observeOutbound(data []byte) {
	t.mu.Lock()
	t.partial = append(t.partial, data...)

	var lines [][]byte

	for {
		index := bytes.IndexByte(t.partial, '\n')
		if index < 0 {
			break
		}

		lines = append(lines, bytes.Clone(t.partial[:index]))
		t.partial = t.partial[index+1:]
	}
	t.mu.Unlock()

	for _, line := range lines {
		t.observeOutboundFrame(line)
	}
}

func (t *transport) observeOutboundFrame(line []byte) {
	var f frame
	if err := json.Unmarshal(line, &f); err != nil {
		return
	}

	if f.Method != "" {
		t.releaseRequestWrite(f.Params)

		return
	}

	if len(f.ID) == 0 {
		return
	}

	t.mu.Lock()
	request, ok := t.requests[string(f.ID)]
	delete(t.requests, string(f.ID))

	var hook func(context.Context)

	if ok && len(f.Error) == 0 {
		sessionID := request.sessionID

		switch request.method {
		case acp.AgentMethodSessionNew:
			var result sessionParams

			_ = json.Unmarshal(f.Result, &result)
			sessionID = result.SessionID
		case acp.AgentMethodSessionLoad, acp.AgentMethodSessionResume:
		default:
			sessionID = ""
		}

		if sessionID != "" {
			hook = t.hooks[sessionID]
			delete(t.hooks, sessionID)
		}
	}
	t.mu.Unlock()

	if hook != nil {
		go hook(context.Background())
	}
}

func (t *transport) releaseRequestWrite(params json.RawMessage) {
	var envelope struct {
		Meta map[string]any `json:"_meta"` //nolint:tagliatelle // ACP reserves this wire spelling.
	}

	if err := json.Unmarshal(params, &envelope); err != nil {
		return
	}

	correlation, _ := envelope.Meta[wire.LifecycleKey].(map[string]any)
	action, _ := correlation["action"].(map[string]any)
	actionID, _ := action["actionId"].(string)

	if strings.TrimSpace(actionID) == "" {
		return
	}

	t.mu.Lock()
	ch, ok := t.written[actionID]
	delete(t.written, actionID)
	t.mu.Unlock()

	if ok {
		close(ch)
	}
}
