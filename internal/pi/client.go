package pi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"unicode"
)

// ErrTransportClosed reports that the pi stdout stream ended before a command
// received its response.
var ErrTransportClosed = errors.New("pi transport closed")

// commandTypeKey is the JSON key selecting the pi RPC command type.
const commandTypeKey = "type"

// Client speaks pi's JSONL RPC protocol over a command writer (pi stdin) and
// an event stream (pi stdout). Commands are correlated by id; agent events
// and extension UI requests are delivered on channels in stream order, so
// events caused by a command are always delivered before that command's
// response resolves (pi's response-after-events barrier is preserved
// in-process).
type Client struct {
	stdin io.Writer
	lines *LineReader

	writeMu sync.Mutex

	pendingMu sync.Mutex
	pending   map[string]chan Response
	failure   error
	failed    bool

	nextID         atomic.Uint64
	decodeFailures atomic.Uint64
	strayResponses atomic.Uint64
	started        atomic.Bool

	events     chan Event
	uiRequests chan UIRequest
	done       chan struct{}
	wg         sync.WaitGroup
}

// NewClient constructs a client over pi's stdin writer and stdout reader.
func NewClient(stdin io.Writer, stdout io.Reader) *Client {
	return &Client{
		stdin:      stdin,
		lines:      NewLineReader(stdout),
		pending:    make(map[string]chan Response, 4),
		events:     make(chan Event),
		uiRequests: make(chan UIRequest),
		done:       make(chan struct{}),
	}
}

// Start launches the stdout read loop. It must be called exactly once before
// any command is issued; the loop stops when the stream ends or ctx is
// cancelled.
func (c *Client) Start(ctx context.Context) error {
	if !c.started.CompareAndSwap(false, true) {
		return fmt.Errorf("pi client already started")
	}

	c.wg.Add(1)

	loop := func() {
		defer c.wg.Done()

		c.readLoop(ctx)
	}

	go loop()

	return nil
}

// Stop waits for the read loop to exit and returns the transport failure, if
// any. The caller must first end the stream (close pi stdout, e.g. by
// terminating the process); Stop does not unblock a pending read by itself.
func (c *Client) Stop() error {
	if c.started.Load() {
		c.wg.Wait()
	}

	return c.Err()
}

// Events returns the agent event stream. It is closed when the read loop
// exits. The consumer must keep draining it while commands are in flight:
// event delivery is synchronous to preserve the response-after-events
// barrier.
func (c *Client) Events() <-chan Event {
	return c.events
}

// UIRequests returns the extension UI request stream. It is closed when the
// read loop exits. Dialog requests must be answered via RespondUI.
func (c *Client) UIRequests() <-chan UIRequest {
	return c.uiRequests
}

// Done is closed when the read loop has exited.
func (c *Client) Done() <-chan struct{} {
	return c.done
}

// Err returns the transport failure, or nil after a clean end-of-stream.
func (c *Client) Err() error {
	c.pendingMu.Lock()
	defer c.pendingMu.Unlock()

	return c.failure
}

// DecodeFailures counts malformed stdout records that were skipped.
func (c *Client) DecodeFailures() uint64 {
	return c.decodeFailures.Load()
}

// StrayResponses counts responses that matched no pending command id.
func (c *Client) StrayResponses() uint64 {
	return c.strayResponses.Load()
}

// Call sends one command with a fresh correlation id and waits for its
// response. fields must not contain "id"; the command "type" key is required.
// A success:false response is returned with a nil error; use Response.Err.
func (c *Client) Call(ctx context.Context, fields map[string]any) (Response, error) {
	commandType, _ := fields[commandTypeKey].(string)
	if commandType == "" {
		return Response{}, fmt.Errorf("pi command requires a type field")
	}

	id := fmt.Sprintf("acp-%d", c.nextID.Add(1))

	payload := make(map[string]any, len(fields)+1)
	for key, value := range fields {
		payload[key] = value
	}

	payload["id"] = id

	response := make(chan Response, 1)
	if err := c.registerPending(id, response); err != nil {
		return Response{}, err
	}

	if err := c.writeLine(payload); err != nil {
		c.unregisterPending(id)

		return Response{}, fmt.Errorf("write %s command: %w", commandType, err)
	}

	select {
	case resp, ok := <-response:
		if !ok {
			return Response{}, c.closedError()
		}

		return resp, nil
	case <-ctx.Done():
		c.unregisterPending(id)

		return Response{}, ctx.Err()
	}
}

// RespondUI answers one extension UI dialog request on pi stdin.
func (c *Client) RespondUI(response UIResponse) error {
	if response.Type == "" {
		response.Type = uiResponseType
	}

	if err := c.writeLine(response); err != nil {
		return fmt.Errorf("write extension ui response: %w", err)
	}

	return nil
}

func (c *Client) writeLine(payload any) error {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("encode jsonl record: %w", err)
	}

	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	if _, err := c.stdin.Write(append(encoded, '\n')); err != nil {
		return err
	}

	return nil
}

func (c *Client) registerPending(id string, response chan Response) error {
	c.pendingMu.Lock()
	defer c.pendingMu.Unlock()

	if c.failed {
		return c.closedErrorLocked()
	}

	c.pending[id] = response

	return nil
}

func (c *Client) unregisterPending(id string) {
	c.pendingMu.Lock()
	defer c.pendingMu.Unlock()

	delete(c.pending, id)
}

func (c *Client) closedError() error {
	c.pendingMu.Lock()
	defer c.pendingMu.Unlock()

	return c.closedErrorLocked()
}

func (c *Client) closedErrorLocked() error {
	if c.failure != nil {
		return fmt.Errorf("%w: %w", ErrTransportClosed, c.failure)
	}

	return ErrTransportClosed
}

func (c *Client) readLoop(ctx context.Context) {
	var failure error

	for {
		line, err := c.lines.Next()
		if len(line) > 0 && !isBlank(line) {
			if !c.dispatch(ctx, line) {
				failure = context.Cause(ctx)

				break
			}
		}

		if err != nil {
			if !errors.Is(err, io.EOF) {
				failure = err
			}

			break
		}
	}

	c.finish(failure)
}

// dispatch routes one record; it returns false when ctx ended mid-delivery.
func (c *Client) dispatch(ctx context.Context, line []byte) bool {
	message, err := DecodeMessage(line)
	if err != nil {
		c.decodeFailures.Add(1)

		return true
	}

	switch message.Kind {
	case MessageKindResponse:
		c.resolve(message.Response)

		return true
	case MessageKindUIRequest:
		select {
		case c.uiRequests <- message.UIRequest:
			return true
		case <-ctx.Done():
			return false
		}
	default:
		select {
		case c.events <- message.Event:
			return true
		case <-ctx.Done():
			return false
		}
	}
}

func (c *Client) resolve(response Response) {
	c.pendingMu.Lock()

	waiter, ok := c.pending[response.ID]
	if ok {
		delete(c.pending, response.ID)
	}

	c.pendingMu.Unlock()

	if !ok {
		c.strayResponses.Add(1)

		return
	}

	waiter <- response
}

func (c *Client) finish(failure error) {
	c.pendingMu.Lock()
	c.failed = true
	c.failure = failure

	for id, waiter := range c.pending {
		close(waiter)
		delete(c.pending, id)
	}
	c.pendingMu.Unlock()

	close(c.events)
	close(c.uiRequests)
	close(c.done)
}

func isBlank(line []byte) bool {
	for _, b := range line {
		if !unicode.IsSpace(rune(b)) {
			return false
		}
	}

	return true
}
