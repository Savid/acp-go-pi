package pi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
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
// an event stream (pi stdout). Commands are correlated by id; agent events and
// extension UI requests are delivered on channels in stream order. Delivery is
// synchronous, so the consumer must keep draining both channels while commands
// are in flight.
type Client struct {
	stdin io.Writer
	lines *LineReader

	writeMu sync.Mutex

	pendingMu sync.Mutex
	pending   map[string]chan Response
	failure   error
	failed    bool

	nextID  atomic.Uint64
	started atomic.Bool

	events     chan Event
	uiRequests chan UIRequest
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
	}
}

// Start launches the stdout read loop. It must be called exactly once before
// any command is issued; the loop stops when the stream ends or ctx is
// cancelled.
func (c *Client) Start(ctx context.Context) error {
	if !c.started.CompareAndSwap(false, true) {
		return errors.New("pi client already started")
	}

	c.wg.Go(func() { c.readLoop(ctx) })

	return nil
}

// Events returns the agent event stream. It is closed when the read loop
// exits.
func (c *Client) Events() <-chan Event {
	return c.events
}

// UIRequests returns the extension UI request stream. It is closed when the
// read loop exits. Dialog requests must be answered via RespondUI.
func (c *Client) UIRequests() <-chan UIRequest {
	return c.uiRequests
}

// Err returns the transport failure, or nil after a clean end-of-stream.
func (c *Client) Err() error {
	c.pendingMu.Lock()
	defer c.pendingMu.Unlock()

	return c.failure
}

// Call sends one command with a fresh correlation id and waits for its
// response. fields must not contain "id"; the command "type" key is required.
// A success:false response is returned with a nil error; use Response.Err.
func (c *Client) Call(ctx context.Context, fields map[string]any) (Response, error) {
	commandType, _ := fields[commandTypeKey].(string)
	if commandType == "" {
		return Response{}, errors.New("pi command requires a type field")
	}

	id := fmt.Sprintf("acp-%d", c.nextID.Add(1))

	payload := make(map[string]any, len(fields)+1)
	maps.Copy(payload, fields)

	payload["id"] = id

	encoded, err := json.Marshal(payload)
	if err != nil {
		return Response{}, fmt.Errorf("encode %s command: %w", commandType, err)
	}

	response := make(chan Response, 1)

	if err := c.registerPending(id, response); err != nil {
		return Response{}, err
	}

	if err := c.writeEncodedLine(encoded); err != nil {
		c.unregisterPending(id)

		return Response{}, fmt.Errorf("write %s command: %w", commandType, err)
	}

	select {
	case result, ok := <-response:
		if !ok {
			return Response{}, c.closedError()
		}

		return result, nil
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

	// UIResponse holds only JSON primitives, so encoding cannot fail.
	encoded, _ := json.Marshal(response)

	if err := c.writeEncodedLine(encoded); err != nil {
		return fmt.Errorf("write extension ui response: %w", err)
	}

	return nil
}

func (c *Client) writeEncodedLine(encoded []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	record := make([]byte, len(encoded)+1)
	copy(record, encoded)
	record[len(encoded)] = '\n'

	n, err := c.stdin.Write(record)
	if err == nil && n != len(record) {
		err = io.ErrShortWrite
	}

	return err
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
			if dispatchErr := c.dispatch(ctx, line); dispatchErr != nil {
				failure = dispatchErr

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

// dispatch routes one record and ends the stream on its first structural
// decode failure.
func (c *Client) dispatch(ctx context.Context, line []byte) error {
	message, err := DecodeMessage(line)
	if err != nil {
		return ErrJSONLStructural
	}

	switch message.Kind {
	case MessageKindResponse:
		c.pendingMu.Lock()
		waiter, ok := c.pending[message.Response.ID]
		delete(c.pending, message.Response.ID)
		c.pendingMu.Unlock()

		if !ok {
			return nil
		}

		waiter <- message.Response

		return nil
	case MessageKindUIRequest:
		select {
		case c.uiRequests <- message.UIRequest:
			return nil
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	default:
		select {
		case c.events <- message.Event:
			return nil
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	}
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
}

func isBlank(line []byte) bool {
	for _, b := range line {
		if !unicode.IsSpace(rune(b)) {
			return false
		}
	}

	return true
}
