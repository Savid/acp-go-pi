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
	"time"
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
	pending   map[string]*pendingCall
	failure   error
	failed    bool

	nextID         atomic.Uint64
	decodeFailures atomic.Uint64
	strayResponses atomic.Uint64
	started        atomic.Bool

	events     chan Event
	uiRequests chan UIRequest
	boundaries chan ResponseBoundary
	done       chan struct{}
	wg         sync.WaitGroup
}

// CallBoundary supplies command boundaries that must be linearized with the
// JSONL transport itself. BeforeDispatch runs immediately before the command
// write and holds its returned release function through that write. Accepted
// is carried to the event consumer when a successful response is decoded; the
// stdout reader waits for that consumer to resolve it before reading further.
type CallBoundary struct {
	BeforeDispatch func() (release func(), err error)
	Accepted       func(context.Context) error
}

type pendingCall struct {
	response  chan callResult
	accepted  func(context.Context) error
	writeDone chan struct{}
	writeErr  error
}

type callResult struct {
	response Response
	boundary error
}

// ResponseBoundary is a successful command-response watermark that the sole
// event consumer must resolve. The client does not read a later stdout record
// until Resolve returns, so every event delivered before this boundary has been
// fully processed and every later event observes its accepted transition.
type ResponseBoundary struct {
	owner *responseBoundaryOwner
}

type responseBoundaryOwner struct {
	accepted func(context.Context) error
	result   chan error
	once     sync.Once
}

// Resolve executes the response hook exactly once and releases the JSONL
// reader. The pump's generation context owns the hook: cancellation therefore
// interrupts every production hook before teardown joins the pump.
func (b ResponseBoundary) Resolve(ctx context.Context) {
	if b.owner == nil {
		return
	}

	b.owner.once.Do(func() {
		b.resolveOwned(ctx)
	})
}

func (b ResponseBoundary) resolveOwned(ctx context.Context) {
	var err error

	defer func() {
		if recover() != nil {
			err = errors.New("pi response boundary hook panicked")
		}

		b.owner.result <- err
	}()

	if b.owner.accepted != nil {
		err = b.owner.accepted(ctx)
	}
}

// NewClient constructs a client over pi's stdin writer and stdout reader.
func NewClient(stdin io.Writer, stdout io.Reader) *Client {
	return &Client{
		stdin:      stdin,
		lines:      NewLineReader(stdout),
		pending:    make(map[string]*pendingCall, 4),
		events:     make(chan Event),
		uiRequests: make(chan UIRequest),
		boundaries: make(chan ResponseBoundary, 1),
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

// ResponseBoundaries returns successful command-response watermarks. The event
// consumer must resolve each boundary in stream order.
func (c *Client) ResponseBoundaries() <-chan ResponseBoundary {
	return c.boundaries
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

// DecodeFailures reports whether the terminal structural record was malformed.
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
	return c.call(ctx, fields, CallBoundary{})
}

// CallWithBoundary is Call with transport-linearized dispatch and successful
// response hooks.
func (c *Client) CallWithBoundary(
	ctx context.Context,
	fields map[string]any,
	boundary CallBoundary,
) (Response, error) {
	return c.call(ctx, fields, boundary)
}

func (c *Client) call(ctx context.Context, fields map[string]any, boundary CallBoundary) (Response, error) {
	commandType, _ := fields[commandTypeKey].(string)
	if commandType == "" {
		return Response{}, fmt.Errorf("pi command requires a type field")
	}

	id := fmt.Sprintf("acp-%d", c.nextID.Add(1))

	payload := make(map[string]any, len(fields)+1)
	maps.Copy(payload, fields)

	payload["id"] = id

	encoded, err := json.Marshal(payload)
	if err != nil {
		return Response{}, fmt.Errorf("encode %s command: %w", commandType, err)
	}

	response := make(chan callResult, 1)
	waiter := &pendingCall{
		response:  response,
		accepted:  boundary.Accepted,
		writeDone: make(chan struct{}),
	}

	if err := c.registerPending(id, waiter); err != nil {
		return Response{}, err
	}

	var release func()

	if boundary.BeforeDispatch != nil {
		var err error

		release, err = boundary.BeforeDispatch()
		if err != nil {
			waiter.finishWrite(err)
			c.unregisterPending(id)

			return Response{}, err
		}
	}

	writeErr := c.writeEncodedLine(encoded)
	waiter.finishWrite(writeErr)

	if writeErr != nil {
		if release != nil {
			release()
		}

		c.unregisterPending(id)

		return Response{}, fmt.Errorf("write %s command: %w", commandType, writeErr)
	}

	if release != nil {
		release()
	}

	select {
	case result, ok := <-response:
		if !ok {
			return Response{}, c.closedError()
		}

		if result.boundary != nil {
			return Response{}, result.boundary
		}

		return result.response, nil
	case <-ctx.Done():
		if c.unregisterPending(id) {
			return Response{}, ctx.Err()
		}

		// The stdout reader already claimed a response. A successful native
		// response owns its acceptance boundary from that instant onward, so
		// caller cancellation cannot turn it back into an unaccepted command.
		return c.awaitClaimedResponse(ctx, response)
	}
}

func (c *Client) awaitClaimedResponse(ctx context.Context, response <-chan callResult) (Response, error) {
	settleCtx, cancelSettle := context.WithTimeout(context.WithoutCancel(ctx), responseBoundarySettlementTimeout)
	defer cancelSettle()

	var result callResult
	select {
	case result = <-response:
	case <-settleCtx.Done():
		return Response{}, fmt.Errorf("%w: join claimed response boundary: %w", ErrTransportClosed, settleCtx.Err())
	}

	if result.boundary != nil {
		return Response{}, result.boundary
	}

	return result.response, nil
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
	// UIResponse contains only JSON primitives, so encoding cannot fail. Keeping
	// an error branch here would pretend an unreachable transport state exists.
	encoded, _ := json.Marshal(payload)

	return c.writeEncodedLine(encoded)
}

func (c *Client) writeEncodedLine(encoded []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	record := make([]byte, len(encoded)+1)
	copy(record, encoded)
	record[len(encoded)] = '\n'

	n, err := c.stdin.Write(record)
	if n != len(record) {
		err = errors.Join(err, io.ErrShortWrite)
	}

	return err
}

func (p *pendingCall) finishWrite(err error) {
	p.writeErr = err
	close(p.writeDone)
}

func (p *pendingCall) awaitWrite(ctx context.Context) error {
	select {
	case <-p.writeDone:
		return p.writeErr
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

func (c *Client) registerPending(id string, call *pendingCall) error {
	c.pendingMu.Lock()
	defer c.pendingMu.Unlock()

	if c.failed {
		return c.closedErrorLocked()
	}

	c.pending[id] = call

	return nil
}

func (c *Client) unregisterPending(id string) bool {
	c.pendingMu.Lock()
	defer c.pendingMu.Unlock()

	if _, ok := c.pending[id]; !ok {
		return false
	}

	delete(c.pending, id)

	return true
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

// dispatch routes one record and terminalizes the stream on its first
// structural decode failure.
func (c *Client) dispatch(ctx context.Context, line []byte) error {
	message, err := DecodeMessage(line)
	if err != nil {
		c.decodeFailures.Add(1)

		return ErrJSONLStructural
	}

	if message.Kind == MessageKindResponse {
		waiter, ok := c.claimPending(message.Response.ID)

		if boundaryErr := c.resolveClaimed(ctx, message.Response, waiter, ok); boundaryErr != nil {
			return boundaryErr
		}

		return nil
	}

	switch message.Kind {
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

func (c *Client) claimPending(id string) (*pendingCall, bool) {
	c.pendingMu.Lock()
	defer c.pendingMu.Unlock()

	waiter, ok := c.pending[id]
	if ok {
		delete(c.pending, id)
	}

	return waiter, ok
}

func (c *Client) resolveClaimed(ctx context.Context, response Response, waiter *pendingCall, ok bool) error {
	if !ok {
		c.strayResponses.Add(1)

		return nil
	}

	if writeErr := waiter.awaitWrite(ctx); writeErr != nil {
		waiter.response <- callResult{boundary: writeErr}

		return fmt.Errorf("response preceded a complete command write: %w", writeErr)
	}

	var (
		boundaryErr      error
		fatalBoundaryErr error
	)

	if response.Success && waiter.accepted != nil {
		settleCtx, cancelSettle := context.WithTimeout(context.Background(), responseBoundarySettlementTimeout)
		boundary := ResponseBoundary{owner: &responseBoundaryOwner{
			accepted: waiter.accepted,
			result:   make(chan error, 1),
		}}

		select {
		case c.boundaries <- boundary:
		case <-settleCtx.Done():
			boundaryErr = fmt.Errorf("%w: hand off successful response boundary: %w",
				ErrTransportClosed, settleCtx.Err())
			fatalBoundaryErr = boundaryErr
		}

		if boundaryErr == nil {
			select {
			case boundaryErr = <-boundary.owner.result:
			case <-settleCtx.Done():
				boundaryErr = fmt.Errorf("%w: settle successful response boundary: %w",
					ErrTransportClosed, settleCtx.Err())
				fatalBoundaryErr = boundaryErr
			}
		}

		cancelSettle()
	}

	waiter.response <- callResult{response: response, boundary: boundaryErr}

	return fatalBoundaryErr
}

// responseBoundarySettlementTimeout bounds only the acceptance handoff.
// Once a successful response is decoded, caller cancellation cannot bypass
// this boundary; a missing consumer instead fails the call deterministically.
var responseBoundarySettlementTimeout = 10 * time.Second

func (c *Client) finish(failure error) {
	c.pendingMu.Lock()
	c.failed = true
	c.failure = failure

	for id, waiter := range c.pending {
		close(waiter.response)
		delete(c.pending, id)
	}
	c.pendingMu.Unlock()

	close(c.events)
	close(c.uiRequests)
	close(c.boundaries)
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
