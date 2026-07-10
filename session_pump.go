package piacp

import (
	"context"

	"github.com/savid/acp-go-pi/internal/pi"
)

// turnSink carries one live turn's events from the pump to the prompt loop.
// events is closed by the pump when the native transport ends; done is closed
// by the prompt loop when it stops reading, so the pump never blocks on a
// finished turn.
type turnSink struct {
	events chan pi.Event
	done   chan struct{}
}

func newTurnSink() *turnSink {
	return &turnSink{
		events: make(chan pi.Event),
		done:   make(chan struct{}),
	}
}

// startPump launches the per-process goroutine that drains the client's
// event and UI request streams. Event delivery from the client is
// synchronous, so the pump must keep draining for the life of the process or
// command responses would never resolve.
func (s *agentSession) startPump(client piClient) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	s.mu.Lock()
	s.pumpCancel = cancel
	s.pumpDone = done
	s.mu.Unlock()

	go s.pump(ctx, client, done)
}

func (s *agentSession) pump(ctx context.Context, client piClient, done chan struct{}) {
	defer close(done)

	events := client.Events()
	uiRequests := client.UIRequests()

	for events != nil || uiRequests != nil {
		select {
		case event, ok := <-events:
			if !ok {
				events = nil

				continue
			}

			s.dispatchEvent(ctx, event)
		case request, ok := <-uiRequests:
			if !ok {
				uiRequests = nil

				continue
			}

			s.dispatchUIRequest(ctx, request)
		case <-ctx.Done():
			s.closeActiveTurnSink()

			return
		}
	}

	s.closeActiveTurnSink()
}

// dispatchEvent forwards one native event to the live turn. Events outside a
// live turn are dropped: pi is silent between accepted prompts and raw events
// are live-turn only.
func (s *agentSession) dispatchEvent(ctx context.Context, event pi.Event) {
	sink := s.activeTurnSink()
	if sink == nil {
		return
	}

	s.emitRawPiEvent(ctx, event.RawJSON())

	select {
	case sink.events <- event:
	case <-sink.done:
	case <-ctx.Done():
	}
}

func (s *agentSession) dispatchUIRequest(ctx context.Context, request pi.UIRequest) {
	if s.activeTurnSink() != nil {
		s.emitRawPiEvent(ctx, request.RawJSON())
	}

	if !request.IsDialog() {
		return
	}

	s.dialogWG.Add(1)

	go func() {
		defer s.dialogWG.Done()

		s.handleUIDialog(ctx, request)
	}()
}

func (s *agentSession) activeTurnSink() *turnSink {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.turnSink
}

// closeActiveTurnSink signals the live turn that the native transport ended.
func (s *agentSession) closeActiveTurnSink() {
	s.mu.Lock()
	sink := s.turnSink
	s.turnSink = nil
	s.mu.Unlock()

	if sink != nil {
		close(sink.events)
	}
}

// stopPump stops the pump goroutine and waits for it and any in-flight
// dialog handlers to finish.
func (s *agentSession) stopPump() {
	s.mu.Lock()
	cancel := s.pumpCancel
	done := s.pumpDone
	s.pumpCancel = nil
	s.pumpDone = nil
	s.mu.Unlock()

	if cancel != nil {
		cancel()
	}

	if done != nil {
		<-done
	}

	s.dialogWG.Wait()
}
