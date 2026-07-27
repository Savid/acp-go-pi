package piacp

import (
	"context"
	"strings"

	"github.com/savid/acp-go-pi/internal/pi"
)

// turnSink carries one live turn's events from the pump to the prompt loop.
// events is closed by the pump when the native transport ends; done is closed
// by the prompt loop when it stops reading, so the pump never blocks on a
// finished turn.
type turnSink struct {
	events     chan pi.Event
	uiRequests chan pi.UIRequest
	done       chan struct{}
}

func newTurnSink() *turnSink {
	return &turnSink{
		events: make(chan pi.Event),
		// pi dialogs are modal and therefore serialized. One slot keeps the
		// transport draining if a dialog arrives before the prompt RPC ack.
		uiRequests: make(chan pi.UIRequest, 1),
		done:       make(chan struct{}),
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
	defer func() {
		handleAgentGoroutinePanic(ctx, agentLogger(s.agent), "session event pump", func(any) {
			s.closeActiveTurnSink()
		}, recover())
	}()

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

	// The native response barrier ends when the client hands this event to the
	// pump, not when the prompt goroutine receives it from the turn sink. Record
	// agent_settled before the cancellable sink send so stopPump cannot erase a
	// durability fence that native pi has already crossed.
	if _, settled := event.(pi.AgentSettledEvent); settled {
		s.mu.Lock()
		if s.turnSink == sink {
			s.turnNativeSettled = true
		}
		s.mu.Unlock()
	}

	select {
	case sink.events <- event:
	case <-sink.done:
	case <-ctx.Done():
	}
}

func (s *agentSession) dispatchUIRequest(ctx context.Context, request pi.UIRequest) {
	// Provider-auth dialogs are routed before the turn-sink check: a login runs
	// outside any turn, so a live sink is neither required nor the right owner.
	if broker := s.agent.providerAuth; broker != nil && strings.HasPrefix(request.Title, pi.AuthTitleMarker) {
		s.dialogWG.Add(1)

		go func() {
			defer recoverAgentGoroutine(ctx, agentLogger(s.agent), "provider auth dialog")
			defer s.dialogWG.Done()

			broker.handleAuthDialog(ctx, s, request)
		}()

		return
	}

	sink := s.activeTurnSink()
	if sink == nil {
		if request.IsDialog() {
			s.respondUIDialog(ctx, pi.UICancelResponse(request.ID))
		}

		return
	}

	select {
	case sink.uiRequests <- request:
	case <-sink.done:
	case <-ctx.Done():
	}
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
