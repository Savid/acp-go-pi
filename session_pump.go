package piacp

import (
	"context"
	"log/slog"
	"strings"
	"sync"

	"github.com/savid/acp-go-pi/internal/pi"
)

// turnDelivery carries one accepted prompt's foreground work from the session
// outbox to the prompt loop. events is closed by the outbox when the native
// generation ends; done is closed by the prompt loop when it stops reading, so
// the pump never blocks on a finished turn.
type turnDelivery struct {
	events     chan pi.Event
	uiRequests chan pi.UIRequest
	done       chan struct{}
}

func newTurnDelivery() *turnDelivery {
	return &turnDelivery{
		events: make(chan pi.Event),
		// pi dialogs are modal and therefore serialized. One slot keeps the
		// transport draining if a dialog arrives before the prompt RPC ack.
		uiRequests: make(chan pi.UIRequest, 1),
		done:       make(chan struct{}),
	}
}

// sessionOutbox routes every native event of exactly one pi process
// generation. It is session-owned rather than prompt-owned: pi survives a
// prompt, so the router that speaks for its generation must survive one too,
// and an event that arrives with no foreground cycle open is routed rather
// than discarded.
type sessionOutbox struct {
	generation uint64

	mu    sync.Mutex
	turn  *turnDelivery
	ended bool
}

func newSessionOutbox(generation uint64) *sessionOutbox {
	return &sessionOutbox{generation: generation}
}

// adopt installs the accepted prompt's foreground delivery. A generation that
// has already ended adopts nothing and ends the delivery immediately, so a
// prompt that raced the end of its own native process observes the ended
// transport instead of waiting on a router that will never speak again.
func (o *sessionOutbox) adopt(delivery *turnDelivery) {
	ended := o == nil

	if !ended {
		o.mu.Lock()
		ended = o.ended

		if !ended {
			o.turn = delivery
		}
		o.mu.Unlock()
	}

	if ended {
		close(delivery.events)
	}
}

func (o *sessionOutbox) release(delivery *turnDelivery) {
	if o == nil {
		return
	}

	o.mu.Lock()
	defer o.mu.Unlock()

	if o.turn == delivery {
		o.turn = nil
	}
}

func (o *sessionOutbox) foreground() *turnDelivery {
	o.mu.Lock()
	defer o.mu.Unlock()

	return o.turn
}

// end fences the generation. The foreground delivery learns the native
// transport is gone from the closed channel, which is the one signal that says
// this generation produces no further event.
func (o *sessionOutbox) end() {
	o.mu.Lock()
	delivery := o.turn
	o.turn = nil
	alreadyEnded := o.ended
	o.ended = true
	o.mu.Unlock()

	if delivery != nil && !alreadyEnded {
		close(delivery.events)
	}
}

// startPump launches the per-process goroutine that drains the client's
// event and UI request streams. Event delivery from the client is
// synchronous, so the pump must keep draining for the life of the process or
// command responses would never resolve.
func (s *agentSession) startPump(client piClient) uint64 {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	s.mu.Lock()
	s.pumpGeneration++
	generation := s.pumpGeneration
	outbox := newSessionOutbox(generation)
	s.outbox = outbox
	s.pumpCancel = cancel
	s.pumpDone = done
	s.mu.Unlock()

	go s.pump(ctx, client, outbox, done)

	return generation
}

func (s *agentSession) pump(ctx context.Context, client piClient, outbox *sessionOutbox, done chan struct{}) {
	defer close(done)
	defer func() {
		handleAgentGoroutinePanic(ctx, agentLogger(s.agent), "session event pump", func(any) {
			outbox.end()
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

			s.routeNativeEvent(ctx, outbox, event)
		case request, ok := <-uiRequests:
			if !ok {
				uiRequests = nil

				continue
			}

			s.routeUIRequest(ctx, outbox, request)
		case <-ctx.Done():
			outbox.end()

			return
		}
	}

	outbox.end()
}

// routeNativeEvent routes one native event under its generation's identity.
// Settlement is recorded at pump receipt, the raw-event stream sees every
// event whether or not a prompt is in flight, and only the ACP projection
// needs an open foreground cycle.
func (s *agentSession) routeNativeEvent(ctx context.Context, outbox *sessionOutbox, event pi.Event) {
	delivery := outbox.foreground()

	// The native response barrier ends when the client hands this event to the
	// pump, not when the prompt goroutine receives it from the delivery. Record
	// agent_settled before the cancellable send so stopPump cannot erase a
	// durability fence native pi has already crossed.
	if _, settled := event.(pi.AgentSettledEvent); settled {
		s.recordNativeSettlement(delivery)
	}

	s.emitRawPiEvent(s.generationRouteContext(ctx, delivery), event.RawJSON())

	if delivery == nil {
		// pi is silent between accepted prompts, so an event arriving with no
		// foreground cycle open has no ACP update to become. It is still
		// carried on the raw-event stream above and still named here, so
		// nothing about this generation is inferred from silence.
		s.agent.log.DebugContext(ctx, "pi event outside a foreground cycle",
			slog.String(acpFieldSessionID, string(s.id)),
			slog.String("event", event.Kind()),
			slog.Uint64("generation", outbox.generation),
		)

		return
	}

	select {
	case delivery.events <- event:
	case <-delivery.done:
	case <-ctx.Done():
	}
}

// recordNativeSettlement fences the mirror for the turn that owns the current
// delivery. The generation binding is what stops a settlement observed on one
// native process from being adopted by a turn running on another.
func (s *agentSession) recordNativeSettlement(delivery *turnDelivery) {
	if delivery == nil {
		return
	}

	s.mu.Lock()
	if s.turnEvents == delivery {
		s.turnNativeSettled = true
	}
	s.mu.Unlock()
}

// generationRouteContext stamps the live turn's route on raw events emitted
// while a foreground cycle is open. An event outside one belongs to the
// generation rather than to a turn, so it carries no route.
func (s *agentSession) generationRouteContext(ctx context.Context, delivery *turnDelivery) context.Context {
	if delivery == nil {
		return ctx
	}

	s.mu.Lock()
	nonce := s.turnNonce
	current := s.turnEvents == delivery
	s.mu.Unlock()

	if !current || nonce == "" {
		return ctx
	}

	return withTurnRoute(ctx, nonce)
}

func (s *agentSession) routeUIRequest(ctx context.Context, outbox *sessionOutbox, request pi.UIRequest) {
	if broker := s.agent.providerAuth; broker != nil && strings.HasPrefix(request.Title, pi.AuthTitleMarker) {
		s.dialogWG.Add(1)

		go func() {
			defer recoverAgentGoroutine(ctx, agentLogger(s.agent), "provider auth dialog")
			defer s.dialogWG.Done()

			broker.handleAuthDialog(ctx, s, request)
		}()

		return
	}

	// Provider-auth dialogs are excluded above rather than redacted here: a
	// login's presentation and its answers are credential material, and key-name
	// redaction cannot sanitize a secret embedded in prose or a URL.
	s.emitRawPiEvent(s.generationRouteContext(ctx, outbox.foreground()), request.RawJSON())

	delivery := outbox.foreground()
	if delivery == nil {
		// A dialog with no foreground cycle to block is unanswerable by this
		// adapter, so it is cancelled rather than left holding pi's extension.
		// Cancelling is the load-bearing half; dropping it silently is not.
		if request.IsDialog() {
			s.respondUIDialog(ctx, pi.UICancelResponse(request.ID))
		}

		return
	}

	select {
	case delivery.uiRequests <- request:
	case <-delivery.done:
	case <-ctx.Done():
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

// activeTurnDelivery reports the foreground delivery the session's accepted
// prompt is reading, or nil between prompts.
func (s *agentSession) activeTurnDelivery() *turnDelivery {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.turnEvents
}
