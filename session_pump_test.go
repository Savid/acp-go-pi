package piacp

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/savid/acp-go-pi/internal/pi"
)

func TestOutboxRoutingBranches(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	connection := newDirectAgentClient()
	agent.setConnection(connection)
	session := &agentSession{agent: agent, id: "id", rawMessages: rawMessageConfig{All: true}}
	event := pi.AgentStartEvent{}
	outbox := newSessionOutbox(1)
	session.routeNativeEvent(t.Context(), outbox, event)

	delivery := newTurnDelivery()
	outbox.adopt(delivery)
	session.turnEvents = delivery
	close(delivery.done)
	session.routeNativeEvent(t.Context(), outbox, event)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	delivery = newTurnDelivery()
	outbox.adopt(delivery)
	session.turnEvents = delivery
	session.routeNativeEvent(ctx, outbox, event)

	outbox.release(delivery)
	session.turnEvents = nil
	session.routeUIRequest(t.Context(), outbox, pi.UIRequest{Method: "notify"})
	session.routeUIRequest(t.Context(), outbox, pi.UIRequest{ID: "stale", Method: uiMethodSelect})

	delivery = newTurnDelivery()
	outbox.adopt(delivery)
	session.turnEvents = delivery
	dispatched := make(chan struct{})
	go func() {
		session.routeUIRequest(t.Context(), outbox, pi.UIRequest{Method: "notify"})
		close(dispatched)
	}()
	<-delivery.uiRequests
	<-dispatched

	client := newStubPiClient()
	close(client.events)
	close(client.uiRequests)
	done := make(chan struct{})
	session.pump(t.Context(), client, outbox, done)
	select {
	case <-done:
	default:
		t.Fatal("pump did not finish")
	}
}

// TestOutboxRoutesAnEventWithNoForegroundCycle pins that an event arriving
// between accepted prompts is routed rather than discarded: it still reaches the
// raw-event stream, which is what makes the generation's stream gap-detectable.
func TestOutboxRoutesAnEventWithNoForegroundCycle(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	connection := newDirectAgentClient()
	agent.setConnection(connection)

	session := &agentSession{agent: agent, id: "id", rawMessages: rawMessageConfig{All: true}}

	message, err := pi.DecodeMessage([]byte(`{"type":"event","event":{"type":"queue_update","steering":[],"followUp":[]}}`))
	require.NoError(t, err)
	require.NotNil(t, message.Event)

	session.routeNativeEvent(t.Context(), newSessionOutbox(7), message.Event)

	require.Len(t, connection.notified, 1)
}

// TestOutboxRecordsSettlementOnlyForItsOwnGeneration pins the generation
// binding: a settlement observed on a delivery the session no longer holds
// fences nothing.
func TestOutboxRecordsSettlementOnlyForItsOwnGeneration(t *testing.T) {
	session := &agentSession{agent: NewAgent(WithLogger(slog.New(slog.DiscardHandler))), id: "id"}

	stale := newTurnDelivery()
	session.recordNativeSettlement(stale)
	require.False(t, session.turnNativeSettled)

	session.turnEvents = stale
	session.recordNativeSettlement(stale)
	require.True(t, session.turnNativeSettled)

	session.recordNativeSettlement(nil)
}

// TestAdoptingAnEndedGenerationEndsTheDelivery pins that a prompt which raced
// the end of its own native process observes the ended transport instead of
// waiting on a router that will never speak again.
func TestAdoptingAnEndedGenerationEndsTheDelivery(t *testing.T) {
	outbox := newSessionOutbox(3)
	outbox.end()

	delivery := newTurnDelivery()
	outbox.adopt(delivery)

	_, open := <-delivery.events
	require.False(t, open)
	require.Nil(t, outbox.foreground())
}

func TestPumpPanicIsContainedAndEndsTurn(t *testing.T) {
	var logs bytes.Buffer
	agent := NewAgent(WithLogger(slog.New(slog.NewTextHandler(&logs, nil))))
	delivery := newTurnDelivery()
	outbox := newSessionOutbox(1)
	outbox.adopt(delivery)
	session := &agentSession{agent: agent, id: "id", turnEvents: delivery}
	client := newStubPiClient()
	client.eventsFunc = func() <-chan pi.Event {
		panic("event stream boom")
	}
	done := make(chan struct{})

	session.pump(t.Context(), client, outbox, done)

	select {
	case <-done:
	default:
		t.Fatal("panicking pump did not finish")
	}
	if _, ok := <-delivery.events; ok {
		t.Fatal("panicking pump left the turn event stream open")
	}
	if !strings.Contains(logs.String(), "session event pump") || !strings.Contains(logs.String(), "event stream boom") {
		t.Fatalf("panic log = %q", logs.String())
	}
}
