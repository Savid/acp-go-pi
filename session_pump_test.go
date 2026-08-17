package piacp

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/savid/acp-go-pi/internal/pi"
)

func TestPumpDeliveryBranches(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	connection := newDirectAgentClient()
	agent.setConnection(connection)
	session := &agentSession{agent: agent, id: "id", rawMessages: rawMessageConfig{All: true}}
	event := pi.AgentStartEvent{}
	session.dispatchEvent(t.Context(), event)

	sink := newTurnSink()
	session.turnSink = sink
	close(sink.done)
	session.dispatchEvent(t.Context(), event)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	sink = newTurnSink()
	session.turnSink = sink
	session.dispatchEvent(ctx, event)

	session.turnSink = nil
	session.dispatchUIRequest(t.Context(), pi.UIRequest{Method: "notify"})
	session.dispatchUIRequest(t.Context(), pi.UIRequest{ID: "stale", Method: uiMethodSelect})
	sink = newTurnSink()
	session.turnSink = sink
	dispatched := make(chan struct{})
	go func() {
		session.dispatchUIRequest(t.Context(), pi.UIRequest{Method: "notify"})
		close(dispatched)
	}()
	<-sink.uiRequests
	<-dispatched

	client := newStubPiClient()
	close(client.events)
	close(client.uiRequests)
	done := make(chan struct{})
	session.pump(t.Context(), client, done)
	select {
	case <-done:
	default:
		t.Fatal("pump did not finish")
	}
}

func TestPumpPanicIsContainedAndEndsTurn(t *testing.T) {
	var logs bytes.Buffer
	agent := NewAgent(WithLogger(slog.New(slog.NewTextHandler(&logs, nil))))
	sink := newTurnSink()
	session := &agentSession{agent: agent, id: "id", turnSink: sink}
	client := newStubPiClient()
	client.eventsFunc = func() <-chan pi.Event {
		panic("event stream boom")
	}
	done := make(chan struct{})

	session.pump(t.Context(), client, done)

	select {
	case <-done:
	default:
		t.Fatal("panicking pump did not finish")
	}
	if _, ok := <-sink.events; ok {
		t.Fatal("panicking pump left the turn event stream open")
	}
	if !strings.Contains(logs.String(), "session event pump") || !strings.Contains(logs.String(), "event stream boom") {
		t.Fatalf("panic log = %q", logs.String())
	}
}
