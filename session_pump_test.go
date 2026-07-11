package piacp

import (
	"context"
	"log/slog"
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
	session.turnSink = newTurnSink()
	session.dispatchUIRequest(t.Context(), pi.UIRequest{Method: "notify"})

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
