package piacp

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"
)

func TestRouteEnvelopeHardCutover(t *testing.T) {
	ctx := withTurnRoute(context.Background(), "turn-old")
	require.Equal(t, turnRouteMeta("turn-old"), turnRouteMetaFromContext(ctx))
	require.Nil(t, turnRouteMetaFromContext(context.Background()))

	boundaryNonce := strings.Repeat("n", routeTurnNonceMaxBytes)
	route, err := parseInboundTurnRoute(turnRouteMeta(boundaryNonce))
	require.NoError(t, err)
	require.Equal(t, boundaryNonce, route.turnNonce)
	require.NotNil(t, requestTurnRouteMeta(boundaryNonce))
	require.Nil(t, requestTurnRouteMeta(""))
	require.Nil(t, requestTurnRouteMeta(strings.Repeat("n", routeTurnNonceMaxBytes+1)))

	for _, meta := range []map[string]any{
		nil,
		{routeMetaKey: "bad"},
		{routeMetaKey: map[string]any{routeFieldVer: 2, routeFieldTurn: "turn"}},
		{routeMetaKey: map[string]any{routeFieldVer: float64(1), routeFieldTurn: "turn", "extra": true}},
		{routeMetaKey: map[string]any{routeFieldVer: 1, routeFieldTurn: ""}},
		{routeMetaKey: map[string]any{routeFieldVer: 1.5, routeFieldTurn: "turn"}},
		{routeMetaKey: map[string]any{routeFieldVer: "1", routeFieldTurn: "turn"}},
		{routeMetaKey: map[string]any{routeFieldVer: 1, routeFieldTurn: strings.Repeat("n", routeTurnNonceMaxBytes+1)}},
	} {
		_, routeErr := parseInboundTurnRoute(meta)
		require.Error(t, routeErr)
	}

	decoded, err := parseInboundTurnRoute(map[string]any{routeMetaKey: map[string]any{
		routeFieldVer: float64(1), routeFieldTurn: "decoded-turn",
	}})
	require.NoError(t, err)
	require.Equal(t, "decoded-turn", decoded.turnNonce)

	meta, err := stampRouteMeta(map[string]any{"pi": map[string]any{"native": true}}, elicitationScope{
		SessionID: "session-1", TurnNonce: "turn-1", ToolCallID: "tool-1",
	})
	require.NoError(t, err)
	require.Contains(t, meta, "pi")
	require.Equal(t, map[string]any{
		routeFieldVer: 1, routeFieldID: acp.SessionId("session-1"), routeFieldTurn: "turn-1", "toolCallId": acp.ToolCallId("tool-1"),
	}, meta[routeMetaKey])

	_, err = stampRouteMeta(map[string]any{routeMetaKey: map[string]any{}}, elicitationScope{SessionID: "s", TurnNonce: "t"})
	require.ErrorContains(t, err, "collision")
	_, err = stampRouteMeta(nil, elicitationScope{SessionID: "s", TurnNonce: "t", ToolCallID: "tool", RequestID: "req"})
	require.ErrorContains(t, err, "exactly one")
	_, err = stampRouteMeta(nil, elicitationScope{})
	require.Error(t, err)
	_, err = stampRouteMeta(nil, elicitationScope{SessionID: "s", TurnNonce: strings.Repeat("n", routeTurnNonceMaxBytes+1)})
	require.ErrorContains(t, err, "maximum size")
	boundaryStamped, err := stampRouteMeta(nil, elicitationScope{
		SessionID: "s", TurnNonce: boundaryNonce, RequestID: "request-boundary",
	})
	require.NoError(t, err)
	require.Equal(t, boundaryNonce, anyMap(t, boundaryStamped[routeMetaKey])[routeFieldTurn])
	generated, err := stampRouteMeta(nil, elicitationScope{SessionID: "s", TurnNonce: "t"})
	require.NoError(t, err)
	generatedRoute, ok := generated[routeMetaKey].(map[string]any)
	require.True(t, ok)
	require.Len(t, generatedRoute["requestId"], 32)

	previous := routeRandRead
	routeRandRead = func([]byte) (int, error) { return 0, errors.New("entropy") }
	t.Cleanup(func() { routeRandRead = previous })
	_, err = stampRouteMeta(nil, elicitationScope{SessionID: "s", TurnNonce: "t"})
	require.ErrorContains(t, err, "entropy")
}

type routeRecordingPiClient struct {
	directAgentClient
	notifications []acp.SessionNotification
}

func (c *routeRecordingPiClient) SessionUpdate(_ context.Context, notification acp.SessionNotification) error {
	c.notifications = append(c.notifications, notification)

	return nil
}

func TestTurnScopedNotificationsCarryExactRoute(t *testing.T) {
	agent := NewAgent()
	client := &routeRecordingPiClient{directAgentClient: *newDirectAgentClient()}
	agent.setConnection(client)
	session := &agentSession{agent: agent, id: "session", rawMessages: rawMessageConfig{All: true}}
	ctx := withTurnRoute(context.Background(), "turn-old")

	require.NoError(t, session.emitUpdates(ctx, []acp.SessionUpdate{acp.UpdateAgentMessageText("late")}))
	require.Equal(t, turnRouteMeta("turn-old"), client.notifications[0].Meta)

	session.emitRawPiEvent(ctx, []byte(`{"type":"late"}`))
	require.Equal(t, turnRouteMeta("turn-old"), client.notified[0]["_meta"])
}

func TestInitializeAdvertisesRouteV1(t *testing.T) {
	resp, err := NewAgent().Initialize(context.Background(), acp.InitializeRequest{})
	require.NoError(t, err)
	require.Equal(t, map[string]any{"versions": []int{1}}, resp.AgentCapabilities.Meta[routeMetaKey])
}

func TestPromptAndActiveCancelRequireCurrentRoute(t *testing.T) {
	turnCtx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	session := &agentSession{id: "session-1", cancel: cancel, turnNonce: "active-turn"}
	agent := NewAgent()
	agent.sessions[session.id] = session

	_, err := agent.Prompt(turnCtx, acp.PromptRequest{SessionId: session.id})
	require.Error(t, err)
	_, err = session.Prompt(turnCtx, acp.PromptRequest{SessionId: session.id})
	require.Error(t, err)
	require.Error(t, agent.Cancel(turnCtx, acp.CancelNotification{SessionId: session.id}))
	require.Error(t, agent.Cancel(turnCtx, CancelRequest(session.id, "stale-turn")))
}
