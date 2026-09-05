package piacp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"
)

type routeEnvelopeSDKClient struct {
	*conformanceClient
	received chan acp.UnstableCreateElicitationRequest
}

func (c *routeEnvelopeSDKClient) UnstableCreateElicitation(
	_ context.Context,
	params acp.UnstableCreateElicitationRequest,
) (acp.UnstableCreateElicitationResponse, error) {
	c.received <- params

	return acp.UnstableCreateElicitationResponse{
		Accept: &acp.UnstableCreateElicitationAccept{Action: "accept", Content: map[string]any{"ok": true}},
	}, nil
}

func TestSDKPipeDecodesExactRouteEnvelope(t *testing.T) {
	clientToAgentReader, clientToAgentWriter := io.Pipe()
	agentToClientReader, agentToClientWriter := io.Pipe()
	agent := NewAgent()
	agentConnection := newLocalAgentConnection(agent, agentToClientWriter, clientToAgentReader)
	agent.setConnection(agentConnection)

	client := &routeEnvelopeSDKClient{
		conformanceClient: &conformanceClient{},
		received:          make(chan acp.UnstableCreateElicitationRequest, 1),
	}
	clientConnection := acp.NewClientSideConnection(client, clientToAgentWriter, agentToClientReader)

	t.Cleanup(func() {
		_ = clientToAgentReader.Close()
		_ = clientToAgentWriter.Close()
		_ = agentToClientReader.Close()
		_ = agentToClientWriter.Close()

		select {
		case <-clientConnection.Done():
		case <-time.After(time.Second):
			t.Error("SDK client connection did not stop")
		}
	})

	_, err := agentConnection.CreateElicitation(t.Context(), acp.UnstableCreateElicitationRequest{
		Form: &acp.UnstableCreateElicitationForm{
			Message:         "route",
			Mode:            elicitationModeForm,
			RequestedSchema: acp.UnstableElicitationSchema{Type: acp.UnstableElicitationSchemaTypeObject},
			Meta:            map[string]any{"native": map[string]any{"variant": "form"}},
		},
	}, elicitationScope{
		SessionID: "session-1",
		TurnNonce: "turn-1",
		RequestID: "request-1",
	})
	require.NoError(t, err)

	decoded := <-client.received
	require.NotNil(t, decoded.Form)
	require.Nil(t, decoded.Url)
	require.Equal(t, map[string]any{"variant": "form"}, decoded.Form.Meta["native"])
	require.Equal(t, map[string]any{
		routeFieldVer:  float64(1),
		routeFieldID:   "session-1",
		routeFieldTurn: "turn-1",
		"requestId":    "request-1",
	}, decoded.Form.Meta[routeMetaKey])

	_, err = agentConnection.CreateElicitation(t.Context(), acp.UnstableCreateElicitationRequest{
		Url: &acp.UnstableCreateElicitationUrl{
			ElicitationId: "url-1",
			Message:       "open",
			Mode:          elicitationModeURL,
			Url:           "https://example.test",
			Meta:          map[string]any{"native": map[string]any{"variant": "url"}},
		},
	}, elicitationScope{
		SessionID:  "session-2",
		TurnNonce:  "turn-2",
		ToolCallID: "tool-1",
	})
	require.NoError(t, err)

	decoded = <-client.received
	require.Nil(t, decoded.Form)
	require.NotNil(t, decoded.Url)
	require.Equal(t, map[string]any{"variant": "url"}, decoded.Url.Meta["native"])
	require.Equal(t, map[string]any{
		routeFieldVer:  float64(1),
		routeFieldID:   "session-2",
		routeFieldTurn: "turn-2",
		"toolCallId":   "tool-1",
	}, decoded.Url.Meta[routeMetaKey])
}

func TestAgentConnectionAndErrorMapping(t *testing.T) {
	_, err := scopedElicitationParams(acp.UnstableCreateElicitationRequest{}, elicitationScope{})
	require.Error(t, err)
	params := acp.UnstableCreateElicitationRequest{Form: &acp.UnstableCreateElicitationForm{
		Message: "message", Mode: elicitationModeForm,
		RequestedSchema: acp.UnstableElicitationSchema{Type: acp.UnstableElicitationSchemaTypeObject},
		Meta:            map[string]any{"x": true},
	}}
	raw, err := scopedElicitationParams(params, elicitationScope{SessionID: "session", TurnNonce: "turn-1", ToolCallID: "tool"})
	require.NoError(t, err)
	require.Contains(t, string(raw), "toolCallId")

	params.Form.Meta = map[string]any{routeMetaKey: map[string]any{}}
	_, err = scopedElicitationParams(params, elicitationScope{SessionID: "session", TurnNonce: "turn-1", ToolCallID: "tool"})
	require.ErrorContains(t, err, "collision")

	urlRaw, err := scopedElicitationParams(acp.UnstableCreateElicitationRequest{
		Url: &acp.UnstableCreateElicitationUrl{
			ElicitationId: "url-1", Message: "open", Mode: elicitationModeURL,
			Url: "https://example.test", Meta: map[string]any{"native": true},
		},
	}, elicitationScope{SessionID: "session", TurnNonce: "turn-2", RequestID: "request-1"})
	require.NoError(t, err)
	require.JSONEq(t, `{
		"elicitationId":"url-1","message":"open","mode":"url","url":"https://example.test",
		"_meta":{"native":true,"acp-go.dev/route":{
			"version":1,"sessionId":"session","turnNonce":"turn-2","requestId":"request-1"
		}}
	}`, string(urlRaw))
	connection := &localAgentConnection{agent: NewAgent()}
	_, err = connection.CreateElicitation(t.Context(), acp.UnstableCreateElicitationRequest{}, elicitationScope{})
	require.Error(t, err)

	log := slog.New(slog.DiscardHandler)
	require.Nil(t, requestError(t.Context(), log, nil))

	requestErr := acp.NewInvalidParams(nil)
	require.Same(t, requestErr, requestError(t.Context(), log, requestErr))

	internal := requestError(t.Context(), log, errors.New("failure"))
	require.Equal(t, -32603, internal.Code)
	require.Equal(t, map[string]any{jsonFieldError: internalFailureError}, internal.Data,
		"an unclassified handler failure always carries the closed token and never a message member")
}

// An honored $/cancel_request is the only thing that cancels a request context
// with cause context.Canceled, so that cause outranks any RequestError the
// handler happened to return and carries no error text of its own.
func TestRequestErrorPrefersHonoredCancelOverEmbeddedRequestError(t *testing.T) {
	t.Parallel()

	log := slog.New(slog.DiscardHandler)

	cancelled, cancel := context.WithCancelCause(t.Context())
	cancel(context.Canceled)

	reqErr := requestError(cancelled, log, acp.NewInvalidParams(map[string]any{"error": "unsupported", "field": "cwd"}))
	require.Equal(t, -32800, reqErr.Code)
	require.Nil(t, reqErr.Data)

	// A wrapped context.Canceled that no honored cancel produced keeps the
	// caller-owned error it carries.
	wrapped := requestError(t.Context(), log, fmt.Errorf("drain: %w", context.Canceled))
	require.Equal(t, -32603, wrapped.Code)

	// An adapter deadline is an internal failure, never a client cancel.
	expired, cancelDeadline := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
	defer cancelDeadline()

	require.Equal(t, -32603, requestError(expired, log, context.DeadlineExceeded).Code)
}
