package piacp

import (
	"context"
	"errors"
	"io"
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

	require.Nil(t, requestError(nil))
	requestErr := acp.NewInvalidParams(nil)
	require.Same(t, requestErr, requestError(requestErr))
	require.Equal(t, -32800, requestError(context.Canceled).Code)
	require.Equal(t, -32603, requestError(errors.New("failure")).Code)
	require.Same(t, requestErr, lifecycleMetaError(requestErr))
	var lifecycleRequestError *acp.RequestError
	require.ErrorAs(t, lifecycleMetaError(errors.New("bad meta")), &lifecycleRequestError)
	require.Equal(t, -32602, lifecycleRequestError.Code)
}
