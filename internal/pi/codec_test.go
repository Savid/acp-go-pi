package pi

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDecodeMessage(t *testing.T) {
	t.Parallel()

	message, err := DecodeMessage([]byte(`{"type":"response","id":"1","command":"prompt","success":false,"error":"no"}`))
	require.NoError(t, err)
	require.Equal(t, MessageKindResponse, message.Kind)
	require.EqualError(t, message.Response.Err(), "pi command prompt failed: no")

	message, err = DecodeMessage([]byte(`{"type":"extension_ui_request","id":"u","method":"select","title":"t","options":["a"]}`))
	require.NoError(t, err)
	require.Equal(t, MessageKindUIRequest, message.Kind)
	require.True(t, message.UIRequest.IsDialog())

	message, err = DecodeMessage([]byte(`{"type":"extension_ui_request","id":"u","method":"notify"}`))
	require.NoError(t, err)
	require.False(t, message.UIRequest.IsDialog())

	message, err = DecodeMessage([]byte(`{"type":"extension_ui_request","id":"u","method":"setStatus","statusKey":"acp-go-pi:response","statusText":"gen-1790864381-f5Fi7D0M1fyN1HUUIrAz"}`))
	require.NoError(t, err)
	require.Equal(t, MessageKindEvent, message.Kind, "the bridge's response-id relay is an event in stream order")
	relay, ok := message.Event.(ResponseIDEvent)
	require.True(t, ok)
	require.Equal(t, "gen-1790864381-f5Fi7D0M1fyN1HUUIrAz", relay.ResponseID)

	message, err = DecodeMessage([]byte(`{"type":"extension_ui_request","id":"u","method":"setStatus","statusKey":"other","statusText":"gen-1"}`))
	require.NoError(t, err)
	require.Equal(t, MessageKindUIRequest, message.Kind, "another extension's status stays a UI request")

	message, err = DecodeMessage([]byte(`{"type":"agent_start"}`))
	require.NoError(t, err)
	require.Equal(t, MessageKindEvent, message.Kind)
	require.IsType(t, AgentStartEvent{}, message.Event)

	message, err = DecodeMessage([]byte(`{"type":"something_new","x":1}`))
	require.NoError(t, err)
	unknown, ok := message.Event.(UnknownEvent)
	require.True(t, ok)
	require.Equal(t, "something_new", unknown.EventType)

	_, err = DecodeMessage([]byte(`not json`))
	require.Error(t, err)

	_, err = DecodeMessage([]byte(`{"type":"tool_execution_end","toolCallId":1}`))
	require.Error(t, err)
}

func TestUIResponses(t *testing.T) {
	t.Parallel()

	value := UIValueResponse("1", "v")
	require.Equal(t, "v", *value.Value)
	require.Equal(t, uiResponseType, value.Type)

	confirm := UIConfirmResponse("1", false)
	require.False(t, *confirm.Confirmed)

	require.True(t, UICancelResponse("1").Cancelled)
}

func TestDecodeMessageTypePlacement(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		line string
		kind MessageKind
	}{
		{"id before type", `{"id":"acp-1","type":"response","command":"get_state","success":true,"data":{"a":1}}`, MessageKindResponse},
		{"compound before type", `{"data":{"type":"x"},"id":"acp-1","type":"response","command":"c","success":true}`, MessageKindResponse},
		{"escaped type", `{"type":"extension_ui_request","id":"u","method":"confirm"}`, MessageKindUIRequest},
		{"type last", `{"message":{"role":"user"},"type":"message_end"}`, MessageKindEvent},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			message, err := DecodeMessage([]byte(tc.line))
			require.NoError(t, err)
			require.Equal(t, tc.kind, message.Kind)
		})
	}

	message, err := DecodeMessage([]byte(`{"data":{"type":"x"},"type":"message_end","message":{"role":"assistant"}}`))
	require.NoError(t, err)
	end, ok := message.Event.(MessageEndEvent)
	require.True(t, ok)
	require.Equal(t, "assistant", end.Message.Role)
}

func TestDecodeMessageRetainsLine(t *testing.T) {
	t.Parallel()

	line := []byte(`{"type":"agent_start"}`)

	message, err := DecodeMessage(line)
	require.NoError(t, err)
	require.Same(t, &line[0], &message.Event.RawJSON()[0], "the record is retained, not copied")
}

func TestDecodeMessageRejectsMalformedTail(t *testing.T) {
	t.Parallel()

	for _, line := range []string{`{"type":"agent_start",}`, `{"type":"queue_update","x":}`, `{"type":"turn_end"`} {
		_, err := DecodeMessage([]byte(line))
		require.Error(t, err, line)
	}
}
