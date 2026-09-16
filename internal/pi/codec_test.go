package pi

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLineReaderStrictLF(t *testing.T) {
	t.Parallel()

	reader := NewLineReader(strings.NewReader("{\"a\":1}\r\n{\"b\":\"x y\"}\n"))

	line, err := reader.Next()
	require.NoError(t, err)
	require.Equal(t, `{"a":1}`, string(line))

	line, err = reader.Next()
	require.NoError(t, err)
	require.Equal(t, "{\"b\":\"x y\"}", string(line))

	_, err = reader.Next()
	require.Error(t, err)
}

func TestLineReaderUnterminatedRecordIsStructural(t *testing.T) {
	t.Parallel()

	reader := NewLineReader(strings.NewReader(`{"a":1}`))

	_, err := reader.Next()
	require.ErrorIs(t, err, ErrJSONLStructural)
}

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
