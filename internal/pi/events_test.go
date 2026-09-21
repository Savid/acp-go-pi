package pi

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDecodeEventKinds(t *testing.T) {
	t.Parallel()

	cases := map[string]struct {
		line  string
		event Event
	}{
		EventTypeAgentStart:          {`{"type":"agent_start"}`, AgentStartEvent{}},
		EventTypeAgentEnd:            {`{"type":"agent_end","messages":[],"willRetry":true}`, AgentEndEvent{}},
		EventTypeAgentSettled:        {`{"type":"agent_settled"}`, AgentSettledEvent{}},
		EventTypeTurnStart:           {`{"type":"turn_start"}`, TurnStartEvent{}},
		EventTypeTurnEnd:             {`{"type":"turn_end","message":{},"toolResults":[]}`, TurnEndEvent{}},
		EventTypeMessageStart:        {`{"type":"message_start","message":{"role":"assistant"}}`, MessageStartEvent{}},
		EventTypeMessageUpdate:       {`{"type":"message_update","assistantMessageEvent":{"type":"text_delta","delta":"x"}}`, MessageUpdateEvent{}},
		EventTypeMessageEnd:          {`{"type":"message_end","message":{"role":"assistant","content":"hi"}}`, MessageEndEvent{}},
		EventTypeToolExecutionStart:  {`{"type":"tool_execution_start","toolCallId":"c","toolName":"bash","args":{}}`, ToolExecutionStartEvent{}},
		EventTypeToolExecutionUpdate: {`{"type":"tool_execution_update","toolCallId":"c","toolName":"bash","partialResult":{"content":[]}}`, ToolExecutionUpdateEvent{}},
		EventTypeToolExecutionEnd:    {`{"type":"tool_execution_end","toolCallId":"c","toolName":"bash","isError":false}`, ToolExecutionEndEvent{}},
		EventTypeExtensionError:      {`{"type":"extension_error","extensionPath":"/x.ts","event":"tool_call","error":"boom"}`, ExtensionErrorEvent{}},
		"queue_update":               {`{"type":"queue_update","steering":[],"followUp":["x"]}`, UnknownEvent{}},
		"compaction_start":           {`{"type":"compaction_start","reason":"threshold"}`, UnknownEvent{}},
		"auto_retry_end":             {`{"type":"auto_retry_end","success":true,"attempt":1}`, UnknownEvent{}},
	}

	for kind, tc := range cases {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()

			message, err := DecodeMessage([]byte(tc.line))
			require.NoError(t, err)
			require.IsType(t, tc.event, message.Event)
			require.JSONEq(t, tc.line, string(message.Event.RawJSON()))
		})
	}
}

func TestAgentMessageContentBlocks(t *testing.T) {
	t.Parallel()

	blocks, err := AgentMessage{Content: json.RawMessage(`"plain"`)}.ContentBlocks()
	require.NoError(t, err)
	require.Equal(t, []ContentBlock{{Type: "text", Text: "plain"}}, blocks)

	blocks, err = AgentMessage{Content: json.RawMessage(`[{"type":"thinking","thinking":"t"}]`)}.ContentBlocks()
	require.NoError(t, err)
	require.Equal(t, "t", blocks[0].Thinking)

	blocks, err = AgentMessage{}.ContentBlocks()
	require.NoError(t, err)
	require.Nil(t, blocks)

	_, err = AgentMessage{Content: json.RawMessage(`{}`)}.ContentBlocks()
	require.Error(t, err)
}
