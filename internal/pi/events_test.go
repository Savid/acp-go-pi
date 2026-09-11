package pi

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDecodeEventKinds(t *testing.T) {
	t.Parallel()

	cases := map[string]string{
		EventTypeAgentEnd:            `{"type":"agent_end","messages":[],"willRetry":true}`,
		EventTypeAgentSettled:        `{"type":"agent_settled"}`,
		EventTypeTurnStart:           `{"type":"turn_start"}`,
		EventTypeTurnEnd:             `{"type":"turn_end","message":{},"toolResults":[]}`,
		EventTypeMessageStart:        `{"type":"message_start","message":{"role":"assistant"}}`,
		EventTypeMessageUpdate:       `{"type":"message_update","assistantMessageEvent":{"type":"text_delta","delta":"x"}}`,
		EventTypeMessageEnd:          `{"type":"message_end","message":{"role":"assistant","content":"hi"}}`,
		EventTypeToolExecutionStart:  `{"type":"tool_execution_start","toolCallId":"c","toolName":"bash","args":{}}`,
		EventTypeToolExecutionUpdate: `{"type":"tool_execution_update","toolCallId":"c","toolName":"bash","partialResult":{"content":[]}}`,
		EventTypeToolExecutionEnd:    `{"type":"tool_execution_end","toolCallId":"c","toolName":"bash","isError":false}`,
		EventTypeQueueUpdate:         `{"type":"queue_update","steering":[],"followUp":["x"]}`,
		EventTypeCompactionStart:     `{"type":"compaction_start","reason":"threshold"}`,
		EventTypeCompactionEnd:       `{"type":"compaction_end","reason":"threshold","aborted":false}`,
		EventTypeAutoRetryStart:      `{"type":"auto_retry_start","attempt":1,"maxAttempts":3,"delayMs":10}`,
		EventTypeAutoRetryEnd:        `{"type":"auto_retry_end","success":true,"attempt":1}`,
		EventTypeExtensionError:      `{"type":"extension_error","extensionPath":"/x.ts","event":"tool_call","error":"boom"}`,
	}

	for kind, line := range cases {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()

			message, err := DecodeMessage([]byte(line))
			require.NoError(t, err)
			require.Equal(t, kind, message.Event.Kind())
			require.JSONEq(t, line, string(message.Event.RawJSON()))
		})
	}
}

func TestAgentMessageContentBlocks(t *testing.T) {
	t.Parallel()

	blocks, err := AgentMessage{Content: json.RawMessage(`"plain"`)}.ContentBlocks()
	require.NoError(t, err)
	require.Equal(t, []ContentBlock{{Type: ContentBlockTypeText, Text: "plain"}}, blocks)

	blocks, err = AgentMessage{Content: json.RawMessage(`[{"type":"thinking","thinking":"t"}]`)}.ContentBlocks()
	require.NoError(t, err)
	require.Equal(t, "t", blocks[0].Thinking)

	blocks, err = AgentMessage{}.ContentBlocks()
	require.NoError(t, err)
	require.Nil(t, blocks)

	_, err = AgentMessage{Content: json.RawMessage(`{}`)}.ContentBlocks()
	require.Error(t, err)
}
