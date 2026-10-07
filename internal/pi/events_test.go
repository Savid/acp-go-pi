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
		EventTypeCompactionEnd:       {`{"type":"compaction_end","reason":"threshold","aborted":false,"willRetry":false}`, CompactionEndEvent{}},
		"queue_update":               {`{"type":"queue_update","steering":[],"followUp":["x"]}`, UnknownEvent{}},
		EventTypeCompactionStart:     {`{"type":"compaction_start","reason":"threshold"}`, CompactionStartEvent{}},
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

func TestCompactionEndResult(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		line      string
		compacted bool
	}{
		"compacted": {`{"type":"compaction_end","reason":"threshold","result":{"summary":"s","firstKeptEntryId":"e2","tokensBefore":900,"estimatedTokensAfter":120},"aborted":false,"willRetry":false}`, true},
		"failed":    {`{"type":"compaction_end","reason":"threshold","aborted":false,"willRetry":false,"errorMessage":"Auto-compaction failed: boom"}`, false},
		"aborted":   {`{"type":"compaction_end","reason":"manual","result":null,"aborted":true,"willRetry":false}`, false},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			message, err := DecodeMessage([]byte(tc.line))
			require.NoError(t, err)
			event, ok := message.Event.(CompactionEndEvent)
			require.True(t, ok)
			require.Equal(t, tc.compacted, event.Result != nil)
		})
	}
}

func TestMessageUpdateUsage(t *testing.T) {
	t.Parallel()

	message, err := DecodeMessage([]byte(`{"type":"message_update","usage":{"input":100,"output":1,"cacheRead":1000,"cacheWrite":50,"totalTokens":1151},"assistantMessageEvent":{"type":"text_start","contentIndex":0}}`))
	require.NoError(t, err)
	event, ok := message.Event.(MessageUpdateEvent)
	require.True(t, ok)
	require.Equal(t, &Usage{Input: 100, Output: 1, CacheRead: 1000, CacheWrite: 50, TotalTokens: 1151}, event.Usage)
	require.Equal(t, "text_start", event.AssistantMessageEvent.Type)
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
