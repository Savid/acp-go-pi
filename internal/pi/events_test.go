package pi

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func decodeEventLine(t *testing.T, line string) Event {
	t.Helper()

	message, err := DecodeMessage([]byte(line))
	require.NoError(t, err)
	require.Equal(t, MessageKindEvent, message.Kind)
	require.JSONEq(t, line, string(message.Event.RawJSON()))

	return message.Event
}

func TestDecodeEventTypes(t *testing.T) {
	t.Parallel()

	t.Run("agent lifecycle", func(t *testing.T) {
		t.Parallel()

		require.IsType(t, AgentStartEvent{}, decodeEventLine(t, `{"type":"agent_start"}`))
		require.IsType(t, AgentSettledEvent{}, decodeEventLine(t, `{"type":"agent_settled"}`))
		require.IsType(t, TurnStartEvent{}, decodeEventLine(t, `{"type":"turn_start"}`))

		agentEnd, ok := decodeEventLine(t, `{"type":"agent_end","messages":[{"role":"assistant"}],"willRetry":true}`).(AgentEndEvent)
		require.True(t, ok)
		require.Len(t, agentEnd.Messages, 1)
		require.True(t, agentEnd.WillRetry)

		turnEnd, ok := decodeEventLine(t, `{"type":"turn_end","message":{"role":"assistant"},"toolResults":[{"role":"toolResult"}]}`).(TurnEndEvent)
		require.True(t, ok)
		require.NotEmpty(t, turnEnd.Message)
		require.Len(t, turnEnd.ToolResults, 1)
	})

	t.Run("message stream", func(t *testing.T) {
		t.Parallel()

		start, ok := decodeEventLine(t, `{"type":"message_start","message":{"role":"assistant"}}`).(MessageStartEvent)
		require.True(t, ok)
		require.Equal(t, "assistant", start.Message.Role)

		update, ok := decodeEventLine(t,
			`{"type":"message_update","message":{"role":"assistant"},`+
				`"assistantMessageEvent":{"type":"text_delta","contentIndex":0,"delta":"Hello ","partial":{}}}`,
		).(MessageUpdateEvent)
		require.True(t, ok)
		require.Equal(t, "text_delta", update.AssistantMessageEvent.Type)
		require.Equal(t, "Hello ", update.AssistantMessageEvent.Delta)

		toolcallEnd, ok := decodeEventLine(t,
			`{"type":"message_update","message":{},`+
				`"assistantMessageEvent":{"type":"toolcall_end","contentIndex":1,`+
				`"toolCall":{"type":"toolCall","id":"call_1","name":"bash","arguments":{"command":"ls"}}}}`,
		).(MessageUpdateEvent)
		require.True(t, ok)
		require.NotEmpty(t, toolcallEnd.AssistantMessageEvent.ToolCall)

		end, ok := decodeEventLine(t,
			`{"type":"message_end","message":{"role":"assistant",`+
				`"content":[{"type":"text","text":"Hi"}],"provider":"openai","model":"gpt-4o",`+
				`"usage":{"input":100,"output":50,"cacheRead":1,"cacheWrite":2,`+
				`"cost":{"input":0.1,"output":0.2,"cacheRead":0,"cacheWrite":0,"total":0.3}},`+
				`"stopReason":"stop","timestamp":1733234567890}}`,
		).(MessageEndEvent)
		require.True(t, ok)
		require.Equal(t, "stop", end.Message.StopReason)
		require.NotNil(t, end.Message.Usage)
		require.Equal(t, int64(100), end.Message.Usage.Input)
		require.NotNil(t, end.Message.Usage.Cost)
		require.InDelta(t, 0.3, end.Message.Usage.Cost.Total, 1e-9)

		blocks, err := end.Message.ContentBlocks()
		require.NoError(t, err)
		require.Len(t, blocks, 1)
		require.Equal(t, "Hi", blocks[0].Text)
	})

	t.Run("error message end", func(t *testing.T) {
		t.Parallel()

		end, ok := decodeEventLine(t,
			`{"type":"message_end","message":{"role":"assistant","content":[],`+
				`"stopReason":"error","errorMessage":"No API key for provider: x"}}`,
		).(MessageEndEvent)
		require.True(t, ok)
		require.Equal(t, "error", end.Message.StopReason)
		require.Equal(t, "No API key for provider: x", end.Message.ErrorMessage)
	})

	t.Run("tool execution", func(t *testing.T) {
		t.Parallel()

		start, ok := decodeEventLine(t,
			`{"type":"tool_execution_start","toolCallId":"call_1","toolName":"bash","args":{"command":"ls"}}`,
		).(ToolExecutionStartEvent)
		require.True(t, ok)
		require.Equal(t, "call_1", start.ToolCallID)
		require.Equal(t, "bash", start.ToolName)

		update, ok := decodeEventLine(t,
			`{"type":"tool_execution_update","toolCallId":"call_1","toolName":"bash",`+
				`"partialResult":{"content":[{"type":"text","text":"partial"}],"details":{}}}`,
		).(ToolExecutionUpdateEvent)
		require.True(t, ok)
		require.NotNil(t, update.PartialResult)
		require.Equal(t, "partial", update.PartialResult.Content[0].Text)

		end, ok := decodeEventLine(t,
			`{"type":"tool_execution_end","toolCallId":"call_1","toolName":"bash",`+
				`"result":{"content":[{"type":"text","text":"done"}]},"isError":true}`,
		).(ToolExecutionEndEvent)
		require.True(t, ok)
		require.True(t, end.IsError)
		require.Equal(t, "done", end.Result.Content[0].Text)
	})

	t.Run("queue compaction retry extension", func(t *testing.T) {
		t.Parallel()

		queue, ok := decodeEventLine(t,
			`{"type":"queue_update","steering":["s1"],"followUp":["f1","f2"]}`,
		).(QueueUpdateEvent)
		require.True(t, ok)
		require.Equal(t, []string{"s1"}, queue.Steering)
		require.Len(t, queue.FollowUp, 2)

		compactionStart, ok := decodeEventLine(t,
			`{"type":"compaction_start","reason":"threshold"}`,
		).(CompactionStartEvent)
		require.True(t, ok)
		require.Equal(t, "threshold", compactionStart.Reason)

		compactionEnd, ok := decodeEventLine(t,
			`{"type":"compaction_end","reason":"overflow","result":null,"aborted":false,`+
				`"willRetry":false,"errorMessage":"quota exceeded"}`,
		).(CompactionEndEvent)
		require.True(t, ok)
		require.Equal(t, "quota exceeded", compactionEnd.ErrorMessage)

		retryStart, ok := decodeEventLine(t,
			`{"type":"auto_retry_start","attempt":1,"maxAttempts":3,"delayMs":2000,"errorMessage":"529"}`,
		).(AutoRetryStartEvent)
		require.True(t, ok)
		require.Equal(t, 1, retryStart.Attempt)
		require.InDelta(t, 2000.0, retryStart.DelayMs, 0)

		retryEnd, ok := decodeEventLine(t,
			`{"type":"auto_retry_end","success":false,"attempt":3,"finalError":"overloaded"}`,
		).(AutoRetryEndEvent)
		require.True(t, ok)
		require.False(t, retryEnd.Success)
		require.Equal(t, "overloaded", retryEnd.FinalError)

		extensionErr, ok := decodeEventLine(t,
			`{"type":"extension_error","extensionPath":"/x.ts","event":"tool_call","error":"boom"}`,
		).(ExtensionErrorEvent)
		require.True(t, ok)
		require.Equal(t, "boom", extensionErr.Error)
	})

	t.Run("unknown event keeps type and raw payload", func(t *testing.T) {
		t.Parallel()

		unknown, ok := decodeEventLine(t, `{"type":"session_info","name":"x"}`).(UnknownEvent)
		require.True(t, ok)
		require.Equal(t, "session_info", unknown.Kind())
	})

	t.Run("malformed typed event fails", func(t *testing.T) {
		t.Parallel()

		_, err := DecodeMessage([]byte(`{"type":"queue_update","steering":"nope"}`))
		require.Error(t, err)
	})
}

func TestAgentMessageContentBlocks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		message AgentMessage
		want    []ContentBlock
		wantErr bool
	}{
		{
			name:    "empty content",
			message: AgentMessage{},
			want:    nil,
		},
		{
			name:    "string content becomes one text block",
			message: AgentMessage{Content: json.RawMessage(`"hello"`)},
			want:    []ContentBlock{{Type: "text", Text: "hello"}},
		},
		{
			name: "block array",
			message: AgentMessage{Content: json.RawMessage(
				`[{"type":"thinking","thinking":"hm"},{"type":"toolCall","id":"c1","name":"bash","arguments":{"command":"ls"}}]`,
			)},
			want: []ContentBlock{
				{Type: "thinking", Thinking: "hm"},
				{Type: "toolCall", ID: "c1", Name: "bash", Arguments: json.RawMessage(`{"command":"ls"}`)},
			},
		},
		{
			name:    "invalid content",
			message: AgentMessage{Content: json.RawMessage(`42`)},
			wantErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			blocks, err := test.message.ContentBlocks()
			if test.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.Equal(t, test.want, blocks)
		})
	}
}

// TestEventKindStrings pins every typed event's Kind to its native type
// string.
func TestEventKindStrings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		line string
		want string
	}{
		{line: `{"type":"agent_start"}`, want: "agent_start"},
		{line: `{"type":"agent_end","messages":[]}`, want: "agent_end"},
		{line: `{"type":"agent_settled"}`, want: "agent_settled"},
		{line: `{"type":"turn_start"}`, want: "turn_start"},
		{line: `{"type":"turn_end"}`, want: "turn_end"},
		{line: `{"type":"message_start","message":{}}`, want: "message_start"},
		{line: `{"type":"message_update","message":{},"assistantMessageEvent":{"type":"start"}}`, want: "message_update"},
		{line: `{"type":"message_end","message":{}}`, want: "message_end"},
		{line: `{"type":"tool_execution_start","toolCallId":"c","toolName":"bash"}`, want: "tool_execution_start"},
		{line: `{"type":"tool_execution_update","toolCallId":"c","toolName":"bash"}`, want: "tool_execution_update"},
		{line: `{"type":"tool_execution_end","toolCallId":"c","toolName":"bash","isError":false}`, want: "tool_execution_end"},
		{line: `{"type":"queue_update","steering":[],"followUp":[]}`, want: "queue_update"},
		{line: `{"type":"compaction_start","reason":"manual"}`, want: "compaction_start"},
		{line: `{"type":"compaction_end","reason":"manual","aborted":true}`, want: "compaction_end"},
		{line: `{"type":"auto_retry_start","attempt":1}`, want: "auto_retry_start"},
		{line: `{"type":"auto_retry_end","success":true,"attempt":1}`, want: "auto_retry_end"},
		{line: `{"type":"extension_error","extensionPath":"/x.ts","event":"e","error":"boom"}`, want: "extension_error"},
		{line: `{"type":"something_new"}`, want: "something_new"},
	}

	for _, test := range tests {
		t.Run(test.want, func(t *testing.T) {
			t.Parallel()

			event := decodeEventLine(t, test.line)
			require.Equal(t, test.want, event.Kind())
		})
	}
}
