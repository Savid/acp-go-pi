package pi

import (
	"encoding/json"
	"fmt"
)

// Native event type strings.
const (
	EventTypeAgentStart          = "agent_start"
	EventTypeAgentEnd            = "agent_end"
	EventTypeAgentSettled        = "agent_settled"
	EventTypeTurnStart           = "turn_start"
	EventTypeTurnEnd             = "turn_end"
	EventTypeMessageStart        = "message_start"
	EventTypeMessageUpdate       = "message_update"
	EventTypeMessageEnd          = "message_end"
	EventTypeToolExecutionStart  = "tool_execution_start"
	EventTypeToolExecutionUpdate = "tool_execution_update"
	EventTypeToolExecutionEnd    = "tool_execution_end"
	EventTypeExtensionError      = "extension_error"
)

// Event is one decoded pi RPC agent event.
type Event interface {
	// RawJSON returns the raw event line bytes.
	RawJSON() json.RawMessage
}

type baseEvent struct {
	raw json.RawMessage
}

// RawJSON returns the raw event line bytes.
func (e baseEvent) RawJSON() json.RawMessage {
	return e.raw
}

// Usage is per-assistant-message token usage as reported by the harness.
type Usage struct {
	Input      int64      `json:"input"`
	Output     int64      `json:"output"`
	CacheRead  int64      `json:"cacheRead"`
	CacheWrite int64      `json:"cacheWrite"`
	Cost       *UsageCost `json:"cost,omitempty"`
}

// UsageCost is per-assistant-message cost as reported by the harness.
type UsageCost struct {
	Input      float64 `json:"input"`
	Output     float64 `json:"output"`
	CacheRead  float64 `json:"cacheRead"`
	CacheWrite float64 `json:"cacheWrite"`
	Total      float64 `json:"total"`
}

// ContentBlock is one message content block (text, thinking, toolCall, or
// image).
type ContentBlock struct {
	Type      string          `json:"type"`
	Text      string          `json:"text,omitempty"`
	Thinking  string          `json:"thinking,omitempty"`
	ID        string          `json:"id,omitempty"`
	Name      string          `json:"name,omitempty"`
	Arguments json.RawMessage `json:"arguments,omitempty"`
	Data      string          `json:"data,omitempty"`
	MimeType  string          `json:"mimeType,omitempty"`
}

// AgentMessage is one pi conversation message. Role selects which fields are
// populated: assistant messages carry usage/stopReason/errorMessage, tool
// result messages carry toolCallId/isError.
type AgentMessage struct {
	ACPMessageID string          `json:"acpMessageId,omitempty"`
	Role         string          `json:"role"`
	Content      json.RawMessage `json:"content,omitempty"`
	Usage        *Usage          `json:"usage,omitempty"`
	StopReason   string          `json:"stopReason,omitempty"`
	ErrorMessage string          `json:"errorMessage,omitempty"`
	ToolCallID   string          `json:"toolCallId,omitempty"`
	IsError      bool            `json:"isError,omitempty"`
}

// ContentBlocks decodes the message content, which is either a plain string
// (user messages) or an array of content blocks.
func (m AgentMessage) ContentBlocks() ([]ContentBlock, error) {
	if len(m.Content) == 0 {
		return nil, nil
	}

	var text string
	if err := json.Unmarshal(m.Content, &text); err == nil {
		return []ContentBlock{{Type: "text", Text: text}}, nil
	}

	var blocks []ContentBlock
	if err := json.Unmarshal(m.Content, &blocks); err != nil {
		return nil, fmt.Errorf("decode message content blocks: %w", err)
	}

	return blocks, nil
}

// AssistantMessageEvent is one streaming delta inside a message_update event.
type AssistantMessageEvent struct {
	Type  string `json:"type"`
	Delta string `json:"delta,omitempty"`
}

// ToolResult is a tool execution result or accumulated partial result.
type ToolResult struct {
	Content []ContentBlock `json:"content"`
}

// AgentStartEvent signals the agent began processing a prompt.
type AgentStartEvent struct {
	baseEvent
}

// AgentEndEvent signals one low-level agent run completed.
type AgentEndEvent struct {
	baseEvent
}

// AgentSettledEvent signals the run fully settled: no automatic retry,
// compaction retry, or queued continuation remains. It fires exactly once per
// accepted prompt and is the point at which pi's session file holds the whole
// run.
type AgentSettledEvent struct {
	baseEvent
}

// TurnStartEvent signals a new turn began.
type TurnStartEvent struct {
	baseEvent
}

// TurnEndEvent signals a turn completed.
type TurnEndEvent struct {
	baseEvent
}

// MessageStartEvent signals a message began.
type MessageStartEvent struct {
	baseEvent

	Message AgentMessage `json:"message"`
}

// MessageUpdateEvent streams one assistant message delta.
type MessageUpdateEvent struct {
	baseEvent

	AssistantMessageEvent AssistantMessageEvent `json:"assistantMessageEvent"`
}

// MessageEndEvent signals a message completed. For assistant messages the
// message carries usage, stopReason, and errorMessage.
type MessageEndEvent struct {
	baseEvent

	Message AgentMessage `json:"message"`
}

// ToolExecutionStartEvent signals a tool began executing.
type ToolExecutionStartEvent struct {
	baseEvent

	ToolCallID string          `json:"toolCallId"`
	ToolName   string          `json:"toolName"`
	Args       json.RawMessage `json:"args,omitempty"`
}

// ToolExecutionUpdateEvent streams accumulated partial tool output.
type ToolExecutionUpdateEvent struct {
	baseEvent

	ToolCallID    string          `json:"toolCallId"`
	ToolName      string          `json:"toolName"`
	Args          json.RawMessage `json:"args,omitempty"`
	PartialResult *ToolResult     `json:"partialResult,omitempty"`
}

// ToolExecutionEndEvent signals a tool completed.
type ToolExecutionEndEvent struct {
	baseEvent

	ToolCallID string      `json:"toolCallId"`
	ToolName   string      `json:"toolName"`
	Result     *ToolResult `json:"result,omitempty"`
	IsError    bool        `json:"isError"`
}

// ExtensionErrorEvent signals an extension threw an error.
type ExtensionErrorEvent struct {
	baseEvent

	ExtensionPath string `json:"extensionPath"`
	Event         string `json:"event"`
	Error         string `json:"error"`
}

// UnknownEvent carries an event type this package does not model: a queue
// report, compaction, an automatic retry pair, or a type pi added. The adapter
// acts on none of them and keeps the raw payload for raw-event forwarding.
type UnknownEvent struct {
	baseEvent

	EventType string
}

func decodeEvent(eventType string, line []byte, raw json.RawMessage) (Event, error) {
	base := baseEvent{raw: raw}

	var (
		event Event
		err   error
	)

	switch eventType {
	case EventTypeAgentStart:
		event = AgentStartEvent{baseEvent: base}
	case EventTypeAgentEnd:
		event = AgentEndEvent{baseEvent: base}
	case EventTypeAgentSettled:
		event = AgentSettledEvent{baseEvent: base}
	case EventTypeTurnStart:
		event = TurnStartEvent{baseEvent: base}
	case EventTypeTurnEnd:
		event = TurnEndEvent{baseEvent: base}
	case EventTypeMessageStart:
		typed := MessageStartEvent{baseEvent: base}
		err = json.Unmarshal(line, &typed)
		event = typed
	case EventTypeMessageUpdate:
		typed := MessageUpdateEvent{baseEvent: base}
		err = json.Unmarshal(line, &typed)
		event = typed
	case EventTypeMessageEnd:
		typed := MessageEndEvent{baseEvent: base}
		err = json.Unmarshal(line, &typed)
		event = typed
	case EventTypeToolExecutionStart:
		typed := ToolExecutionStartEvent{baseEvent: base}
		err = json.Unmarshal(line, &typed)
		event = typed
	case EventTypeToolExecutionUpdate:
		typed := ToolExecutionUpdateEvent{baseEvent: base}
		err = json.Unmarshal(line, &typed)
		event = typed
	case EventTypeToolExecutionEnd:
		typed := ToolExecutionEndEvent{baseEvent: base}
		err = json.Unmarshal(line, &typed)
		event = typed
	case EventTypeExtensionError:
		typed := ExtensionErrorEvent{baseEvent: base}
		err = json.Unmarshal(line, &typed)
		event = typed
	default:
		event = UnknownEvent{baseEvent: base, EventType: eventType}
	}

	if err != nil {
		return nil, fmt.Errorf("decode %s event: %w", eventType, err)
	}

	return event, nil
}
