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
	EventTypeQueueUpdate         = "queue_update"
	EventTypeCompactionStart     = "compaction_start"
	EventTypeCompactionEnd       = "compaction_end"
	EventTypeAutoRetryStart      = "auto_retry_start"
	EventTypeAutoRetryEnd        = "auto_retry_end"
	EventTypeExtensionError      = "extension_error"
)

// Event is one decoded pi RPC agent event.
type Event interface {
	// Kind returns the native event type string.
	Kind() string
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

// ContentBlockTypeText is the content block type carrying plain text.
const ContentBlockTypeText = "text"

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
// result messages carry toolCallId/toolName/isError.
type AgentMessage struct {
	ACPMessageID string          `json:"acpMessageId,omitempty"`
	Role         string          `json:"role"`
	Content      json.RawMessage `json:"content,omitempty"`
	Provider     string          `json:"provider,omitempty"`
	Model        string          `json:"model,omitempty"`
	Usage        *Usage          `json:"usage,omitempty"`
	StopReason   string          `json:"stopReason,omitempty"`
	ErrorMessage string          `json:"errorMessage,omitempty"`
	ToolCallID   string          `json:"toolCallId,omitempty"`
	ToolName     string          `json:"toolName,omitempty"`
	IsError      bool            `json:"isError,omitempty"`
	Timestamp    int64           `json:"timestamp,omitempty"`
}

// ContentBlocks decodes the message content, which is either a plain string
// (user messages) or an array of content blocks.
func (m AgentMessage) ContentBlocks() ([]ContentBlock, error) {
	if len(m.Content) == 0 {
		return nil, nil
	}

	var text string
	if err := json.Unmarshal(m.Content, &text); err == nil {
		return []ContentBlock{{Type: ContentBlockTypeText, Text: text}}, nil
	}

	var blocks []ContentBlock
	if err := json.Unmarshal(m.Content, &blocks); err != nil {
		return nil, fmt.Errorf("decode message content blocks: %w", err)
	}

	return blocks, nil
}

// AssistantMessageEvent is one streaming delta inside a message_update event.
type AssistantMessageEvent struct {
	Type         string          `json:"type"`
	ContentIndex int             `json:"contentIndex,omitempty"`
	Delta        string          `json:"delta,omitempty"`
	Content      string          `json:"content,omitempty"`
	Reason       string          `json:"reason,omitempty"`
	ToolCall     json.RawMessage `json:"toolCall,omitempty"`
	Partial      json.RawMessage `json:"partial,omitempty"`
}

// ToolResult is a tool execution result or accumulated partial result.
type ToolResult struct {
	Content []ContentBlock  `json:"content"`
	Details json.RawMessage `json:"details,omitempty"`
}

// AgentStartEvent signals the agent began processing a prompt.
type AgentStartEvent struct {
	baseEvent
}

// Kind returns the native event type string.
func (AgentStartEvent) Kind() string { return EventTypeAgentStart }

// AgentEndEvent signals one low-level agent run completed.
type AgentEndEvent struct {
	baseEvent

	Messages  []json.RawMessage `json:"messages"`
	WillRetry bool              `json:"willRetry"`
}

// Kind returns the native event type string.
func (AgentEndEvent) Kind() string { return EventTypeAgentEnd }

// AgentSettledEvent signals the run fully settled: no automatic retry,
// compaction retry, or queued continuation remains. It fires exactly once per
// accepted prompt and fences the mirror commit point.
type AgentSettledEvent struct {
	baseEvent
}

// Kind returns the native event type string.
func (AgentSettledEvent) Kind() string { return EventTypeAgentSettled }

// TurnStartEvent signals a new turn began.
type TurnStartEvent struct {
	baseEvent
}

// Kind returns the native event type string.
func (TurnStartEvent) Kind() string { return EventTypeTurnStart }

// TurnEndEvent signals a turn completed with its assistant message and tool
// results.
type TurnEndEvent struct {
	baseEvent

	Message     json.RawMessage   `json:"message"`
	ToolResults []json.RawMessage `json:"toolResults"`
}

// Kind returns the native event type string.
func (TurnEndEvent) Kind() string { return EventTypeTurnEnd }

// MessageStartEvent signals a message began.
type MessageStartEvent struct {
	baseEvent

	Message AgentMessage `json:"message"`
}

// Kind returns the native event type string.
func (MessageStartEvent) Kind() string { return EventTypeMessageStart }

// MessageUpdateEvent streams one assistant message delta.
type MessageUpdateEvent struct {
	baseEvent

	Message               json.RawMessage       `json:"message"`
	AssistantMessageEvent AssistantMessageEvent `json:"assistantMessageEvent"`
}

// Kind returns the native event type string.
func (MessageUpdateEvent) Kind() string { return EventTypeMessageUpdate }

// MessageEndEvent signals a message completed. For assistant messages the
// message carries usage, stopReason, and errorMessage.
type MessageEndEvent struct {
	baseEvent

	Message AgentMessage `json:"message"`
}

// Kind returns the native event type string.
func (MessageEndEvent) Kind() string { return EventTypeMessageEnd }

// ToolExecutionStartEvent signals a tool began executing.
type ToolExecutionStartEvent struct {
	baseEvent

	ToolCallID string          `json:"toolCallId"`
	ToolName   string          `json:"toolName"`
	Args       json.RawMessage `json:"args,omitempty"`
}

// Kind returns the native event type string.
func (ToolExecutionStartEvent) Kind() string { return EventTypeToolExecutionStart }

// ToolExecutionUpdateEvent streams accumulated partial tool output.
type ToolExecutionUpdateEvent struct {
	baseEvent

	ToolCallID    string          `json:"toolCallId"`
	ToolName      string          `json:"toolName"`
	Args          json.RawMessage `json:"args,omitempty"`
	PartialResult *ToolResult     `json:"partialResult,omitempty"`
}

// Kind returns the native event type string.
func (ToolExecutionUpdateEvent) Kind() string { return EventTypeToolExecutionUpdate }

// ToolExecutionEndEvent signals a tool completed.
type ToolExecutionEndEvent struct {
	baseEvent

	ToolCallID string      `json:"toolCallId"`
	ToolName   string      `json:"toolName"`
	Result     *ToolResult `json:"result,omitempty"`
	IsError    bool        `json:"isError"`
}

// Kind returns the native event type string.
func (ToolExecutionEndEvent) Kind() string { return EventTypeToolExecutionEnd }

// QueueUpdateEvent signals the pending steering/follow-up queue changed.
type QueueUpdateEvent struct {
	baseEvent

	Steering []string `json:"steering"`
	FollowUp []string `json:"followUp"`
}

// Kind returns the native event type string.
func (QueueUpdateEvent) Kind() string { return EventTypeQueueUpdate }

// CompactionStartEvent signals compaction began.
type CompactionStartEvent struct {
	baseEvent

	Reason string `json:"reason"`
}

// Kind returns the native event type string.
func (CompactionStartEvent) Kind() string { return EventTypeCompactionStart }

// CompactionEndEvent signals compaction completed, aborted, or failed.
type CompactionEndEvent struct {
	baseEvent

	Reason       string          `json:"reason"`
	Result       json.RawMessage `json:"result,omitempty"`
	Aborted      bool            `json:"aborted"`
	WillRetry    bool            `json:"willRetry"`
	ErrorMessage string          `json:"errorMessage,omitempty"`
}

// Kind returns the native event type string.
func (CompactionEndEvent) Kind() string { return EventTypeCompactionEnd }

// AutoRetryStartEvent signals an automatic retry began after a transient
// error. The adapter disables auto-retry at session start, so this appears
// only if an operator re-enables it natively.
type AutoRetryStartEvent struct {
	baseEvent

	Attempt      int     `json:"attempt"`
	MaxAttempts  int     `json:"maxAttempts"`
	DelayMs      float64 `json:"delayMs"`
	ErrorMessage string  `json:"errorMessage,omitempty"`
}

// Kind returns the native event type string.
func (AutoRetryStartEvent) Kind() string { return EventTypeAutoRetryStart }

// AutoRetryEndEvent signals an automatic retry finished.
type AutoRetryEndEvent struct {
	baseEvent

	Success    bool   `json:"success"`
	Attempt    int    `json:"attempt"`
	FinalError string `json:"finalError,omitempty"`
}

// Kind returns the native event type string.
func (AutoRetryEndEvent) Kind() string { return EventTypeAutoRetryEnd }

// ExtensionErrorEvent signals an extension threw an error.
type ExtensionErrorEvent struct {
	baseEvent

	ExtensionPath string `json:"extensionPath"`
	Event         string `json:"event"`
	Error         string `json:"error"`
}

// Kind returns the native event type string.
func (ExtensionErrorEvent) Kind() string { return EventTypeExtensionError }

// UnknownEvent carries an event type this package does not model. It keeps
// the raw payload available for raw-event forwarding.
type UnknownEvent struct {
	baseEvent

	EventType string
}

// Kind returns the native event type string.
func (e UnknownEvent) Kind() string { return e.EventType }

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
		typed := AgentEndEvent{baseEvent: base}
		err = json.Unmarshal(line, &typed)
		event = typed
	case EventTypeAgentSettled:
		event = AgentSettledEvent{baseEvent: base}
	case EventTypeTurnStart:
		event = TurnStartEvent{baseEvent: base}
	case EventTypeTurnEnd:
		typed := TurnEndEvent{baseEvent: base}
		err = json.Unmarshal(line, &typed)
		event = typed
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
	case EventTypeQueueUpdate:
		typed := QueueUpdateEvent{baseEvent: base}
		err = json.Unmarshal(line, &typed)
		event = typed
	case EventTypeCompactionStart:
		typed := CompactionStartEvent{baseEvent: base}
		err = json.Unmarshal(line, &typed)
		event = typed
	case EventTypeCompactionEnd:
		typed := CompactionEndEvent{baseEvent: base}
		err = json.Unmarshal(line, &typed)
		event = typed
	case EventTypeAutoRetryStart:
		typed := AutoRetryStartEvent{baseEvent: base}
		err = json.Unmarshal(line, &typed)
		event = typed
	case EventTypeAutoRetryEnd:
		typed := AutoRetryEndEvent{baseEvent: base}
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
