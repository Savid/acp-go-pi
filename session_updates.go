package piacp

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"log/slog"
	"strings"
	"time"

	"github.com/coder/acp-go-sdk"

	"github.com/savid/acp-go-core/image"
	"github.com/savid/acp-go-core/wire"
	"github.com/savid/acp-go-pi/internal/pi"
)

const (
	messageRoleUser       = "user"
	messageRoleAssistant  = "assistant"
	messageRoleToolResult = "toolResult"
	messageRoleCustom     = "custom"

	rowTypeMessage = "message"

	contentBlockTypeText     = "text"
	contentBlockTypeThinking = "thinking"
	contentBlockTypeToolCall = "toolCall"
	contentBlockTypeImage    = "image"

	assistantEventTextDelta     = "text_delta"
	assistantEventThinkingDelta = "thinking_delta"
)

// cycleState accumulates what one cycle streamed.
type cycleState struct {
	usage         *acp.Usage
	cost          *pi.UsageCost
	stopReason    string
	errorMessage  string
	imagesEmitted bool
	// streamedText and streamedThought hold what the open assistant message
	// already streamed, so its terminal frame contributes only the suffix.
	streamedText    string
	streamedThought string
	finalized       map[string]struct{}
	agentImages     map[string]struct{}
	tools           map[string]*toolState
}

// emit delivers session updates to the host. A session with no attached
// connection delivers nothing. Delivery never rides a request's cancellation.
func (s *session) emit(ctx context.Context, updates ...acp.SessionUpdate) error {
	conn := s.agent.connection()
	if conn == nil {
		return nil
	}

	ctx = context.WithoutCancel(ctx)

	for _, update := range updates {
		if err := conn.SessionUpdate(ctx, acp.SessionNotification{SessionId: s.id, Update: update}); err != nil {
			return err
		}
	}

	return nil
}

// projectEvent maps one native event of a cycle to session updates and
// reports whether the run settled. An update the host refused is returned so
// the cycle can record it, but the native run keeps draining to its settle
// marker.
func (s *session) projectEvent(ctx context.Context, rt *runtime, c *cycle, event pi.Event) (bool, error) {
	state := &c.state

	switch typed := event.(type) {
	case pi.AgentSettledEvent:
		return true, nil
	case pi.MessageStartEvent:
		if typed.Message.Role == messageRoleAssistant {
			state.streamedText = ""
			state.streamedThought = ""
		}

		return false, nil
	case pi.MessageUpdateEvent:
		return false, s.emitAssistantDelta(ctx, typed.AssistantMessageEvent, state)
	case pi.MessageEndEvent:
		if typed.Message.Role != messageRoleAssistant {
			return false, nil
		}

		observeAssistantMessageEnd(typed.Message, state)

		if err := s.emitAssistantTextSuffix(ctx, typed.Message, state); err != nil {
			return false, err
		}

		return false, s.emitAssistantImages(ctx, typed.Message, state)
	case pi.ToolExecutionStartEvent:
		return false, s.publishToolStart(ctx, state, typed)
	case pi.ToolExecutionUpdateEvent:
		if typed.PartialResult == nil {
			return false, nil
		}

		return false, s.publishToolUpdate(ctx, state, typed.ToolCallID, typed.PartialResult.Content)
	case pi.ToolExecutionEndEvent:
		status := acp.ToolCallStatusCompleted
		if typed.IsError {
			status = acp.ToolCallStatusFailed
		}

		return false, s.publishToolTerminal(ctx, state, typed.ToolCallID, status, typed.Result)
	case pi.ExtensionErrorEvent:
		// A failure in the wrapper's own extensions means the permission gate
		// or question tool is no longer running under the admission the host
		// granted, so the cycle fails closed. Any other extension is the
		// operator's, and pi reports it natively.
		if s.agent.extensions.IsWrapperExtension(typed.ExtensionPath) {
			s.agent.log.ErrorContext(ctx, "wrapper extension failed",
				slog.String("session_id", string(s.id)), slog.String("event", typed.Event))
			s.abortAsync(ctx, rt)

			return false, wire.TurnFailed(vendor, wire.TurnFailure{Cause: "extension", Message: extensionFailureMessage})
		}

		return false, nil
	default:
		return false, nil
	}
}

func (s *session) emitAssistantDelta(ctx context.Context, delta pi.AssistantMessageEvent, state *cycleState) error {
	if delta.Delta == "" {
		return nil
	}

	switch delta.Type {
	case assistantEventTextDelta:
		state.streamedText += delta.Delta

		return s.emit(ctx, acp.UpdateAgentMessageText(delta.Delta))
	case assistantEventThinkingDelta:
		state.streamedThought += delta.Delta

		return s.emit(ctx, acp.UpdateAgentThoughtText(delta.Delta))
	default:
		return nil
	}
}

// emitAssistantTextSuffix projects the terminal message frame's text as
// append-only deltas: only what the streamed deltas did not carry, once per
// native message identity.
func (s *session) emitAssistantTextSuffix(ctx context.Context, message pi.AgentMessage, state *cycleState) error {
	if state.finalized == nil {
		state.finalized = make(map[string]struct{})
	}

	if message.ACPMessageID != "" {
		if _, seen := state.finalized[message.ACPMessageID]; seen {
			return nil
		}

		state.finalized[message.ACPMessageID] = struct{}{}
	}

	blocks, _ := message.ContentBlocks()

	var text, thinking strings.Builder

	for index := range blocks {
		switch blocks[index].Type {
		case contentBlockTypeText:
			text.WriteString(blocks[index].Text)
		case contentBlockTypeThinking:
			thinking.WriteString(blocks[index].Thinking)
		}
	}

	messageID := optionalString(message.ACPMessageID)
	updates := make([]acp.SessionUpdate, 0, 2)

	if suffix := wire.UnstreamedSuffix(state.streamedThought, thinking.String()); suffix != "" {
		updates = append(updates, acp.SessionUpdate{AgentThoughtChunk: &acp.SessionUpdateAgentThoughtChunk{
			Content: acp.TextBlock(suffix), MessageId: messageID,
		}})
	}

	if suffix := wire.UnstreamedSuffix(state.streamedText, text.String()); suffix != "" {
		updates = append(updates, acp.SessionUpdate{AgentMessageChunk: &acp.SessionUpdateAgentMessageChunk{
			Content: acp.TextBlock(suffix), MessageId: messageID,
		}})
	}

	state.streamedText = ""
	state.streamedThought = ""

	return s.emit(ctx, updates...)
}

// emitAssistantImages projects image blocks of a finalized assistant message
// as agent chunks, one image per chunk, deduplicated on message identity plus
// fingerprint. A refused image is replaced by its guidance as agent text.
func (s *session) emitAssistantImages(ctx context.Context, message pi.AgentMessage, state *cycleState) error {
	blocks, _ := message.ContentBlocks()
	messageID := optionalString(message.ACPMessageID)

	for index := range blocks {
		if blocks[index].Type != contentBlockTypeImage {
			continue
		}

		block := &blocks[index]

		output, failure := image.DecodeOutput(block.Data, block.MimeType, s.agent.options.ImageLimits.core().EffectiveOutputPerImage())
		if failure != nil {
			guidance, _ := failure.Guidance()

			if err := s.emit(ctx, acp.SessionUpdate{AgentMessageChunk: &acp.SessionUpdateAgentMessageChunk{
				Content: acp.TextBlock(guidance), MessageId: messageID,
			}}); err != nil {
				return err
			}

			continue
		}

		key := message.ACPMessageID + ":" + output.Fingerprint

		if state.agentImages == nil {
			state.agentImages = make(map[string]struct{})
		}

		if _, seen := state.agentImages[key]; seen {
			continue
		}

		if err := s.emit(ctx, acp.SessionUpdate{AgentMessageChunk: &acp.SessionUpdateAgentMessageChunk{
			Content: acp.ImageBlock(output.Data, output.MIME), MessageId: messageID,
		}}); err != nil {
			return err
		}

		state.agentImages[key] = struct{}{}
		state.imagesEmitted = true
	}

	return nil
}

func optionalString(value string) *string {
	if value == "" {
		return nil
	}

	return &value
}

func observeAssistantMessageEnd(message pi.AgentMessage, state *cycleState) {
	if message.StopReason != "" {
		state.stopReason = message.StopReason
	}

	if message.ErrorMessage != "" {
		state.errorMessage = message.ErrorMessage
	}

	if message.Usage != nil {
		state.usage = mergeUsage(state.usage, message.Usage)

		if message.Usage.Cost != nil {
			state.cost = message.Usage.Cost
		}
	}
}

func mergeUsage(total *acp.Usage, next *pi.Usage) *acp.Usage {
	if total == nil {
		total = &acp.Usage{CachedReadTokens: new(0), CachedWriteTokens: new(0)}
	}

	total.InputTokens += int(next.Input)
	total.OutputTokens += int(next.Output)
	*total.CachedReadTokens += int(next.CacheRead)
	*total.CachedWriteTokens += int(next.CacheWrite)
	total.TotalTokens = total.InputTokens + total.OutputTokens + *total.CachedReadTokens + *total.CachedWriteTokens

	return total
}

// emitUsage reports harness-reported usage. size is the model's context
// window from get_session_stats, else the selected model's catalog value,
// else 0.
func (s *session) emitUsage(ctx context.Context, state *cycleState, stats *pi.SessionStats) {
	used := 0
	if state.usage != nil {
		used = state.usage.TotalTokens
	}

	var size int64

	if stats != nil && stats.ContextUsage != nil {
		if stats.ContextUsage.Tokens != nil {
			used = int(*stats.ContextUsage.Tokens)
		}

		size = stats.ContextUsage.ContextWindow
	}

	if size == 0 {
		s.mu.Lock()
		size = s.contextWindow
		s.mu.Unlock()
	}

	if state.usage == nil && used == 0 && size == 0 {
		return
	}

	update := &acp.SessionUsageUpdate{Size: int(size), Used: used}
	if state.cost != nil {
		update.Cost = &acp.Cost{Amount: state.cost.Total, Currency: "USD"}
	}

	_ = s.emit(ctx, acp.SessionUpdate{UsageUpdate: update})
}

// emitRestoredUsage reports the restored session's context usage when pi
// knows it.
func (s *session) emitRestoredUsage(ctx context.Context, rt *runtime) {
	stats, err := rt.client.GetSessionStats(ctx)
	if err != nil || stats.ContextUsage == nil || stats.ContextUsage.ContextWindow <= 0 {
		return
	}

	used := 0
	if stats.ContextUsage.Tokens != nil {
		used = int(*stats.ContextUsage.Tokens)
	}

	s.mu.Lock()
	s.contextWindow = stats.ContextUsage.ContextWindow
	s.mu.Unlock()

	_ = s.emit(ctx, acp.SessionUpdate{UsageUpdate: &acp.SessionUsageUpdate{Size: int(stats.ContextUsage.ContextWindow), Used: used}})
}

// emitSessionInfo records the turn's time and, on the first prompt, a title.
func (s *session) emitSessionInfo(ctx context.Context, prompt []acp.ContentBlock) {
	updatedAt := time.Now().UTC().Format(time.RFC3339)
	update := acp.SessionSessionInfoUpdate{UpdatedAt: &updatedAt}

	s.mu.Lock()
	s.updatedAt = updatedAt

	if s.title == "" {
		if title := wire.PromptTitle(prompt); title != "" {
			s.title = title
			update.Title = &title
		}
	}
	s.mu.Unlock()

	_ = s.emit(ctx, acp.SessionUpdate{SessionInfoUpdate: &update})
}

func (s *session) sessionInfo() acp.SessionInfo {
	s.mu.Lock()
	title := s.title
	updatedAt := s.updatedAt
	s.mu.Unlock()

	if title == "" {
		title = string(s.id)
	}

	info := acp.SessionInfo{
		Meta:                  wire.NativeSessionMeta(vendor, s.nativeID),
		SessionId:             s.id,
		Title:                 &title,
		Cwd:                   s.cwd,
		AdditionalDirectories: append([]string(nil), s.additionalDirectories...),
	}
	if updatedAt != "" {
		info.UpdatedAt = &updatedAt
	}

	return info
}

// publishCommands emits the session's command catalog as a full replacement,
// including the explicit empty one.
func (s *session) publishCommands(ctx context.Context) error {
	s.mu.Lock()
	commands := append([]acp.AvailableCommand{}, s.commands...)
	s.mu.Unlock()

	return s.emit(ctx, acp.SessionUpdate{AvailableCommandsUpdate: &acp.SessionAvailableCommandsUpdate{AvailableCommands: commands}})
}

func (s *session) clearCommands(ctx context.Context) {
	s.mu.Lock()
	s.commands = nil
	s.mu.Unlock()

	_ = s.publishCommands(ctx)
}

// availableCommands converts pi's command catalog, dropping names the shared
// sanitizer rejects.
func availableCommands(commands []pi.SlashCommand) []acp.AvailableCommand {
	available := make([]acp.AvailableCommand, 0, len(commands))

	for _, command := range commands {
		if !wire.ValidCommandName(command.Name) {
			continue
		}

		available = append(available, acp.AvailableCommand{Name: command.Name, Description: command.Description})
	}

	return available
}

// emitRawEvent forwards one native record on the raw-event channel when the
// session opted in. An image payload is replaced by its decoded size so
// diagnostics never carry a second copy of the bytes.
func (s *session) emitRawEvent(ctx context.Context, event pi.Event) {
	if !s.rawEvents.Enabled() {
		return
	}

	conn := s.agent.connection()
	if conn == nil {
		return
	}

	var payload map[string]any
	if err := json.Unmarshal(event.RawJSON(), &payload); err != nil {
		return
	}

	redactImages(payload)

	notify := func(ctx context.Context, method string, params map[string]any) error {
		return conn.NotifyExtension(ctx, method, params)
	}

	if err := s.rawEvents.Emit(ctx, notify, payload); err != nil {
		s.agent.observe.RecordRawMessageEmitFailure(ctx, err)
	}
}

func redactImages(value any) {
	switch typed := value.(type) {
	case map[string]any:
		blockType, _ := typed["type"].(string)
		if data, ok := typed["data"].(string); ok && blockType == contentBlockTypeImage && data != "" {
			typed["data"] = ""
			typed["sizeBytes"] = base64.RawStdEncoding.DecodedLen(len(strings.TrimRight(data, "=")))
		}

		for _, item := range typed {
			redactImages(item)
		}
	case []any:
		for _, item := range typed {
			redactImages(item)
		}
	}
}
