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

	// costCurrency is the currency of every cost pi reports.
	costCurrency = "USD"
)

// cycleState accumulates what one cycle streamed.
type cycleState struct {
	// usage sums every response's reported tokens; context is what the last
	// usable response left occupied, 0 when none has since the cycle opened
	// or pi last compacted.
	usage         *acp.Usage
	context       int
	stopReason    string
	errorMessage  string
	imagesEmitted bool
	// inputPending holds from an assistant message's start until its first
	// delta, the earliest point pi can report the input the response was
	// sent with.
	inputPending bool
	// responseID is the id the model gateway returned for the open assistant
	// message, once pi holds it; its streamed chunks carry it as messageId.
	responseID string
	// streamedText and streamedThought hold what the open assistant message
	// already streamed, so its terminal frame contributes only the suffix.
	streamedText    string
	streamedThought string
	// tools holds each tool call until pi ends its execution.
	tools map[string]*toolState
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
	if handled, err := s.projectCompaction(ctx, rt, &c.state, event); handled {
		return false, err
	}

	if s.cycleCancelled(c) {
		_, settled := event.(pi.AgentSettledEvent)

		return settled, nil
	}

	state := &c.state

	switch typed := event.(type) {
	case pi.AgentSettledEvent:
		return true, nil
	case pi.MessageStartEvent:
		if typed.Message.Role == messageRoleAssistant {
			state.responseID = typed.Message.ResponseID
			state.streamedText = ""
			state.streamedThought = ""
			state.inputPending = true
		}

		return false, nil
	case pi.ResponseIDEvent:
		state.responseID = typed.ResponseID

		return false, nil
	case pi.MessageUpdateEvent:
		if state.inputPending {
			state.inputPending = false

			if err := s.emitResponseInput(ctx, typed.Usage); err != nil {
				return false, err
			}
		}

		return false, s.emitAssistantDelta(ctx, typed.AssistantMessageEvent, state)
	case pi.MessageEndEvent:
		if typed.Message.Role != messageRoleAssistant {
			return false, nil
		}

		state.responseID = ""
		state.inputPending = false

		observeAssistantMessageEnd(typed.Message, state)

		if err := s.emitAssistantTextSuffix(ctx, typed.Message, state); err != nil {
			return false, err
		}

		if err := s.emitAssistantImages(ctx, typed.Message, state); err != nil {
			return false, err
		}

		return false, s.emitResponseUsage(ctx, typed.Message, state)
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

		// pi says nothing more about a tool call once its execution ends.
		defer delete(state.tools, typed.ToolCallID)

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

			return false, wire.TurnFailed(vendor, wire.TurnFailure{Cause: "extension", Message: typed.Error})
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

	messageID := optionalString(state.responseID)

	switch delta.Type {
	case assistantEventTextDelta:
		state.streamedText += delta.Delta

		return s.emit(ctx, acp.SessionUpdate{AgentMessageChunk: &acp.SessionUpdateAgentMessageChunk{
			Content: acp.TextBlock(delta.Delta), MessageId: messageID,
		}})
	case assistantEventThinkingDelta:
		state.streamedThought += delta.Delta

		return s.emit(ctx, acp.SessionUpdate{AgentThoughtChunk: &acp.SessionUpdateAgentThoughtChunk{
			Content: acp.TextBlock(delta.Delta), MessageId: messageID,
		}})
	default:
		return nil
	}
}

// emitAssistantTextSuffix projects the terminal message frame's text as
// append-only deltas: only what the streamed deltas did not carry.
func (s *session) emitAssistantTextSuffix(ctx context.Context, message pi.AgentMessage, state *cycleState) error {
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

	messageID := optionalString(message.ResponseID)
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
// as agent chunks, one image per chunk. A refused image is replaced by its
// guidance as agent text.
func (s *session) emitAssistantImages(ctx context.Context, message pi.AgentMessage, state *cycleState) error {
	blocks, _ := message.ContentBlocks()
	messageID := optionalString(message.ResponseID)

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

		if err := s.emit(ctx, acp.SessionUpdate{AgentMessageChunk: &acp.SessionUpdateAgentMessageChunk{
			Content: acp.ImageBlock(output.Data, output.MIME), MessageId: messageID,
		}}); err != nil {
			return err
		}

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

	if message.Usage != nil && callUsage(message.Usage).Known() {
		state.usage = mergeUsage(state.usage, message.Usage)
	}
}

// callUsage is the token breakdown pi reported for one response. pi reports
// input without the cache tokens it read or wrote and output with any
// reasoning included, and records a figure its provider omitted as 0.
func callUsage(usage *pi.Usage) wire.CallUsage {
	return wire.CallUsage{
		InputTokens:       new(int(usage.Input)),
		CachedReadTokens:  new(int(usage.CacheRead)),
		CachedWriteTokens: new(int(usage.CacheWrite)),
		OutputTokens:      new(int(usage.Output)),
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

// contextTokens is the context an assistant response leaves occupied, counted
// as pi counts it: the reported total, else the sum of input, output, and
// cache tokens. Aborted and failed responses carry no usable figure, and a
// response reporting no token at all, as a gateway replaying a cached
// response does, states nothing.
func contextTokens(message pi.AgentMessage) (int, bool) {
	usage := message.Usage
	if usage == nil || message.StopReason == stopReasonAborted || message.StopReason == stopReasonError || !callUsage(usage).Known() {
		return 0, false
	}

	tokens := usage.TotalTokens
	if tokens == 0 {
		tokens = usage.Input + usage.Output + usage.CacheRead + usage.CacheWrite
	}

	return int(tokens), tokens > 0
}

// emitResponseInput reports the context an assistant response was sent with,
// its input and cache tokens, when pi holds them before the response streams
// its output. A provider that reports usage only when the stream ends leaves
// them zero here, which states nothing. size is the session's known context
// window.
func (s *session) emitResponseInput(ctx context.Context, usage *pi.Usage) error {
	if usage == nil {
		return nil
	}

	used := int(usage.Input + usage.CacheRead + usage.CacheWrite)
	if used <= 0 {
		return nil
	}

	return s.emit(ctx, acp.SessionUpdate{UsageUpdate: &acp.SessionUsageUpdate{Size: s.knownContextWindow(), Used: used}})
}

// emitResponseUsage reports the context a finished assistant response leaves
// occupied, with the response's token breakdown and the id the model gateway
// returned for it. A response with no usable figure leaves the cycle's last
// figure in place. size is the session's known context window.
func (s *session) emitResponseUsage(ctx context.Context, message pi.AgentMessage, state *cycleState) error {
	used, ok := contextTokens(message)
	if !ok {
		return nil
	}

	state.context = used

	breakdown := callUsage(message.Usage)
	breakdown.ResponseID = message.ResponseID

	return s.emit(ctx, acp.SessionUpdate{UsageUpdate: &acp.SessionUsageUpdate{
		Size: s.knownContextWindow(), Used: used, Meta: breakdown.Apply(nil),
	}})
}

// emitSettledUsage reports pi's statistics once a cycle settles. used is pi's
// context estimate, else the context the cycle's last response left; an
// estimate of zero tokens is unknown, since no model call has an empty
// context. After a compaction no response has followed, pi has no estimate
// and nothing is sent. size is the context window get_session_stats reports,
// which becomes the session's known window; cost is the session's cumulative
// cost.
func (s *session) emitSettledUsage(ctx context.Context, state *cycleState, stats *pi.SessionStats) {
	if stats == nil {
		return
	}

	used, known := state.context, state.context > 0

	var size int64

	if usage := stats.ContextUsage; usage != nil {
		size = usage.ContextWindow

		if usage.Tokens != nil && *usage.Tokens > 0 {
			used, known = int(*usage.Tokens), true
		}
	}

	s.mu.Lock()
	s.contextWindow = size
	s.mu.Unlock()

	if !known {
		return
	}

	_ = s.emit(ctx, acp.SessionUpdate{UsageUpdate: &acp.SessionUsageUpdate{
		Size: int(size), Used: used, Cost: &acp.Cost{Amount: stats.Cost, Currency: costCurrency},
	}})
}

// knownContextWindow is the selected model's context window as pi last
// reported it, else 0.
func (s *session) knownContextWindow() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return int(s.contextWindow)
}

// emitRestoredUsage records the restored session's context window and
// reports its context when pi estimates a non-zero one.
func (s *session) emitRestoredUsage(ctx context.Context, rt *runtime) {
	stats, err := rt.client.GetSessionStats(ctx)
	if err != nil || stats.ContextUsage == nil || stats.ContextUsage.ContextWindow <= 0 {
		return
	}

	s.mu.Lock()
	s.contextWindow = stats.ContextUsage.ContextWindow
	s.mu.Unlock()

	tokens := stats.ContextUsage.Tokens
	if tokens == nil || *tokens <= 0 {
		return
	}

	_ = s.emit(ctx, acp.SessionUpdate{UsageUpdate: &acp.SessionUsageUpdate{Size: int(stats.ContextUsage.ContextWindow), Used: int(*tokens)}})
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
	s.openMu.Lock()
	defer s.openMu.Unlock()

	s.mu.Lock()
	s.commands = nil
	closing := s.closing
	s.mu.Unlock()

	if !closing {
		_ = s.publishCommands(ctx)
	}
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
// session opted in. The bridge extension's response-id relay is a UI request,
// which the channel does not carry. An image payload is replaced by its
// decoded size so diagnostics never carry a second copy of the bytes.
func (s *session) emitRawEvent(ctx context.Context, event pi.Event) {
	if _, relay := event.(pi.ResponseIDEvent); relay {
		return
	}

	s.mu.Lock()
	rawEvents := s.rawEvents
	s.mu.Unlock()

	if !rawEvents.Enabled() {
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

	if err := rawEvents.Emit(ctx, notify, payload); err != nil {
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
