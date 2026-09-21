package piacp

import (
	"context"

	"github.com/coder/acp-go-sdk"

	"github.com/savid/acp-go-core/image"
	"github.com/savid/acp-go-core/wire"
	"github.com/savid/acp-go-pi/internal/pi"
)

// toolState is the exact-id lifecycle published for one native tool call.
type toolState struct {
	published bool
	terminal  bool
	// content is the last emitted complete content array; each later
	// content-bearing update merges onto it so no delivered item disappears
	// under ACP's whole-array replacement.
	content []toolContentItem
}

type toolContentItem struct {
	content    acp.ToolCallContent
	key        string
	imageBytes int64
}

func (state *cycleState) tool(id string) *toolState {
	if state.tools == nil {
		state.tools = make(map[string]*toolState)
	}

	tool := state.tools[id]
	if tool == nil {
		tool = &toolState{}
		state.tools[id] = tool
	}

	return tool
}

// publishPendingTool announces a tool call that is awaiting permission before
// pi reports its execution.
func (s *session) publishPendingTool(ctx context.Context, state *cycleState, prompt pi.PermissionPrompt) error {
	tool := state.tool(prompt.ToolCallID)
	if tool.published {
		return nil
	}

	opts := []acp.ToolCallStartOpt{
		acp.WithStartKind(toolKindForName(prompt.ToolName)),
		acp.WithStartStatus(acp.ToolCallStatusPending),
	}
	if len(prompt.Input) > 0 {
		opts = append(opts, acp.WithStartRawInput(prompt.Input))
	}

	if err := s.emit(ctx, acp.StartToolCall(acp.ToolCallId(prompt.ToolCallID), prompt.ToolName, opts...)); err != nil {
		return err
	}

	tool.published = true

	return nil
}

func (s *session) publishToolStart(ctx context.Context, state *cycleState, event pi.ToolExecutionStartEvent) error {
	tool := state.tool(event.ToolCallID)
	if tool.terminal {
		return nil
	}

	kind := toolKindForName(event.ToolName)

	if tool.published {
		opts := []acp.ToolCallUpdateOpt{
			acp.WithUpdateTitle(event.ToolName),
			acp.WithUpdateKind(kind),
			acp.WithUpdateStatus(acp.ToolCallStatusInProgress),
		}
		if len(event.Args) > 0 {
			opts = append(opts, acp.WithUpdateRawInput(event.Args))
		}

		return s.emit(ctx, acp.UpdateToolCall(acp.ToolCallId(event.ToolCallID), opts...))
	}

	opts := []acp.ToolCallStartOpt{acp.WithStartKind(kind), acp.WithStartStatus(acp.ToolCallStatusInProgress)}
	if len(event.Args) > 0 {
		opts = append(opts, acp.WithStartRawInput(event.Args))
	}

	if err := s.emit(ctx, acp.StartToolCall(acp.ToolCallId(event.ToolCallID), event.ToolName, opts...)); err != nil {
		return err
	}

	tool.published = true

	return nil
}

// publishToolUpdate emits one partial result as a complete content snapshot.
// An update that adds nothing is skipped rather than retransmitted.
func (s *session) publishToolUpdate(ctx context.Context, state *cycleState, toolCallID string, blocks []pi.ContentBlock) error {
	tool := state.tool(toolCallID)
	if tool.terminal {
		return nil
	}

	snapshot, failure := mapToolContent(tool.content, blocks, s.agent.options.ImageLimits.core())
	if failure != nil {
		return s.failToolImage(ctx, state, toolCallID, failure)
	}

	if len(snapshot) == 0 || len(snapshot) == len(tool.content) {
		return nil
	}

	if err := s.emit(ctx, acp.UpdateToolCall(acp.ToolCallId(toolCallID), acp.WithUpdateContent(toolContent(snapshot)))); err != nil {
		return err
	}

	s.recordToolContent(state, tool, snapshot)

	return nil
}

// publishToolTerminal emits the terminal status and, when the result carries
// mappable content, the complete final snapshot.
func (s *session) publishToolTerminal(ctx context.Context, state *cycleState, toolCallID string, status acp.ToolCallStatus, result *pi.ToolResult) error {
	tool := state.tool(toolCallID)
	if tool.terminal {
		return nil
	}

	opts := []acp.ToolCallUpdateOpt{acp.WithUpdateStatus(status)}

	var snapshot []toolContentItem

	if result != nil {
		var failure *image.OutputError

		snapshot, failure = mapToolContent(tool.content, result.Content, s.agent.options.ImageLimits.core())
		if failure != nil {
			return s.failToolImage(ctx, state, toolCallID, failure)
		}

		if len(snapshot) > 0 {
			opts = append(opts, acp.WithUpdateContent(toolContent(snapshot)))
		}
	}

	if err := s.emit(ctx, acp.UpdateToolCall(acp.ToolCallId(toolCallID), opts...)); err != nil {
		return err
	}

	s.recordToolContent(state, tool, snapshot)
	tool.terminal = true

	return nil
}

func (s *session) recordToolContent(state *cycleState, tool *toolState, snapshot []toolContentItem) {
	if len(snapshot) == 0 {
		return
	}

	tool.content = snapshot

	for _, item := range snapshot {
		if item.imageBytes > 0 {
			state.imagesEmitted = true

			return
		}
	}
}

// failToolImage handles an image the adapter will not ship on tool
// provenance. A verdict the model can act on carries its guidance as that
// call's own content and the turn continues; a storage failure ends the turn.
func (s *session) failToolImage(ctx context.Context, state *cycleState, toolCallID string, failure *image.OutputError) error {
	tool := state.tool(toolCallID)
	opts := []acp.ToolCallUpdateOpt{acp.WithUpdateStatus(acp.ToolCallStatusFailed)}

	guidance, recoverable := failure.Guidance()
	if recoverable {
		opts = append(opts, acp.WithUpdateContent([]acp.ToolCallContent{acp.ToolContent(acp.TextBlock(guidance))}))
	}

	err := s.emit(ctx, acp.UpdateToolCall(acp.ToolCallId(toolCallID), opts...))
	tool.terminal = true

	if recoverable {
		return err
	}

	return wire.TurnFailed(vendor, failure.TurnFailure())
}

// mapToolContent builds the next complete content snapshot from the
// previously emitted one: text and validated images, merged so an already
// delivered item never disappears, bounded per tool call.
func mapToolContent(previous []toolContentItem, blocks []pi.ContentBlock, limits image.Limits) ([]toolContentItem, *image.OutputError) {
	next := make([]toolContentItem, 0, len(blocks))

	for index := range blocks {
		block := &blocks[index]

		switch block.Type {
		case contentBlockTypeText:
			if block.Text == "" {
				continue
			}

			next = append(next, toolContentItem{content: acp.ToolContent(acp.TextBlock(block.Text)), key: "text:" + block.Text})
		case contentBlockTypeImage:
			output, failure := image.DecodeOutput(block.Data, block.MimeType, limits.EffectiveOutputPerImage())
			if failure != nil {
				return nil, failure
			}

			next = append(next, toolContentItem{
				content:    acp.ToolContent(acp.ImageBlock(output.Data, output.MIME)),
				key:        "image:" + output.MIME + ":" + output.Fingerprint,
				imageBytes: output.SizeBytes,
			})
		}
	}

	merged := mergeToolContent(previous, next)

	var total int64

	for _, item := range merged {
		total += item.imageBytes
		if item.imageBytes > 0 && total > limits.EffectiveOutputPerToolCall() {
			return nil, &image.OutputError{
				Reason:    image.ReasonTooLarge,
				Message:   "tool call image content exceeds the per-tool-call limit",
				SizeBytes: total,
				MaxBytes:  limits.EffectiveOutputPerToolCall(),
			}
		}
	}

	return merged, nil
}

// mergeToolContent builds the next snapshot: pi's partial results replace
// text, so the latest text stands, while an image already delivered stays in
// the array until the call ends.
func mergeToolContent(previous []toolContentItem, next []toolContentItem) []toolContentItem {
	if len(previous) == 0 {
		return next
	}

	present := make(map[string]struct{}, len(next))
	for _, item := range next {
		present[item.key] = struct{}{}
	}

	merged := make([]toolContentItem, 0, len(previous)+len(next))

	for _, item := range previous {
		if _, ok := present[item.key]; item.imageBytes > 0 && !ok {
			merged = append(merged, item)
		}
	}

	return append(merged, next...)
}

func toolContent(items []toolContentItem) []acp.ToolCallContent {
	content := make([]acp.ToolCallContent, 0, len(items))
	for _, item := range items {
		content = append(content, item.content)
	}

	return content
}
