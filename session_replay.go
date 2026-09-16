package piacp

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/coder/acp-go-sdk"

	"github.com/savid/acp-go-core/image"
	"github.com/savid/acp-go-pi/internal/pi"
)

// messageRow decodes one native row's message and its content blocks. ok is
// false for a row that carries no message.
func messageRow(row []byte) (pi.AgentMessage, []pi.ContentBlock, bool, error) {
	var entry struct {
		Type    string          `json:"type"`
		Message json.RawMessage `json:"message"`
	}

	if err := json.Unmarshal(row, &entry); err != nil {
		return pi.AgentMessage{}, nil, false, err
	}

	if entry.Type != rowTypeMessage {
		return pi.AgentMessage{}, nil, false, nil
	}

	var message pi.AgentMessage
	if err := json.Unmarshal(entry.Message, &message); err != nil {
		return pi.AgentMessage{}, nil, false, err
	}

	blocks, err := message.ContentBlocks()
	if err != nil {
		return pi.AgentMessage{}, nil, false, fmt.Errorf("decode stored message content: %w", err)
	}

	return message, blocks, true, nil
}

// validateRows refuses a native log the adapter cannot decode. Restore runs it
// before any row reaches pi's own session file, so the file an operator
// continues outside ACP never gains a row this adapter could not read back.
func validateRows(rows [][]byte) error {
	for index, row := range rows {
		if _, _, _, err := messageRow(row); err != nil {
			return fmt.Errorf("invalid native row %d: %w", index, err)
		}
	}

	return nil
}

// replay delivers mirrored native rows as session updates in append order.
// Replay runs the same image validation as live emission; a stored artifact
// the adapter can no longer reproduce fails the whole load.
func (s *session) replay(ctx context.Context, rows [][]byte) error {
	limits := s.agent.options.ImageLimits.core()

	for _, row := range rows {
		message, blocks, ok, err := messageRow(row)
		if err != nil {
			return s.agent.restoreRefused(ctx, s.id, err)
		}

		if !ok {
			continue
		}

		updates, failure := replayUpdates(message, blocks, limits)
		if failure != nil {
			return s.agent.restoreRefused(ctx, s.id, failure)
		}

		if err := s.emit(ctx, updates...); err != nil {
			return err
		}
	}

	return nil
}

func replayUpdates(message pi.AgentMessage, blocks []pi.ContentBlock, limits image.Limits) ([]acp.SessionUpdate, *image.OutputError) {
	switch message.Role {
	case messageRoleUser:
		updates := make([]acp.SessionUpdate, 0, len(blocks))

		for index := range blocks {
			block := &blocks[index]

			switch {
			case block.Type == contentBlockTypeText && block.Text != "":
				updates = append(updates, acp.UpdateUserMessageText(block.Text))
			case block.Type == contentBlockTypeImage && block.Data != "":
				output, failure := image.DecodeOutput(block.Data, block.MimeType, limits.EffectiveOutputPerImage())
				if failure != nil {
					return nil, failure
				}

				updates = append(updates, acp.UpdateUserMessage(acp.ImageBlock(output.Data, output.MIME)))
			}
		}

		return updates, nil
	case messageRoleAssistant:
		return assistantReplayUpdates(message, blocks, limits)
	case messageRoleToolResult:
		if message.ToolCallID == "" {
			return nil, nil
		}

		status := acp.ToolCallStatusCompleted
		if message.IsError {
			status = acp.ToolCallStatusFailed
		}

		items, failure := mapToolContent(nil, blocks, limits)
		if failure != nil {
			return nil, failure
		}

		opts := []acp.ToolCallUpdateOpt{acp.WithUpdateStatus(status)}
		if len(items) > 0 {
			opts = append(opts, acp.WithUpdateContent(toolContent(items)))
		}

		return []acp.SessionUpdate{acp.UpdateToolCall(acp.ToolCallId(message.ToolCallID), opts...)}, nil
	default:
		return nil, nil
	}
}

func assistantReplayUpdates(message pi.AgentMessage, blocks []pi.ContentBlock, limits image.Limits) ([]acp.SessionUpdate, *image.OutputError) {
	updates := make([]acp.SessionUpdate, 0, len(blocks))
	messageID := optionalString(message.ACPMessageID)

	for index := range blocks {
		block := &blocks[index]

		switch block.Type {
		case contentBlockTypeText:
			if block.Text != "" {
				updates = append(updates, acp.SessionUpdate{AgentMessageChunk: &acp.SessionUpdateAgentMessageChunk{
					Content: acp.TextBlock(block.Text), MessageId: messageID,
				}})
			}
		case contentBlockTypeThinking:
			if block.Thinking != "" {
				updates = append(updates, acp.SessionUpdate{AgentThoughtChunk: &acp.SessionUpdateAgentThoughtChunk{
					Content: acp.TextBlock(block.Thinking), MessageId: messageID,
				}})
			}
		case contentBlockTypeImage:
			output, failure := image.DecodeOutput(block.Data, block.MimeType, limits.EffectiveOutputPerImage())
			if failure != nil {
				return nil, failure
			}

			updates = append(updates, acp.SessionUpdate{AgentMessageChunk: &acp.SessionUpdateAgentMessageChunk{
				Content: acp.ImageBlock(output.Data, output.MIME), MessageId: messageID,
			}})
		case contentBlockTypeToolCall:
			opts := []acp.ToolCallStartOpt{
				acp.WithStartKind(toolKindForName(block.Name)),
				acp.WithStartStatus(acp.ToolCallStatusCompleted),
			}
			if len(block.Arguments) > 0 {
				opts = append(opts, acp.WithStartRawInput(block.Arguments))
			}

			updates = append(updates, acp.StartToolCall(acp.ToolCallId(block.ID), block.Name, opts...))
		}
	}

	return updates, nil
}
