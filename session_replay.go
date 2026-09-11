package piacp

import (
	"context"
	"encoding/json"

	"github.com/coder/acp-go-sdk"

	"github.com/savid/acp-go-core/image"
	"github.com/savid/acp-go-pi/internal/pi"
)

// replay delivers mirrored native rows as session updates in append order.
// Replay runs the same image validation as live emission; a stored artifact
// the adapter can no longer reproduce fails the whole load.
func (s *session) replay(ctx context.Context, rows [][]byte) error {
	limits := s.agent.options.ImageLimits.core()

	for _, row := range rows {
		var entry struct {
			Type    string          `json:"type"`
			Message json.RawMessage `json:"message"`
		}

		if json.Unmarshal(row, &entry) != nil || entry.Type != rowTypeMessage {
			continue
		}

		var message pi.AgentMessage
		if json.Unmarshal(entry.Message, &message) != nil {
			continue
		}

		updates, failure := replayUpdates(message, limits)
		if failure != nil {
			return s.agent.restoreRefused(ctx, s.id, failure)
		}

		if err := s.emit(ctx, updates...); err != nil {
			return err
		}
	}

	return nil
}

func replayUpdates(message pi.AgentMessage, limits image.Limits) ([]acp.SessionUpdate, *image.OutputError) {
	blocks, err := message.ContentBlocks()
	if err != nil {
		return nil, nil //nolint:nilerr // a row whose content does not decode replays nothing
	}

	switch message.Role {
	case messageRoleUser:
		updates := make([]acp.SessionUpdate, 0, len(blocks))

		for index := range blocks {
			block := &blocks[index]

			switch {
			case block.Type == contentBlockTypeText && block.Text != "":
				updates = append(updates, acp.UpdateUserMessageText(block.Text))
			case block.Type == contentBlockTypeImage && block.Data != "":
				updates = append(updates, acp.UpdateUserMessage(acp.ImageBlock(block.Data, block.MimeType)))
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
			output, failure := decodeOutputImage(*block, limits.EffectiveOutputPerImage())
			if failure != nil {
				return nil, failure
			}

			updates = append(updates, acp.SessionUpdate{AgentMessageChunk: &acp.SessionUpdateAgentMessageChunk{
				Content: acp.ImageBlock(output.data, output.mime), MessageId: messageID,
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
