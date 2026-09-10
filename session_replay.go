package piacp

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/coder/acp-go-sdk"

	"github.com/savid/acp-go-pi/internal/pi"
)

const (
	storeRowTypeSession     = "session"
	storeRowTypeMessage     = "message"
	storeRowTypeSessionInfo = "session_info"

	messageRoleUser       = "user"
	messageRoleAssistant  = "assistant"
	messageRoleToolResult = "toolResult"
	messageRoleCustom     = "custom"

	contentBlockTypeText     = "text"
	contentBlockTypeThinking = "thinking"
	contentBlockTypeToolCall = "toolCall"
	contentBlockTypeImage    = "image"
)

type storeRow struct {
	Type    string          `json:"type"`
	ID      string          `json:"id,omitempty"`
	Cwd     string          `json:"cwd,omitempty"`
	Name    string          `json:"name,omitempty"`
	Message json.RawMessage `json:"message,omitempty"`
}

// replayStoredSession replays mirrored native rows as ACP session updates in
// append order for session/load. Replay runs the same image validation as
// live emission: a stored artifact the store can no longer reproduce fails
// the whole load rather than streaming a transcript with a hole in it.
func (s *agentSession) replayStoredSession(ctx context.Context, entries []SessionStoreEntry) error {
	for _, entry := range entries {
		row, ok := decodeStoreRow(entry)
		if !ok || row.Type != storeRowTypeMessage {
			continue
		}

		var message pi.AgentMessage
		if err := json.Unmarshal(row.Message, &message); err != nil {
			continue
		}

		updates, failure := messageReplayUpdates(message, s.agent.imageLimits())
		if failure != nil {
			// Replay is a restore, not a turn: a stored artifact this adapter
			// can no longer reproduce is an entry it could not restore, and the
			// verdict a host reads says exactly that.
			return s.agent.restoreRefused(ctx, string(s.id), "replay stored image artifact failed", nil)
		}

		if message.Role == messageRoleAssistant && len(updates) == 0 && message.ACPMessageID != "" {
			updates = []acp.SessionUpdate{{SessionInfoUpdate: &acp.SessionSessionInfoUpdate{}}}
		}

		if err := s.emitUpdatesWithNativeMessageID(ctx, updates, message.ACPMessageID); err != nil {
			return err
		}
	}

	return nil
}

func replayUpdates(entries []SessionStoreEntry, limits ImageLimits) ([]acp.SessionUpdate, error) {
	updates := make([]acp.SessionUpdate, 0, len(entries))

	for _, entry := range entries {
		row, ok := decodeStoreRow(entry)
		if !ok || row.Type != storeRowTypeMessage {
			continue
		}

		var message pi.AgentMessage
		if err := json.Unmarshal(row.Message, &message); err != nil {
			continue
		}

		rowUpdates, failure := messageReplayUpdates(message, limits)
		if failure != nil {
			return nil, imageOutputTurnFailure(failure)
		}

		updates = append(updates, rowUpdates...)
	}

	return updates, nil
}

// terminalAssistantMessageID returns the durable identity on the final
// assistant row. Resume does not replay transcript content, but persistent
// hosts still need this identity-only checkpoint to reconcile the native
// transcript before publishing the restored route. A final assistant row
// without an identity returns empty rather than falling back to an older turn.
func terminalAssistantMessageID(entries []SessionStoreEntry) string {
	for index := len(entries) - 1; index >= 0; index-- {
		row, ok := decodeStoreRow(entries[index])
		if !ok || row.Type != storeRowTypeMessage {
			continue
		}

		var message pi.AgentMessage
		if json.Unmarshal(row.Message, &message) != nil || message.Role != messageRoleAssistant {
			continue
		}

		return message.ACPMessageID
	}

	return ""
}

func messageReplayUpdates(message pi.AgentMessage, limits ImageLimits) ([]acp.SessionUpdate, *imageOutputError) {
	blocks, err := message.ContentBlocks()
	if err != nil {
		return nil, nil //nolint:nilerr // a row whose content does not decode replays nothing, like other malformed stored rows
	}

	switch message.Role {
	case messageRoleUser:
		updates := make([]acp.SessionUpdate, 0, len(blocks))

		for index := range blocks {
			block := &blocks[index]

			switch block.Type {
			case contentBlockTypeText:
				if block.Text != "" {
					updates = append(updates, acp.UpdateUserMessageText(block.Text))
				}
			case contentBlockTypeImage:
				if block.Data != "" {
					updates = append(updates, acp.UpdateUserMessage(acp.ImageBlock(block.Data, block.MimeType)))
				}
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

		content, failure := replayToolContent(blocks, limits)
		if failure != nil {
			return nil, failure
		}

		opts := []acp.ToolCallUpdateOpt{acp.WithUpdateStatus(status)}
		if len(content) > 0 {
			opts = append(opts, acp.WithUpdateContent(content))
		}

		return []acp.SessionUpdate{acp.UpdateToolCall(
			acp.ToolCallId(message.ToolCallID),
			opts...,
		)}, nil
	default:
		return nil, nil
	}
}

// assistantReplayUpdates projects one stored assistant row, including image
// blocks, through the same validation as live emission so replay delivers
// identical typed content.
func assistantReplayUpdates(
	message pi.AgentMessage,
	blocks []pi.ContentBlock,
	limits ImageLimits,
) ([]acp.SessionUpdate, *imageOutputError) {
	updates := make([]acp.SessionUpdate, 0, len(blocks))
	messageID := message.ACPMessageID

	var messageIDPtr *string
	if messageID != "" {
		messageIDPtr = &messageID
	}

	for index := range blocks {
		block := &blocks[index]

		switch block.Type {
		case contentBlockTypeText:
			if block.Text != "" {
				updates = append(updates, acp.SessionUpdate{AgentMessageChunk: &acp.SessionUpdateAgentMessageChunk{
					Content: acp.TextBlock(block.Text), MessageId: messageIDPtr,
				}})
			}
		case contentBlockTypeImage:
			if block.Data == "" {
				return nil, sweptImageFailure()
			}

			image, failure := normalizeOutputImage(block.Data, block.MimeType, effectiveOutputImageLimit(limits.MaxOutputBytesPerImage))
			if failure != nil {
				return nil, replayImageFailure(failure)
			}

			updates = append(updates, acp.SessionUpdate{AgentMessageChunk: &acp.SessionUpdateAgentMessageChunk{
				Content: acp.ImageBlock(image.data, image.mime), MessageId: messageIDPtr,
			}})
		case contentBlockTypeThinking:
			if block.Thinking != "" {
				updates = append(updates, acp.SessionUpdate{AgentThoughtChunk: &acp.SessionUpdateAgentThoughtChunk{
					Content: acp.TextBlock(block.Thinking), MessageId: messageIDPtr,
				}})
			}
		case contentBlockTypeToolCall:
			updates = append(updates, replayToolCallUpdate(block))
		}
	}

	return updates, nil
}

// replayToolContent validates one stored tool result row's complete content
// array under the current limits before it is replayed as a snapshot.
func replayToolContent(blocks []pi.ContentBlock, limits ImageLimits) ([]acp.ToolCallContent, *imageOutputError) {
	for index := range blocks {
		if blocks[index].Type == contentBlockTypeImage && blocks[index].Data == "" {
			return nil, sweptImageFailure()
		}
	}

	items, failure := buildToolContent(nil, blocks, limits)
	if failure != nil {
		return nil, replayImageFailure(failure)
	}

	return toolContentSnapshot(items), nil
}

// sweptImageFailure reports a stored image artifact whose bytes are gone: the
// bounded artifact window swept them, so the load fails truthfully instead
// of omitting the artifact.
func sweptImageFailure() *imageOutputError {
	return &imageOutputError{
		reason:  imageReasonStorageFailed,
		message: "stored image artifact bytes are no longer available",
	}
}

func replayToolCallUpdate(block *pi.ContentBlock) acp.SessionUpdate {
	var toolCall struct {
		ID        string          `json:"id"`
		Name      string          `json:"name"`
		Arguments json.RawMessage `json:"arguments,omitempty"`
	}

	toolCall.ID = block.ID
	toolCall.Name = block.Name

	if len(block.Arguments) > 0 {
		toolCall.Arguments = block.Arguments
	}

	opts := []acp.ToolCallStartOpt{
		acp.WithStartKind(toolKindForName(toolCall.Name)),
		acp.WithStartStatus(acp.ToolCallStatusCompleted),
	}
	if len(toolCall.Arguments) > 0 {
		opts = append(opts, acp.WithStartRawInput(toolCall.Arguments))
	}

	return acp.StartToolCall(acp.ToolCallId(toolCall.ID), toolCall.Name, opts...)
}

func decodeStoreRow(entry SessionStoreEntry) (storeRow, bool) {
	var row storeRow
	if err := json.Unmarshal(entry, &row); err != nil {
		return storeRow{}, false
	}

	return row, true
}

// storeSessionTitle derives a display title from mirrored rows: the native
// session name when one was recorded, else the first user message text, else
// the session id.
func storeSessionTitle(sessionID string, entries []SessionStoreEntry) string {
	for _, entry := range entries {
		row, ok := decodeStoreRow(entry)
		if !ok {
			continue
		}

		if row.Type == storeRowTypeSessionInfo && strings.TrimSpace(row.Name) != "" {
			return strings.TrimSpace(row.Name)
		}
	}

	for _, entry := range entries {
		row, ok := decodeStoreRow(entry)
		if !ok || row.Type != storeRowTypeMessage {
			continue
		}

		var message pi.AgentMessage
		if err := json.Unmarshal(row.Message, &message); err != nil {
			continue
		}

		if message.Role != messageRoleUser {
			continue
		}

		blocks, err := message.ContentBlocks()
		if err != nil {
			continue
		}

		for index := range blocks {
			if blocks[index].Type == contentBlockTypeText {
				if title := normalizeLiveSessionTitle(blocks[index].Text); title != "" {
					return title
				}
			}
		}
	}

	return sessionID
}

// storeSessionCwd reads the session working directory from the mirrored
// native header row.
func storeSessionCwd(entries []SessionStoreEntry) string {
	for _, entry := range entries {
		row, ok := decodeStoreRow(entry)
		if !ok {
			continue
		}

		if row.Type == storeRowTypeSession {
			return row.Cwd
		}
	}

	return ""
}

// storeSessionHasContent reports whether the native session has conversation
// content. Initialization and configuration entries alone cannot be cloned.
func storeSessionHasContent(entries []SessionStoreEntry) bool {
	for _, entry := range entries {
		row, ok := decodeStoreRow(entry)
		if !ok {
			continue
		}

		if row.Type == storeRowTypeMessage {
			return true
		}
	}

	return false
}
