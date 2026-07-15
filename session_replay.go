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
// append order for session/load.
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

		updates := messageReplayUpdates(message)
		if message.Role == messageRoleAssistant && len(updates) == 0 && message.ACPMessageID != "" {
			updates = []acp.SessionUpdate{{SessionInfoUpdate: &acp.SessionSessionInfoUpdate{}}}
		}

		if err := s.emitUpdatesWithNativeMessageID(ctx, updates, message.ACPMessageID); err != nil {
			return err
		}
	}

	return nil
}

func replayUpdates(entries []SessionStoreEntry) []acp.SessionUpdate {
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

		updates = append(updates, messageReplayUpdates(message)...)
	}

	return updates
}

func messageReplayUpdates(message pi.AgentMessage) []acp.SessionUpdate {
	blocks, err := message.ContentBlocks()
	if err != nil {
		return nil
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

		return updates
	case messageRoleAssistant:
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

		return updates
	case messageRoleToolResult:
		if message.ToolCallID == "" {
			return nil
		}

		status := acp.ToolCallStatusCompleted
		if message.IsError {
			status = acp.ToolCallStatusFailed
		}

		return []acp.SessionUpdate{acp.UpdateToolCall(
			acp.ToolCallId(message.ToolCallID),
			acp.WithUpdateStatus(status),
			acp.WithUpdateContent(toolCallContent(blocks)),
		)}
	default:
		return nil
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

func toolCallContent(blocks []pi.ContentBlock) []acp.ToolCallContent {
	content := make([]acp.ToolCallContent, 0, len(blocks))

	for index := range blocks {
		block := &blocks[index]

		switch block.Type {
		case contentBlockTypeText:
			if block.Text != "" {
				content = append(content, acp.ToolContent(acp.TextBlock(block.Text)))
			}
		case contentBlockTypeImage:
			if block.Data != "" {
				content = append(content, acp.ToolContent(acp.ImageBlock(block.Data, block.MimeType)))
			}
		}
	}

	return content
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

// storeSessionHasContent reports whether the mirrored rows contain anything
// beyond the native header row; pi rejects cloning a session with no entries.
func storeSessionHasContent(entries []SessionStoreEntry) bool {
	for _, entry := range entries {
		row, ok := decodeStoreRow(entry)
		if !ok {
			continue
		}

		if row.Type != storeRowTypeSession {
			return true
		}
	}

	return false
}
