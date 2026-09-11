package pi

import (
	"context"
	"encoding/json"
	"fmt"
)

// pi RPC command type names.
const (
	commandPrompt             = "prompt"
	commandAbort              = "abort"
	commandGetState           = "get_state"
	commandGetAvailableModels = "get_available_models"
	commandSetModel           = "set_model"
	commandSetThinkingLevel   = "set_thinking_level"
	commandSetAutoRetry       = "set_auto_retry"
	commandGetSessionStats    = "get_session_stats"
	commandGetCommands        = "get_commands"
)

// ImageContent is one base64 image attached to a prompt.
type ImageContent struct {
	Type     string `json:"type"`
	Data     string `json:"data"`
	MimeType string `json:"mimeType"`
}

// NewImageContent constructs an image prompt attachment.
func NewImageContent(data string, mimeType string) ImageContent {
	return ImageContent{Type: "image", Data: data, MimeType: mimeType}
}

// SessionState is the get_state response payload.
type SessionState struct {
	Model                 *Model `json:"model,omitempty"`
	ThinkingLevel         string `json:"thinkingLevel"`
	IsStreaming           bool   `json:"isStreaming"`
	IsCompacting          bool   `json:"isCompacting"`
	SessionFile           string `json:"sessionFile,omitempty"`
	SessionID             string `json:"sessionId"`
	SessionName           string `json:"sessionName,omitempty"`
	AutoCompactionEnabled bool   `json:"autoCompactionEnabled"`
	MessageCount          int    `json:"messageCount"`
	PendingMessageCount   int    `json:"pendingMessageCount"`
}

// Model is one pi model catalog entry.
type Model struct {
	ID            string          `json:"id"`
	Name          string          `json:"name"`
	API           string          `json:"api"`
	Provider      string          `json:"provider"`
	BaseURL       string          `json:"baseUrl,omitempty"`
	Reasoning     bool            `json:"reasoning"`
	Input         []string        `json:"input"`
	ContextWindow int64           `json:"contextWindow"`
	MaxTokens     int64           `json:"maxTokens"`
	Cost          json.RawMessage `json:"cost,omitempty"`
}

// Ref is the model's "provider/id" address.
func (m Model) Ref() string {
	return m.Provider + "/" + m.ID
}

// SlashCommand is one entry of the get_commands response payload.
type SlashCommand struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	Source      string `json:"source"`
	Location    string `json:"location,omitempty"`
	Path        string `json:"path,omitempty"`
}

// TokenTotals is the session-level token usage inside get_session_stats.
type TokenTotals struct {
	Input      int64 `json:"input"`
	Output     int64 `json:"output"`
	CacheRead  int64 `json:"cacheRead"`
	CacheWrite int64 `json:"cacheWrite"`
	Total      int64 `json:"total"`
}

// ContextUsage is the current context-window estimate inside
// get_session_stats. Tokens and Percent are null immediately after
// compaction.
type ContextUsage struct {
	Tokens        *int64   `json:"tokens"`
	ContextWindow int64    `json:"contextWindow"`
	Percent       *float64 `json:"percent"`
}

// SessionStats is the get_session_stats response payload.
type SessionStats struct {
	SessionFile       string        `json:"sessionFile,omitempty"`
	SessionID         string        `json:"sessionId"`
	UserMessages      int           `json:"userMessages"`
	AssistantMessages int           `json:"assistantMessages"`
	ToolCalls         int           `json:"toolCalls"`
	ToolResults       int           `json:"toolResults"`
	TotalMessages     int           `json:"totalMessages"`
	Tokens            TokenTotals   `json:"tokens"`
	Cost              float64       `json:"cost"`
	ContextUsage      *ContextUsage `json:"contextUsage,omitempty"`
}

// Prompt sends a user prompt. The response acknowledges acceptance; results
// stream as events afterwards.
func (c *Client) Prompt(ctx context.Context, message string, images []ImageContent) error {
	fields := map[string]any{commandTypeKey: commandPrompt, "message": message}
	if len(images) > 0 {
		fields["images"] = images
	}

	return c.simpleCall(ctx, fields)
}

// Abort aborts the current agent operation. The terminal events for the
// aborted turn precede the response.
func (c *Client) Abort(ctx context.Context) error {
	return c.simpleCall(ctx, map[string]any{commandTypeKey: commandAbort})
}

// GetState fetches the current session state.
func (c *Client) GetState(ctx context.Context) (SessionState, error) {
	return callWithData[SessionState](ctx, c, map[string]any{commandTypeKey: commandGetState})
}

// GetAvailableModels lists all configured models.
func (c *Client) GetAvailableModels(ctx context.Context) ([]Model, error) {
	data, err := callWithData[struct {
		Models []Model `json:"models"`
	}](ctx, c, map[string]any{commandTypeKey: commandGetAvailableModels})
	if err != nil {
		return nil, err
	}

	return data.Models, nil
}

// SetModel switches to a specific model and returns the selected catalog
// entry.
func (c *Client) SetModel(ctx context.Context, provider string, modelID string) (Model, error) {
	return callWithData[Model](ctx, c, map[string]any{
		commandTypeKey: commandSetModel,
		"provider":     provider,
		"modelId":      modelID,
	})
}

// SetThinkingLevel sends the requested reasoning level to pi. pi acknowledges
// unknown values even when its effective thinking level does not change, so a
// caller that needs the level pi actually runs reads GetState back.
func (c *Client) SetThinkingLevel(ctx context.Context, level string) error {
	return c.simpleCall(ctx, map[string]any{commandTypeKey: commandSetThinkingLevel, "level": level})
}

// SetAutoRetry enables or disables automatic retry on transient errors.
func (c *Client) SetAutoRetry(ctx context.Context, enabled bool) error {
	return c.simpleCall(ctx, map[string]any{commandTypeKey: commandSetAutoRetry, "enabled": enabled})
}

// GetSessionStats fetches token usage, cost, and context-window usage.
func (c *Client) GetSessionStats(ctx context.Context) (SessionStats, error) {
	return callWithData[SessionStats](ctx, c, map[string]any{commandTypeKey: commandGetSessionStats})
}

// GetCommands lists available slash commands (extension commands, prompt
// templates, and skills).
func (c *Client) GetCommands(ctx context.Context) ([]SlashCommand, error) {
	data, err := callWithData[struct {
		Commands []SlashCommand `json:"commands"`
	}](ctx, c, map[string]any{commandTypeKey: commandGetCommands})
	if err != nil {
		return nil, err
	}

	return data.Commands, nil
}

func (c *Client) simpleCall(ctx context.Context, fields map[string]any) error {
	response, err := c.Call(ctx, fields)
	if err != nil {
		return err
	}

	return response.Err()
}

func callWithData[T any](ctx context.Context, c *Client, fields map[string]any) (T, error) {
	var data T

	response, err := c.Call(ctx, fields)
	if err != nil {
		return data, err
	}

	if err := response.Err(); err != nil {
		return data, err
	}

	if len(response.Data) == 0 {
		return data, fmt.Errorf("pi command %s returned no data", response.Command)
	}

	if err := json.Unmarshal(response.Data, &data); err != nil {
		return data, fmt.Errorf("decode %s response data: %w", response.Command, err)
	}

	return data, nil
}
