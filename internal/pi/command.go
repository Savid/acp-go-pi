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
	commandNewSession         = "new_session"
	commandSwitchSession      = "switch_session"
	commandClone              = "clone"
	commandGetState           = "get_state"
	commandGetAvailableModels = "get_available_models"
	commandSetModel           = "set_model"
	commandSetThinkingLevel   = "set_thinking_level"
	commandSetAutoRetry       = "set_auto_retry"
	commandSetSessionName     = "set_session_name"
	commandGetSessionStats    = "get_session_stats"
	commandGetEntries         = "get_entries"
	commandGetCommands        = "get_commands"
)

// fieldEnabled is the JSON key for boolean toggle commands.
const fieldEnabled = "enabled"

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
	SteeringMode          string `json:"steeringMode"`
	FollowUpMode          string `json:"followUpMode"`
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

// SlashCommand is one entry of the get_commands response payload. Location
// and Path cover the flattened wire shape; SourceInfo keeps the structured
// source metadata when pi emits it in place of the flattened fields.
type SlashCommand struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	Source      string          `json:"source"`
	Location    string          `json:"location,omitempty"`
	Path        string          `json:"path,omitempty"`
	SourceInfo  json.RawMessage `json:"sourceInfo,omitempty"`
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

// Entries is the get_entries response payload: raw session rows in append
// order plus the current leaf id (nil for an empty session).
type Entries struct {
	Entries []json.RawMessage `json:"entries"`
	LeafID  *string           `json:"leafId"`
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
// aborted turn precede the response (pi's response-after-events barrier).
func (c *Client) Abort(ctx context.Context) error {
	return c.simpleCall(ctx, map[string]any{commandTypeKey: commandAbort})
}

// NewSession starts a fresh session, optionally recording a parent session
// path. It reports whether an extension cancelled the switch.
func (c *Client) NewSession(ctx context.Context, parentSession string) (bool, error) {
	fields := map[string]any{commandTypeKey: commandNewSession}
	if parentSession != "" {
		fields["parentSession"] = parentSession
	}

	return c.cancellableCall(ctx, fields)
}

// SwitchSession loads a different session file. It reports whether an
// extension cancelled the switch.
func (c *Client) SwitchSession(ctx context.Context, sessionPath string) (bool, error) {
	return c.cancellableCall(ctx, map[string]any{commandTypeKey: commandSwitchSession, "sessionPath": sessionPath})
}

// Clone duplicates the current active branch into a new session with a new
// session id. It reports whether an extension cancelled the clone. Cloning an
// empty session fails natively.
func (c *Client) Clone(ctx context.Context) (bool, error) {
	return c.cancellableCall(ctx, map[string]any{commandTypeKey: commandClone})
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

// SetThinkingLevel sets the reasoning level. pi accepts invalid levels and
// coerces them, so callers validate against IsValidThinkingLevel first.
func (c *Client) SetThinkingLevel(ctx context.Context, level string) error {
	return c.simpleCall(ctx, map[string]any{commandTypeKey: commandSetThinkingLevel, "level": level})
}

// SetAutoRetry enables or disables automatic retry on transient errors. The
// adapter disables it at session start so native failures surface once with
// the real cause.
func (c *Client) SetAutoRetry(ctx context.Context, enabled bool) error {
	return c.simpleCall(ctx, map[string]any{commandTypeKey: commandSetAutoRetry, fieldEnabled: enabled})
}

// SetSessionName sets the session display name.
func (c *Client) SetSessionName(ctx context.Context, name string) error {
	return c.simpleCall(ctx, map[string]any{commandTypeKey: commandSetSessionName, "name": name})
}

// GetSessionStats fetches token usage, cost, and context-window usage.
func (c *Client) GetSessionStats(ctx context.Context) (SessionStats, error) {
	return callWithData[SessionStats](ctx, c, map[string]any{commandTypeKey: commandGetSessionStats})
}

// GetEntries fetches session entries in append order. A non-empty since is a
// durable cursor: only entries strictly after it are returned, and an invalid
// cursor fails with a native error.
func (c *Client) GetEntries(ctx context.Context, since string) (Entries, error) {
	fields := map[string]any{commandTypeKey: commandGetEntries}
	if since != "" {
		fields["since"] = since
	}

	return callWithData[Entries](ctx, c, fields)
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

func (c *Client) cancellableCall(ctx context.Context, fields map[string]any) (bool, error) {
	data, err := callWithData[struct {
		Cancelled bool `json:"cancelled"`
	}](ctx, c, fields)
	if err != nil {
		return false, err
	}

	return data.Cancelled, nil
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
