//go:build integration

package integration

import (
	"encoding/json"
)

// The struct field orders below reproduce the JSON key order real pi 0.80.6
// emits, so fake output is byte-shaped like native output.

type fakeResponse struct {
	ID      string `json:"id,omitempty"`
	Type    string `json:"type"`
	Command string `json:"command,omitempty"`
	Success bool   `json:"success"`
	Error   string `json:"error,omitempty"`
	Data    any    `json:"data,omitempty"`
}

func (s *fakePiServer) respondOK(id string, command string) {
	s.out.writeJSON(fakeResponse{ID: id, Type: "response", Command: command, Success: true})
}

func (s *fakePiServer) respondData(id string, command string, data any) {
	s.out.writeJSON(fakeResponse{ID: id, Type: "response", Command: command, Success: true, Data: data})
}

func (s *fakePiServer) respondError(id string, command string, message string) {
	s.out.writeJSON(fakeResponse{ID: id, Type: "response", Command: command, Success: false, Error: message})
}

type fakeCancelledData struct {
	Cancelled bool `json:"cancelled"`
}

type fakeEntriesData struct {
	Entries []json.RawMessage `json:"entries"`
	LeafID  any               `json:"leafId"`
}

type fakeCommandsData struct {
	Commands []fakeCommandSpec `json:"commands"`
}

type fakeModelsData struct {
	Models []fakeModelPayload `json:"models"`
}

type fakeModelCost struct {
	Input      float64 `json:"input"`
	Output     float64 `json:"output"`
	CacheRead  float64 `json:"cacheRead"`
	CacheWrite float64 `json:"cacheWrite"`
}

type fakeModelPayload struct {
	ID            string        `json:"id"`
	Name          string        `json:"name"`
	API           string        `json:"api"`
	Provider      string        `json:"provider"`
	BaseURL       string        `json:"baseUrl"`
	Reasoning     bool          `json:"reasoning"`
	Input         []string      `json:"input"`
	Cost          fakeModelCost `json:"cost"`
	ContextWindow int64         `json:"contextWindow"`
	MaxTokens     int64         `json:"maxTokens"`
}

func fakeModelValueFor(spec fakeModelSpec) fakeModelPayload {
	return fakeModelPayload{
		ID:            spec.ID,
		Name:          spec.Name,
		API:           "fake-api",
		Provider:      spec.Provider,
		BaseURL:       "http://127.0.0.1:1",
		Reasoning:     true,
		Input:         []string{"text", "image"},
		Cost:          fakeModelCost{Input: 3, Output: 15, CacheRead: 0.3, CacheWrite: 3.75},
		ContextWindow: spec.ContextWindow,
		MaxTokens:     spec.MaxTokens,
	}
}

// fakeUnknownModelValue is the placeholder model object real pi reports when
// no model is configured: every identity field is the string "unknown", not
// null.
func fakeUnknownModelValue() fakeModelPayload {
	return fakeModelPayload{
		ID:       "unknown",
		Name:     "unknown",
		API:      "unknown",
		Provider: "unknown",
		Input:    []string{},
	}
}

type fakeStateData struct {
	Model                 fakeModelPayload `json:"model"`
	ThinkingLevel         string           `json:"thinkingLevel"`
	IsStreaming           bool             `json:"isStreaming"`
	IsCompacting          bool             `json:"isCompacting"`
	SteeringMode          string           `json:"steeringMode"`
	FollowUpMode          string           `json:"followUpMode"`
	SessionFile           string           `json:"sessionFile"`
	SessionID             string           `json:"sessionId"`
	SessionName           string           `json:"sessionName,omitempty"`
	AutoCompactionEnabled bool             `json:"autoCompactionEnabled"`
	MessageCount          int              `json:"messageCount"`
	PendingMessageCount   int              `json:"pendingMessageCount"`
}

func (s *fakePiServer) stateData() fakeStateData {
	s.mu.Lock()
	defer s.mu.Unlock()

	model := fakeUnknownModelValue()
	if s.model != nil {
		model = fakeModelValueFor(*s.model)
	}

	messageCount := 0
	for _, entry := range s.session.entries {
		if entry.typ == "message" {
			messageCount++
		}
	}

	return fakeStateData{
		Model:                 model,
		ThinkingLevel:         s.session.thinkingLevel,
		IsStreaming:           s.turn != nil,
		SteeringMode:          "one-at-a-time",
		FollowUpMode:          "one-at-a-time",
		SessionFile:           s.session.file,
		SessionID:             s.session.id,
		SessionName:           s.session.name,
		AutoCompactionEnabled: true,
		MessageCount:          messageCount,
		PendingMessageCount:   len(s.steering) + len(s.followUp),
	}
}

func (s *fakePiServer) commandsData() fakeCommandsData {
	commands := s.scenario.Commands
	if commands == nil {
		commands = []fakeCommandSpec{}
	}

	return fakeCommandsData{Commands: commands}
}

func (s *fakePiServer) modelsData() fakeModelsData {
	models := make([]fakeModelPayload, 0, len(s.scenario.Models))
	for _, spec := range s.scenario.Models {
		models = append(models, fakeModelValueFor(spec))
	}

	return fakeModelsData{Models: models}
}

type fakeTokenTotals struct {
	Input      int64 `json:"input"`
	Output     int64 `json:"output"`
	CacheRead  int64 `json:"cacheRead"`
	CacheWrite int64 `json:"cacheWrite"`
	Total      int64 `json:"total"`
}

type fakeContextUsage struct {
	Tokens        *int64   `json:"tokens"`
	ContextWindow int64    `json:"contextWindow"`
	Percent       *float64 `json:"percent"`
}

type fakeStatsData struct {
	SessionFile       string            `json:"sessionFile"`
	SessionID         string            `json:"sessionId"`
	UserMessages      int               `json:"userMessages"`
	AssistantMessages int               `json:"assistantMessages"`
	ToolCalls         int               `json:"toolCalls"`
	ToolResults       int               `json:"toolResults"`
	TotalMessages     int               `json:"totalMessages"`
	Tokens            fakeTokenTotals   `json:"tokens"`
	Cost              float64           `json:"cost"`
	ContextUsage      *fakeContextUsage `json:"contextUsage,omitempty"`
}

func (s *fakePiServer) statsData() fakeStatsData {
	s.mu.Lock()
	defer s.mu.Unlock()

	stats := fakeStatsData{
		SessionFile: s.session.file,
		SessionID:   s.session.id,
	}

	for _, entry := range s.session.entries {
		if entry.typ != "message" {
			continue
		}

		stats.TotalMessages++
		stats.ToolCalls += entry.toolCalls

		switch entry.role {
		case "user":
			stats.UserMessages++
		case "assistant":
			stats.AssistantMessages++
		case "toolResult":
			stats.ToolResults++
		}

		if entry.usage != nil {
			stats.Tokens.Input += entry.usage.Input
			stats.Tokens.Output += entry.usage.Output
			stats.Tokens.CacheRead += entry.usage.CacheRead
			stats.Tokens.CacheWrite += entry.usage.CacheWrite
			stats.Tokens.Total += entry.usage.TotalTokens
			stats.Cost += entry.usage.Cost.Total
		}
	}

	// contextUsage exists only once a model with a context window is
	// selected; tokens/percent stay null until an assistant response
	// supplies usage.
	if s.model != nil && s.model.ContextWindow > 0 {
		usage := &fakeContextUsage{ContextWindow: s.model.ContextWindow}
		if stats.AssistantMessages > 0 {
			tokens := stats.Tokens.Total
			percent := float64(tokens) / float64(s.model.ContextWindow) * 100
			usage.Tokens = &tokens
			usage.Percent = &percent
		}
		stats.ContextUsage = usage
	}

	return stats
}

func (s *fakePiServer) queueUpdateLocked() fakeQueueUpdateEvent {
	return fakeQueueUpdateEvent{
		Type:     "queue_update",
		Steering: append([]string{}, s.steering...),
		FollowUp: append([]string{}, s.followUp...),
	}
}

type fakeTypeOnlyEvent struct {
	Type string `json:"type"`
}

type fakeMessageEvent struct {
	Type    string          `json:"type"`
	Message json.RawMessage `json:"message"`
}

type fakeDelta struct {
	Type         string          `json:"type"`
	ContentIndex *int            `json:"contentIndex,omitempty"`
	Delta        string          `json:"delta,omitempty"`
	Content      string          `json:"content,omitempty"`
	Reason       string          `json:"reason,omitempty"`
	ToolCall     json.RawMessage `json:"toolCall,omitempty"`
}

type fakeMessageUpdateEvent struct {
	Type                  string          `json:"type"`
	Message               json.RawMessage `json:"message"`
	AssistantMessageEvent fakeDelta       `json:"assistantMessageEvent"`
}

type fakeTurnEndEvent struct {
	Type        string            `json:"type"`
	Message     json.RawMessage   `json:"message"`
	ToolResults []json.RawMessage `json:"toolResults"`
}

type fakeAgentEndEvent struct {
	Type      string            `json:"type"`
	Messages  []json.RawMessage `json:"messages"`
	WillRetry bool              `json:"willRetry"`
}

type fakeQueueUpdateEvent struct {
	Type     string   `json:"type"`
	Steering []string `json:"steering"`
	FollowUp []string `json:"followUp"`
}

type fakeToolResult struct {
	Content []any           `json:"content"`
	Details json.RawMessage `json:"details,omitempty"`
}

func fakeToolResultPayload(text string) *fakeToolResult {
	return &fakeToolResult{Content: []any{fakeTextBlock{Type: "text", Text: text}}}
}

type fakeToolExecutionEvent struct {
	Type          string          `json:"type"`
	ToolCallID    string          `json:"toolCallId"`
	ToolName      string          `json:"toolName"`
	Args          json.RawMessage `json:"args,omitempty"`
	PartialResult *fakeToolResult `json:"partialResult,omitempty"`
	Result        *fakeToolResult `json:"result,omitempty"`
	IsError       *bool           `json:"isError,omitempty"`
}

type fakeUIRequestEvent struct {
	Type    string   `json:"type"`
	ID      string   `json:"id"`
	Method  string   `json:"method"`
	Title   string   `json:"title"`
	Options []string `json:"options,omitempty"`
}

type fakeSessionInfoChangedEvent struct {
	Type string `json:"type"`
	Name string `json:"name"`
}

type fakeTextBlock struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type fakeToolCallBlock struct {
	Type      string         `json:"type"`
	ID        string         `json:"id"`
	Name      string         `json:"name"`
	Arguments map[string]any `json:"arguments"`
}

type fakeUsageCost struct {
	Input      float64 `json:"input"`
	Output     float64 `json:"output"`
	CacheRead  float64 `json:"cacheRead"`
	CacheWrite float64 `json:"cacheWrite"`
	Total      float64 `json:"total"`
}

type fakeUsage struct {
	Input       int64         `json:"input"`
	Output      int64         `json:"output"`
	CacheRead   int64         `json:"cacheRead"`
	CacheWrite  int64         `json:"cacheWrite"`
	TotalTokens int64         `json:"totalTokens"`
	Cost        fakeUsageCost `json:"cost"`
}

type fakeUserMessage struct {
	Role      string `json:"role"`
	Content   string `json:"content"`
	Timestamp int64  `json:"timestamp"`
}

type fakeAssistantMessage struct {
	Role         string    `json:"role"`
	Content      []any     `json:"content"`
	API          string    `json:"api"`
	Provider     string    `json:"provider"`
	Model        string    `json:"model"`
	Usage        fakeUsage `json:"usage"`
	StopReason   string    `json:"stopReason,omitempty"`
	ErrorMessage string    `json:"errorMessage,omitempty"`
	Timestamp    int64     `json:"timestamp"`
}

type fakeToolResultMessage struct {
	Role       string `json:"role"`
	ToolCallID string `json:"toolCallId"`
	ToolName   string `json:"toolName"`
	Content    []any  `json:"content"`
	IsError    bool   `json:"isError"`
	Timestamp  int64  `json:"timestamp"`
}

type fakeSessionHeader struct {
	Type          string `json:"type"`
	Version       int    `json:"version"`
	ID            string `json:"id"`
	Timestamp     string `json:"timestamp"`
	Cwd           string `json:"cwd"`
	ParentSession string `json:"parentSession,omitempty"`
}

type fakeEntryRow struct {
	Type          string          `json:"type"`
	ID            string          `json:"id"`
	ParentID      any             `json:"parentId"`
	Timestamp     string          `json:"timestamp"`
	Message       json.RawMessage `json:"message,omitempty"`
	ThinkingLevel string          `json:"thinkingLevel,omitempty"`
	Provider      string          `json:"provider,omitempty"`
	ModelID       string          `json:"modelId,omitempty"`
	Name          string          `json:"name,omitempty"`
}
