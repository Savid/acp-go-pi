package piacp

import (
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"

	"github.com/savid/acp-go-pi/internal/pi"
)

func TestAssistantTextIsAppendOnly(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	h.initialize()
	session := h.newSession()

	_, err := h.prompt(session.SessionId, "SUFFIX", nil)
	require.NoError(t, err)
	require.Equal(t, "Hello", agentText(h.rec.snapshot()))

	chunks := 0
	for _, update := range h.rec.snapshot() {
		if update.Update.AgentMessageChunk != nil {
			chunks++
		}
	}

	require.Equal(t, 2, chunks)
}

func TestThoughtChunks(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	h.initialize()
	session := h.newSession()

	_, err := h.prompt(session.SessionId, "THINK", nil)
	require.NoError(t, err)

	var thought strings.Builder
	for _, update := range h.rec.snapshot() {
		if chunk := update.Update.AgentThoughtChunk; chunk != nil {
			thought.WriteString(chunk.Content.Text.Text)
		}
	}

	require.Equal(t, "hmm", thought.String())
	require.Equal(t, "ok", agentText(h.rec.snapshot()))
}

func TestCommandCatalogPublishedAfterResponse(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	h.initialize()
	session := h.newSession()

	h.rec.waitForCount(t, 1)

	first := h.rec.snapshot()[0]
	require.Equal(t, session.SessionId, first.SessionId)
	require.NotNil(t, first.Update.AvailableCommandsUpdate)
	require.Equal(t, []acp.AvailableCommand{{Name: "help", Description: "Show help"}}, first.Update.AvailableCommandsUpdate.AvailableCommands)
}

func TestAvailableCommandsDropsRejectedNames(t *testing.T) {
	t.Parallel()

	commands := availableCommands([]pi.SlashCommand{
		{Name: "help", Description: "Show help"},
		{Name: ""}, {Name: "a/b"}, {Name: "bad name"}, {Name: "tab\there"},
		{Name: "\xff"}, {Name: "zero\u200bwidth"},
	})

	require.Equal(t, []acp.AvailableCommand{{Name: "help", Description: "Show help"}}, commands)
}

func TestUsageAndSessionInfoUpdates(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	h.initialize()
	session := h.newSession()

	_, err := h.prompt(session.SessionId, "HELLO there", nil)
	require.NoError(t, err)

	var usage *acp.SessionUsageUpdate

	var info *acp.SessionSessionInfoUpdate

	for _, update := range h.rec.snapshot() {
		if update.Update.UsageUpdate != nil {
			usage = update.Update.UsageUpdate
		}

		if update.Update.SessionInfoUpdate != nil && update.Update.SessionInfoUpdate.Title != nil {
			info = update.Update.SessionInfoUpdate
		}
	}

	require.NotNil(t, usage)
	require.Equal(t, 1000, usage.Size)
	require.Equal(t, 42, usage.Used)
	require.NotNil(t, usage.Cost)
	require.NotNil(t, info)
	require.Equal(t, "HELLO there", *info.Title)
}

func TestRawEventsOptIn(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	h.initialize()
	session := h.newSession(WithSessionRawEvents(true))

	_, err := h.prompt(session.SessionId, "IMAGE", nil)
	require.NoError(t, err)

	h.rec.waitFor(t, func([]acp.SessionNotification) bool {
		h.rec.mu.Lock()
		defer h.rec.mu.Unlock()

		return len(h.rec.raw) >= 7
	})

	h.rec.mu.Lock()
	raw := append([]json.RawMessage(nil), h.rec.raw...)
	h.rec.mu.Unlock()

	for index, payload := range raw {
		var event struct {
			SessionID string         `json:"sessionId"`
			Sequence  int            `json:"sequence"`
			Source    string         `json:"source"`
			Event     map[string]any `json:"event"`
		}

		require.NoError(t, json.Unmarshal(payload, &event))
		require.Equal(t, string(session.SessionId), event.SessionID)
		require.Equal(t, index+1, event.Sequence)
		require.Equal(t, "pi", event.Source)

		if event.Event["type"] == "message_end" {
			require.NotContains(t, string(payload), tinyPNG)
		}
	}

	other := h.newSession()
	_, err = h.prompt(other.SessionId, "HELLO", nil)
	require.NoError(t, err)

	h.rec.mu.Lock()
	count := len(h.rec.raw)
	h.rec.mu.Unlock()
	require.Equal(t, len(raw), count)
}

func TestRedactImages(t *testing.T) {
	t.Parallel()

	decoded, err := base64.StdEncoding.DecodeString(tinyPNG)
	require.NoError(t, err)

	block := map[string]any{"type": "image", "data": tinyPNG}
	payload := map[string]any{"message": map[string]any{"content": []any{block}}}
	redactImages(payload)

	require.Equal(t, "", block["data"])
	require.Equal(t, len(decoded), block["sizeBytes"], "the padded payload is not over-reported")
}

func TestMergeUsage(t *testing.T) {
	t.Parallel()

	total := mergeUsage(nil, &pi.Usage{Input: 1, Output: 2, CacheRead: 3, CacheWrite: 4})
	total = mergeUsage(total, &pi.Usage{Input: 1})
	require.Equal(t, 2, total.InputTokens)
	require.Equal(t, 11, total.TotalTokens)
}

func TestBearsWork(t *testing.T) {
	t.Parallel()

	require.True(t, bearsWork(pi.AgentStartEvent{}))
	require.True(t, bearsWork(pi.MessageStartEvent{Message: pi.AgentMessage{Role: messageRoleAssistant}}))
	require.False(t, bearsWork(pi.MessageStartEvent{Message: pi.AgentMessage{Role: messageRoleCustom}}))
	require.False(t, bearsWork(pi.UnknownEvent{}))
}
