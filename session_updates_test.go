package piacp

import (
	"encoding/base64"
	"encoding/json"
	"maps"
	"strings"
	"testing"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"

	"github.com/savid/acp-go-core/wire"
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

	var info *acp.SessionSessionInfoUpdate

	for _, update := range h.rec.snapshot() {
		if update.Update.SessionInfoUpdate != nil && update.Update.SessionInfoUpdate.Title != nil {
			info = update.Update.SessionInfoUpdate
		}
	}

	require.Equal(t, []acp.SessionUsageUpdate{
		{Size: 1000, Used: 15, Meta: callMeta(10, 0, 0, 5)},
		{Size: 1000, Used: 15, Cost: usageCost(1)},
	}, usageUpdates(h.rec.snapshot()))
	require.NotNil(t, info)
	require.Equal(t, "HELLO there", *info.Title)
}

// TestUsageFollowsEachResponse proves every model call of a turn reports the
// context it left occupied, never the running sum, while the prompt response
// still carries the turn's summed consumption.
func TestUsageFollowsEachResponse(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	h.initialize()
	session := h.newSession()

	resp, err := h.prompt(session.SessionId, "MULTI", nil)
	require.NoError(t, err)

	require.Equal(t, []acp.SessionUsageUpdate{
		{Size: 1000, Used: 1120, Meta: callMeta(100, 1000, 0, 20)},
		{Size: 1000, Used: 1200, Meta: callMeta(50, 1120, 0, 30)},
		{Size: 1000, Used: 1250, Meta: callMeta(40, 1200, 0, 10)},
		{Size: 1000, Used: 1250, Cost: usageCost(3)},
	}, usageUpdates(h.rec.snapshot()))
	require.NotNil(t, resp.Usage)
	require.Equal(t, 3570, resp.Usage.TotalTokens)
}

// TestUsageReportsResponseInputBeforeOutput proves a provider that reports a
// call's input when its response starts yields that context before the
// response streams, then the context the finished response left.
func TestUsageReportsResponseInputBeforeOutput(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	h.initialize()
	session := h.newSession()

	resp, err := h.prompt(session.SessionId, "EARLY", nil)
	require.NoError(t, err)

	require.Equal(t, []acp.SessionUsageUpdate{
		{Size: 1000, Used: 1150},
		{Size: 1000, Used: 1180, Meta: callMeta(100, 1000, 50, 30)},
		{Size: 1000, Used: 1240},
		{Size: 1000, Used: 1260, Meta: callMeta(60, 1180, 0, 20)},
		{Size: 1000, Used: 1260, Cost: usageCost(2)},
	}, usageUpdates(h.rec.snapshot()))
	require.Equal(t, 2440, resp.Usage.TotalTokens)
}

// TestEmptyUsageStatesNothing proves a usage report whose every figure is
// zero, as a provider that reports usage only when the stream ends sends while
// it streams and a gateway replaying a cached response sends at the end,
// emits nothing and leaves the last figure for settlement, even when pi
// estimates the context at zero tokens.
func TestEmptyUsageStatesNothing(t *testing.T) {
	t.Parallel()

	reported := []acp.SessionUsageUpdate{
		{Size: 1000, Used: 1120, Meta: callMeta(100, 1000, 0, 20)},
		{Size: 1000, Used: 1120, Cost: usageCost(1)},
	}

	for name, tc := range map[string]struct {
		prompt string
		env    map[string]string
		want   []acp.SessionUsageUpdate
		total  int
	}{
		"zero first update":                {"LATEUSAGE", nil, reported, 1120},
		"zero first update, zero estimate": {"LATEUSAGE", map[string]string{fakePiEnvZeroEstimate: "1"}, reported, 1120},
		"cache replay":                     {"REPLAY", nil, reported, 1120},
		"cache replay, zero estimate":      {"REPLAY", map[string]string{fakePiEnvZeroEstimate: "1"}, reported, 1120},
		"only a cache replay":              {"CACHED", map[string]string{fakePiEnvZeroEstimate: "1"}, nil, 0},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			env := map[string]string{fakePiEnv: "1"}
			maps.Copy(env, tc.env)

			h := newHarness(t, WithEnv(env))
			h.initialize()
			session := h.newSession()

			resp, err := h.prompt(session.SessionId, tc.prompt, nil)
			require.NoError(t, err)
			require.Equal(t, tc.want, usageUpdates(h.rec.snapshot()))

			if tc.total == 0 {
				require.Nil(t, resp.Usage, "a turn whose calls reported nothing has no usage")

				return
			}

			require.Equal(t, tc.total, resp.Usage.TotalTokens)
		})
	}
}

// TestRestoredUsage proves a restored session reports pi's estimate of its
// context, and nothing when pi estimates it at zero tokens.
func TestRestoredUsage(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		env  map[string]string
		want []acp.SessionUsageUpdate
	}{
		"estimate":      {map[string]string{"ACP_GO_PI_TEST_ROTATED": "1"}, []acp.SessionUsageUpdate{{Size: 1000, Used: 15}}},
		"zero estimate": {map[string]string{fakePiEnvZeroEstimate: "1"}, nil},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			h := newHarness(t)
			h.initialize()
			session := h.newSession()

			_, err := h.prompt(session.SessionId, "HELLO", nil)
			require.NoError(t, err)

			before := len(h.rec.snapshot())

			_, err = h.conn.ResumeSession(h.ctx(), wire.ResumeSessionRequest(session.SessionId, sessionCwd(t, h, session.SessionId),
				WithSessionPiOptions(NewPiOptions(WithPiEnv(tc.env)))))
			require.NoError(t, err)
			require.Equal(t, tc.want, usageUpdates(h.rec.snapshot()[before:]))
		})
	}
}

// TestRepeatedResponseFrameAddsNothing proves a terminal frame repeated for
// the same message streams no text and reports no usage a second time.
func TestRepeatedResponseFrameAddsNothing(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	h.initialize()
	session := h.newSession()

	resp, err := h.prompt(session.SessionId, "REPEAT", nil)
	require.NoError(t, err)
	require.Equal(t, "Hello world", agentText(h.rec.snapshot()))
	require.Equal(t, []acp.SessionUsageUpdate{
		{Size: 1000, Used: 15, Meta: callMeta(10, 0, 0, 5)},
		{Size: 1000, Used: 15, Cost: usageCost(1)},
	}, usageUpdates(h.rec.snapshot()))
	require.Equal(t, 15, resp.Usage.TotalTokens)
}

// TestUnusableResponsesReportNoUsage proves a failed or aborted model call
// inside a turn reports no context of its own while its cost still counts.
func TestUnusableResponsesReportNoUsage(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		prompt string
		want   []acp.SessionUsageUpdate
	}{
		"failed call retried": {"FLAKY", []acp.SessionUsageUpdate{
			{Size: 1000, Used: 1120, Meta: callMeta(100, 1000, 0, 20)},
			{Size: 1000, Used: 1120, Cost: usageCost(2)},
		}},
		"aborted call": {"EXTERR", []acp.SessionUsageUpdate{
			{Size: 1000, Used: 2, Cost: usageCost(1)},
		}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			h := newHarness(t)
			h.initialize()
			session := h.newSession()

			_, _ = h.prompt(session.SessionId, tc.prompt, nil)
			require.Equal(t, tc.want, usageUpdates(h.rec.snapshot()))
		})
	}
}

// TestSettledUsageAfterCompaction proves settlement never restates the context
// a response left before pi compacted it: with no response since, there is
// nothing to report until the next one.
func TestSettledUsageAfterCompaction(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		env  map[string]string
		size int
	}{
		"context window":    {map[string]string{fakePiEnv: "1"}, 1000},
		"no context window": {map[string]string{fakePiEnv: "1", fakePiEnvNoModel: "1"}, 0},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			h := newHarness(t, WithEnv(tc.env))
			h.initialize()
			session := h.newSession()

			_, err := h.prompt(session.SessionId, "COMPACT", nil)
			require.NoError(t, err)
			require.Equal(t, []acp.SessionUsageUpdate{{Size: tc.size, Used: 950, Meta: callMeta(100, 800, 0, 50)}}, usageUpdates(h.rec.snapshot()))

			_, err = h.prompt(session.SessionId, "HELLO", nil)
			require.NoError(t, err)
			require.Equal(t, []acp.SessionUsageUpdate{
				{Size: tc.size, Used: 950, Meta: callMeta(100, 800, 0, 50)},
				{Size: tc.size, Used: 15, Meta: callMeta(10, 0, 0, 5)},
				{Size: tc.size, Used: 15, Cost: usageCost(2)},
			}, usageUpdates(h.rec.snapshot()))
		})
	}
}

// TestSettledUsageAdoptsNativeModelWindow proves a model pi switched to on its
// own sizes settlement and every later response.
func TestSettledUsageAdoptsNativeModelWindow(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	h.initialize()
	session := h.newSession()

	for _, text := range []string{"SWITCH", "HELLO"} {
		_, err := h.prompt(session.SessionId, text, nil)
		require.NoError(t, err)
	}

	require.Equal(t, []acp.SessionUsageUpdate{
		{Size: 1000, Used: 15, Meta: callMeta(10, 0, 0, 5)},
		{Size: 500, Used: 15, Cost: usageCost(1)},
		{Size: 500, Used: 15, Meta: callMeta(10, 0, 0, 5)},
		{Size: 500, Used: 15, Cost: usageCost(2)},
	}, usageUpdates(h.rec.snapshot()))
}

func TestCancelledTurnReportsNoUsageAfterCancel(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	h.initialize()
	session := h.newSession()

	done := make(chan acp.PromptResponse, 1)

	go func() {
		resp, _ := h.prompt(session.SessionId, "STEPSLOW", nil)
		done <- resp
	}()

	h.rec.waitFor(t, func(updates []acp.SessionNotification) bool { return len(usageUpdates(updates)) == 1 })
	require.NoError(t, h.conn.Cancel(h.ctx(), wire.CancelRequest(session.SessionId)))
	require.Equal(t, acp.StopReasonCancelled, (<-done).StopReason)

	require.Equal(t, []acp.SessionUsageUpdate{{Size: 1000, Used: 1120, Meta: callMeta(100, 1000, 0, 20)}}, usageUpdates(h.rec.snapshot()))
}

func TestAgentOriginUsageReportsSettledStatistics(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	h.initialize(withLifecycle())
	session := h.newSession()

	_, err := h.prompt(session.SessionId, "AGENTWORK", promptMeta(1))
	require.NoError(t, err)

	h.rec.waitFor(t, func(updates []acp.SessionNotification) bool { return idleTransitions(updates, session.SessionId) == 2 })

	require.Equal(t, []acp.SessionUsageUpdate{
		{Size: 1000, Used: 15, Meta: callMeta(10, 0, 0, 5)},
		{Size: 1000, Used: 15, Cost: usageCost(1)},
		{Size: 1000, Used: 15, Meta: callMeta(10, 0, 0, 5)},
		{Size: 1000, Used: 15, Cost: usageCost(2)},
	}, usageUpdates(h.rec.snapshot()))
}

func TestCancelledAgentOriginCycleReportsNoUsage(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	h.initialize(withLifecycle())
	session := h.newSession()

	_, err := h.prompt(session.SessionId, "AGENTHANG", promptMeta(1))
	require.NoError(t, err)

	h.rec.waitFor(t, func(updates []acp.SessionNotification) bool {
		types := eventTypes(lifecycleEvents(updates))

		return len(types) >= 5 && types[4] == "state_update:running"
	})
	require.NoError(t, h.conn.Cancel(h.ctx(), wire.CancelRequest(session.SessionId)))
	h.rec.waitFor(t, func(updates []acp.SessionNotification) bool { return idleTransitions(updates, session.SessionId) == 2 })

	events := lifecycleEvents(h.rec.snapshot())
	require.Equal(t, "cancelled", events[len(events)-1]["outcome"])
	require.Equal(t, []acp.SessionUsageUpdate{
		{Size: 1000, Used: 15, Meta: callMeta(10, 0, 0, 5)},
		{Size: 1000, Used: 15, Cost: usageCost(1)},
	}, usageUpdates(h.rec.snapshot()))
}

func TestContextTokens(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		message pi.AgentMessage
		want    int
		ok      bool
	}{
		"reported total": {pi.AgentMessage{Usage: &pi.Usage{Input: 1, Output: 2, CacheRead: 3, TotalTokens: 9}}, 9, true},
		"summed":         {pi.AgentMessage{Usage: &pi.Usage{Input: 1, Output: 2, CacheRead: 3, CacheWrite: 4}}, 10, true},
		"no usage":       {pi.AgentMessage{}, 0, false},
		"empty":          {pi.AgentMessage{Usage: &pi.Usage{}}, 0, false},
		"total only":     {pi.AgentMessage{Usage: &pi.Usage{TotalTokens: 9}}, 0, false},
		"aborted":        {pi.AgentMessage{StopReason: stopReasonAborted, Usage: &pi.Usage{Input: 1}}, 0, false},
		"failed":         {pi.AgentMessage{StopReason: stopReasonError, Usage: &pi.Usage{Input: 1}}, 0, false},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			got, ok := contextTokens(tc.message)
			require.Equal(t, tc.want, got)
			require.Equal(t, tc.ok, ok)
		})
	}
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
