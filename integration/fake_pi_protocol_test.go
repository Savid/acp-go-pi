//go:build integration

package integration

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/savid/acp-go-pi/internal/pi"
	"github.com/stretchr/testify/require"
)

const harnessCallTimeout = 15 * time.Second

var entryIDPattern = regexp.MustCompile(`^[0-9a-f]{8}$`)

type eventRecorder struct {
	mu    sync.Mutex
	kinds []string
}

func (r *eventRecorder) append(kind string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.kinds = append(r.kinds, kind)
}

func (r *eventRecorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]string(nil), r.kinds...)
}

func (r *eventRecorder) count(kind string) int {
	total := 0
	for _, recorded := range r.snapshot() {
		if recorded == kind {
			total++
		}
	}

	return total
}

func (r *eventRecorder) waitForKind(t *testing.T, kind string) {
	t.Helper()

	require.Eventually(t, func() bool {
		return r.count(kind) > 0
	}, harnessCallTimeout, 10*time.Millisecond, "event %s not observed; saw %v", kind, r.snapshot())
}

// harness drives one pi-protocol binary (fake or real) through the native
// client, with the launch posture the wrapper uses: isolated agent dir,
// per-session session dir, scrubbed environment.
type harness struct {
	process    *pi.Process
	client     *pi.Client
	events     *eventRecorder
	uiRequests chan pi.UIRequest
	root       string
	sessionDir string
}

func startHarness(t *testing.T, ctx context.Context, executable string, withBridge bool) *harness {
	t.Helper()

	root := t.TempDir()
	agentDir := filepath.Join(root, "agent")
	sessionDir := filepath.Join(root, "sessions")
	require.NoError(t, os.MkdirAll(sessionDir, 0o700))

	spec := pi.LaunchSpec{
		ExecutablePath: executable,
		AgentDir:       agentDir,
		SessionDir:     sessionDir,
		Cwd:            root,
	}

	if withBridge {
		extensions, err := pi.WriteExtensions(agentDir, false)
		require.NoError(t, err)
		spec.ExtensionPaths = extensions
		spec.Env = map[string]string{pi.EnvPermissionMode: pi.PermissionModeAsk}
	}

	process, err := pi.StartProcess(ctx, spec)
	require.NoError(t, err)

	client := pi.NewClient(process.Stdin(), process.Stdout())
	require.NoError(t, client.Start(ctx))

	h := &harness{
		process:    process,
		client:     client,
		events:     &eventRecorder{},
		uiRequests: make(chan pi.UIRequest, 4),
		root:       root,
		sessionDir: sessionDir,
	}

	go func() {
		for event := range client.Events() {
			h.events.append(event.Kind())
		}
	}()

	go func() {
		for request := range client.UIRequests() {
			h.uiRequests <- request
		}
	}()

	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		_ = process.Shutdown(shutdownCtx)
		_ = process.Close()
		_ = client.Stop()
	})

	return h
}

func startFakeHarness(t *testing.T, ctx context.Context, scenario fakeScenario, withBridge bool) *harness {
	t.Helper()

	requireRunIntegration(t)

	return startHarness(t, ctx, fakePiExecutable(t, scenario), withBridge)
}

func (h *harness) call(t *testing.T, ctx context.Context, fields map[string]any) pi.Response {
	t.Helper()

	callCtx, cancel := context.WithTimeout(ctx, harnessCallTimeout)
	defer cancel()

	response, err := h.client.Call(callCtx, fields)
	require.NoError(t, err)

	return response
}

func TestFakePiVersionProbe(t *testing.T) {
	requireRunIntegration(t)
	t.Parallel()

	version, err := pi.ProbeVersion(t.Context(), fakePiExecutable(t, fakeScenario{}), integrationContainmentSpec(t))
	require.NoError(t, err)
	require.Equal(t, fakePiVersion, version)
	require.NoError(t, pi.CheckMinimumVersion(version, pi.DefaultMinimumVersion))
}

// TestFakePiMatchesRealPi drives the fake and the real pi binary through the
// same command sequence and requires matching responses, pinning the fake's
// fidelity to the observed native protocol.
func TestFakePiMatchesRealPi(t *testing.T) {
	realPath := smokePiPath(t)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	fake := startHarness(t, ctx, fakePiExecutable(t, fakeScenario{}), false)
	real := startHarness(t, ctx, realPath, false)
	pair := []*harness{fake, real}

	states := make([]pi.SessionState, 0, 2)
	for _, h := range pair {
		state, err := h.client.GetState(ctx)
		require.NoError(t, err)
		require.False(t, state.IsStreaming)
		require.Zero(t, state.MessageCount)
		require.Len(t, state.SessionID, 36)
		require.True(t, strings.HasPrefix(state.SessionFile, h.sessionDir))
		require.True(t, strings.HasSuffix(state.SessionFile, state.SessionID+".jsonl"))
		require.NoFileExists(t, state.SessionFile, "session files are created lazily")
		states = append(states, state)
	}

	require.Equal(t, states[1].ThinkingLevel, states[0].ThinkingLevel)
	require.Equal(t, states[1].SteeringMode, states[0].SteeringMode)
	require.Equal(t, states[1].FollowUpMode, states[0].FollowUpMode)
	require.Equal(t, states[1].AutoCompactionEnabled, states[0].AutoCompactionEnabled)
	require.Equal(t, states[1].Model, states[0].Model, "credential-less model placeholder must match")

	leafIDs := make([]string, 0, 2)
	for _, h := range pair {
		commands, err := h.client.GetCommands(ctx)
		require.NoError(t, err)
		require.Empty(t, commands)

		models, err := h.client.GetAvailableModels(ctx)
		require.NoError(t, err)
		require.Empty(t, models)

		entries, err := h.client.GetEntries(ctx, "")
		require.NoError(t, err)
		require.Len(t, entries.Entries, 1, "a fresh RPC session carries one startup thinking_level_change entry")
		require.Contains(t, string(entries.Entries[0]), `"thinking_level_change"`)
		require.Contains(t, string(entries.Entries[0]), `"parentId":null`)
		require.NotNil(t, entries.LeafID)
		require.Regexp(t, entryIDPattern, *entries.LeafID)
		leafIDs = append(leafIDs, *entries.LeafID)

		_, err = h.client.GetEntries(ctx, "deadbeef")
		requireCommandError(t, err, "get_entries", "Entry not found: deadbeef")

		_, err = h.client.SetModel(ctx, "nope", "missing")
		requireCommandError(t, err, "set_model", "Model not found: nope/missing")
	}

	for i, h := range pair {
		_, err := h.client.Clone(ctx)
		requireCommandError(t, err, "clone", fmt.Sprintf("Entry %s not found", leafIDs[i]))

		response := h.call(t, ctx, map[string]any{"type": "bogus_command"})
		require.False(t, response.Success)
		require.Equal(t, "bogus_command", response.Command)
		require.Equal(t, "Unknown command: bogus_command", response.Error)

		require.NoError(t, h.client.SetThinkingLevel(ctx, "bogus"),
			"pi accepts invalid thinking levels with success and coerces them")
		require.NoError(t, h.client.SetAutoRetry(ctx, false))
		require.NoError(t, h.client.Abort(ctx), "abort while idle acks immediately")
	}

	for _, h := range pair {
		_, err := h.process.Stdin().Write([]byte("this is not json\n"))
		require.NoError(t, err)

		// The parse-failure response carries no id, so it resolves no pending
		// call and counts as a stray.
		stats, err := h.client.GetSessionStats(ctx)
		require.NoError(t, err)
		require.Zero(t, stats.TotalMessages)
		require.Nil(t, stats.ContextUsage)
		require.Eventually(t, func() bool { return h.client.StrayResponses() == 1 },
			harnessCallTimeout, 10*time.Millisecond)
		require.Zero(t, h.client.DecodeFailures())
	}

	for _, h := range pair {
		require.NoError(t, h.client.SetSessionName(ctx, "fidelity-probe"))
		h.events.waitForKind(t, "session_info_changed")

		state, err := h.client.GetState(ctx)
		require.NoError(t, err)
		require.Equal(t, "fidelity-probe", state.SessionName)
	}

	for i, h := range pair {
		cancelled, err := h.client.NewSession(ctx, "")
		require.NoError(t, err)
		require.False(t, cancelled)

		state, err := h.client.GetState(ctx)
		require.NoError(t, err)
		require.NotEqual(t, states[i].SessionID, state.SessionID)
		require.Empty(t, state.SessionName)

		err = h.client.Prompt(ctx, "Reply with exactly OK", nil)
		var commandErr *pi.CommandError
		require.ErrorAs(t, err, &commandErr)
		require.True(t, strings.HasPrefix(commandErr.Message, "No API key found for the selected model."),
			"prompt without credentials is rejected before acceptance: %q", commandErr.Message)
	}

	for _, h := range pair {
		require.NoError(t, h.process.CloseStdin())

		select {
		case <-h.process.Exited():
			require.NoError(t, h.process.WaitErr(), "stdin EOF must exit 0; stderr: %s", h.process.StderrTail())
		case <-time.After(harnessCallTimeout):
			t.Fatal("harness did not exit on stdin EOF")
		}
	}
}

func requireCommandError(t *testing.T, err error, command string, message string) {
	t.Helper()

	var commandErr *pi.CommandError
	require.ErrorAs(t, err, &commandErr)
	require.Equal(t, command, commandErr.Command)
	require.Equal(t, message, commandErr.Message)
}

func TestFakePiPromptTurnLifecycle(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	h := startFakeHarness(t, ctx, fakeTurnScenario(), false)

	// The prompt ack precedes the turn's events; the turn then streams
	// asynchronously until agent_settled.
	require.NoError(t, h.client.Prompt(ctx, "hello", nil))
	h.events.waitForKind(t, pi.EventTypeAgentSettled)

	kinds := h.events.snapshot()
	order := []string{
		pi.EventTypeAgentStart,
		pi.EventTypeTurnStart,
		pi.EventTypeMessageStart,
		pi.EventTypeMessageUpdate,
		pi.EventTypeMessageEnd,
		pi.EventTypeTurnEnd,
		pi.EventTypeAgentEnd,
		pi.EventTypeAgentSettled,
	}
	previous := -1
	for _, kind := range order {
		index := indexOfKind(kinds, kind)
		require.Greater(t, index, previous, "event %s out of order: %v", kind, kinds)
		previous = index
	}
	require.Equal(t, 1, h.events.count(pi.EventTypeAgentSettled))

	// The session file is durable at agent_settled: header row first, then
	// tree entries whose rows get_entries reports identically.
	state, err := h.client.GetState(ctx)
	require.NoError(t, err)
	require.FileExists(t, state.SessionFile)

	data, err := os.ReadFile(state.SessionFile)
	require.NoError(t, err)
	lines := strings.Split(strings.TrimRight(string(data), "\n"), "\n")
	require.GreaterOrEqual(t, len(lines), 4)
	require.Contains(t, lines[0], `"type":"session"`)
	require.Contains(t, lines[0], `"version":3`)
	require.Contains(t, lines[0], fmt.Sprintf(`"id":%q`, state.SessionID))

	entries, err := h.client.GetEntries(ctx, "")
	require.NoError(t, err)
	require.Equal(t, len(lines)-1, len(entries.Entries))
	for i, row := range entries.Entries {
		require.JSONEq(t, lines[i+1], string(row))
	}
	require.Contains(t, string(entries.Entries[len(entries.Entries)-1]), fakeDefaultReply)

	stats, err := h.client.GetSessionStats(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, stats.UserMessages)
	require.Equal(t, 1, stats.AssistantMessages)
	require.NotNil(t, stats.ContextUsage)
	require.NotNil(t, stats.ContextUsage.Tokens)

	require.NoError(t, h.client.Prompt(ctx, "second", nil))
	require.Eventually(t, func() bool { return h.events.count(pi.EventTypeAgentSettled) == 2 },
		harnessCallTimeout, 10*time.Millisecond, "agent_settled fires exactly once per accepted prompt")
}

func indexOfKind(kinds []string, kind string) int {
	for i, recorded := range kinds {
		if recorded == kind {
			return i
		}
	}

	return -1
}

func TestFakePiAbortBarrier(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	scenario := fakeTurnScenario()
	scenario.DeltaTexts = []string{"one ", "two ", "three ", "four ", "five "}
	scenario.StreamDelayMs = 200

	h := startFakeHarness(t, ctx, scenario, false)

	require.NoError(t, h.client.Prompt(ctx, "count", nil))
	h.events.waitForKind(t, pi.EventTypeMessageUpdate)

	require.NoError(t, h.client.Abort(ctx))

	// The abort ack trails the terminal event ladder of the aborted turn.
	require.Eventually(t, func() bool {
		kinds := h.events.snapshot()

		return indexOfKind(kinds, pi.EventTypeMessageEnd) >= 0 &&
			indexOfKind(kinds, pi.EventTypeMessageEnd) < indexOfKind(kinds, pi.EventTypeTurnEnd) &&
			indexOfKind(kinds, pi.EventTypeTurnEnd) < indexOfKind(kinds, pi.EventTypeAgentEnd) &&
			indexOfKind(kinds, pi.EventTypeAgentEnd) < indexOfKind(kinds, pi.EventTypeAgentSettled)
	}, time.Second, 5*time.Millisecond, "terminal events must precede the abort ack: %v", h.events.snapshot())

	require.Equal(t, 1, h.events.count(pi.EventTypeAgentSettled), "abort still settles exactly once")

	// The aborted partial assistant message is retained in the session.
	entries, err := h.client.GetEntries(ctx, "")
	require.NoError(t, err)
	require.Contains(t, string(entries.Entries[len(entries.Entries)-1]), `"stopReason":"aborted"`)
	require.Contains(t, string(entries.Entries[len(entries.Entries)-1]), fakeAbortedErrorMessage)

	require.NoError(t, h.client.Prompt(ctx, "again", nil))
	require.Eventually(t, func() bool { return h.events.count(pi.EventTypeAgentSettled) == 2 },
		harnessCallTimeout, 10*time.Millisecond)
}

func TestFakePiBusyRejectionAndSteerAck(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	scenario := fakeTurnScenario()
	scenario.DeltaTexts = []string{"a", "b", "c", "d", "e", "f"}
	scenario.StreamDelayMs = 300

	h := startFakeHarness(t, ctx, scenario, false)

	require.NoError(t, h.client.Prompt(ctx, "slow", nil))
	h.events.waitForKind(t, pi.EventTypeMessageUpdate)

	err := h.client.Prompt(ctx, "busy", nil)
	requireCommandError(t, err, "prompt", fakeBusyError)

	// A queued prompt is acked only after its queue_update event.
	response := h.call(t, ctx, map[string]any{
		"type":              "prompt",
		"message":           "queued",
		"streamingBehavior": "steer",
	})
	require.True(t, response.Success)
	require.Eventually(t, func() bool { return h.events.count(pi.EventTypeQueueUpdate) == 1 },
		harnessCallTimeout, 5*time.Millisecond)

	state, err := h.client.GetState(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, state.PendingMessageCount)

	response = h.call(t, ctx, map[string]any{"type": "steer", "message": "steer more"})
	require.True(t, response.Success)
	require.Equal(t, "steer", response.Command)
	require.Eventually(t, func() bool { return h.events.count(pi.EventTypeQueueUpdate) == 2 },
		harnessCallTimeout, 5*time.Millisecond)

	require.NoError(t, h.client.Abort(ctx))
}

func TestFakePiMalformedAndNullRecords(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	h := startFakeHarness(t, ctx, fakeScenario{}, false)

	_, err := h.process.Stdin().Write([]byte("garbage line\n\n[1,2]\n\"text\"\n"))
	require.NoError(t, err)

	state, err := h.client.GetState(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, state.SessionID)
	require.Eventually(t, func() bool { return h.client.StrayResponses() == 4 },
		harnessCallTimeout, 10*time.Millisecond,
		"parse and unknown-command rejections carry no id and resolve nothing")

	// A bare `null` record crashes real pi; the fake reproduces the exit.
	_, err = h.process.Stdin().Write([]byte("null\n"))
	require.NoError(t, err)

	select {
	case <-h.process.Exited():
		require.Error(t, h.process.WaitErr())
	case <-time.After(harnessCallTimeout):
		t.Fatal("fake pi did not exit on null record")
	}
}

func TestFakePiCloneSwitchAndCursor(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	h := startFakeHarness(t, ctx, fakeTurnScenario(), false)

	original, err := h.client.GetState(ctx)
	require.NoError(t, err)

	require.NoError(t, h.client.Prompt(ctx, "seed", nil))
	require.Eventually(t, func() bool { return h.events.count(pi.EventTypeAgentSettled) == 1 },
		harnessCallTimeout, 10*time.Millisecond)

	entries, err := h.client.GetEntries(ctx, "")
	require.NoError(t, err)
	cursor := *entries.LeafID

	cancelled, err := h.client.Clone(ctx)
	require.NoError(t, err)
	require.False(t, cancelled)

	clone, err := h.client.GetState(ctx)
	require.NoError(t, err)
	require.NotEqual(t, original.SessionID, clone.SessionID, "clone mints a new session id")
	require.NotEqual(t, original.SessionFile, clone.SessionFile)
	require.FileExists(t, clone.SessionFile)

	cloneData, err := os.ReadFile(clone.SessionFile)
	require.NoError(t, err)
	header := strings.SplitN(string(cloneData), "\n", 2)[0]
	require.Contains(t, header, fmt.Sprintf(`"parentSession":%q`, original.SessionFile))

	// The entry-id cursor stays valid across the clone.
	after, err := h.client.GetEntries(ctx, cursor)
	require.NoError(t, err)
	require.Empty(t, after.Entries)

	require.NoError(t, h.client.Prompt(ctx, "in clone", nil))
	require.Eventually(t, func() bool { return h.events.count(pi.EventTypeAgentSettled) == 2 },
		harnessCallTimeout, 10*time.Millisecond)

	after, err = h.client.GetEntries(ctx, cursor)
	require.NoError(t, err)
	require.Len(t, after.Entries, 2)

	switched, err := h.client.SwitchSession(ctx, clone.SessionFile)
	require.NoError(t, err)
	require.False(t, switched)

	state, err := h.client.GetState(ctx)
	require.NoError(t, err)
	require.Equal(t, clone.SessionID, state.SessionID, "the session id lives in the file header and survives switches")
}

func TestFakePiPermissionDialog(t *testing.T) {
	t.Parallel()

	answers := []struct {
		name      string
		respond   func(id string) pi.UIResponse
		wantAllow bool
	}{
		{"allow", func(id string) pi.UIResponse { return pi.UIValueResponse(id, pi.PermissionOptionAllow) }, true},
		{"deny", func(id string) pi.UIResponse { return pi.UIValueResponse(id, pi.PermissionOptionDeny) }, false},
		{"cancel", pi.UICancelResponse, false},
	}

	for _, testCase := range answers {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()

			scenario := fakeTurnScenario()
			scenario.ToolName = "bash"
			scenario.ToolArgs = map[string]any{"command": "echo hi"}

			h := startFakeHarness(t, ctx, scenario, true)

			require.NoError(t, h.client.Prompt(ctx, "run the tool", nil))

			var request pi.UIRequest
			select {
			case request = <-h.uiRequests:
			case <-time.After(harnessCallTimeout):
				t.Fatal("no extension_ui_request observed")
			}

			require.Equal(t, "select", request.Method)
			require.True(t, request.IsDialog())
			require.Equal(t, []string{pi.PermissionOptionAllow, pi.PermissionOptionDeny}, request.Options)

			prompt, ok := pi.ParsePermissionTitle(request.Title)
			require.True(t, ok, "permission dialog title must carry the marker payload: %q", request.Title)
			require.Equal(t, "call_1", prompt.ToolCallID)
			require.Equal(t, "bash", prompt.ToolName)
			require.JSONEq(t, `{"command":"echo hi"}`, string(prompt.Input))

			require.NoError(t, h.client.RespondUI(testCase.respond(request.ID)))

			require.Eventually(t, func() bool { return h.events.count(pi.EventTypeAgentSettled) == 1 },
				harnessCallTimeout, 10*time.Millisecond)

			entries, err := h.client.GetEntries(ctx, "")
			require.NoError(t, err)

			joined := make([]string, 0, len(entries.Entries))
			for _, row := range entries.Entries {
				joined = append(joined, string(row))
			}
			all := strings.Join(joined, "\n")

			if testCase.wantAllow {
				require.Contains(t, all, `"isError":false`)
				require.Contains(t, all, "fake tool output")
			} else {
				require.Contains(t, all, `"isError":true`)
				require.Contains(t, all, "Denied by ACP client")
			}
		})
	}
}
