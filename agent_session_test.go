package piacp

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/coder/acp-go-sdk"
	acpcore "github.com/savid/acp-go-core"
	"github.com/savid/acp-go-core/wire"
	"github.com/savid/acp-go-pi/internal/pi"
	"github.com/stretchr/testify/require"
)

func TestListSessionsActiveAndStored(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	h.initialize()

	cwd := t.TempDir()
	first, err := h.conn.NewSession(h.ctx(), wire.NewSessionRequest(cwd))
	require.NoError(t, err)

	second := h.newSession()

	_, err = h.prompt(second.SessionId, "HELLO stored", nil)
	require.NoError(t, err)

	_, err = h.conn.CloseSession(h.ctx(), acp.CloseSessionRequest{SessionId: second.SessionId})
	require.NoError(t, err)

	list, err := h.conn.ListSessions(h.ctx(), wire.ListSessionsRequest())
	require.NoError(t, err)
	require.Len(t, list.Sessions, 2)
	require.Nil(t, list.NextCursor)

	byID := make(map[acp.SessionId]acp.SessionInfo)
	for _, info := range list.Sessions {
		byID[info.SessionId] = info
	}

	require.Equal(t, cwd, byID[first.SessionId].Cwd)
	require.Equal(t, "HELLO stored", *byID[second.SessionId].Title)

	filtered, err := h.conn.ListSessions(h.ctx(), wire.ListSessionsRequest(wire.WithListSessionsCwd(cwd)))
	require.NoError(t, err)
	require.Len(t, filtered.Sessions, 1)
	require.Equal(t, first.SessionId, filtered.Sessions[0].SessionId)

	_, err = h.conn.ListSessions(h.ctx(), wire.ListSessionsRequest(wire.WithListSessionsCursor("!!")))
	require.Equal(t, "cursor", requestErrorData(t, err)["field"])
}

func TestDeleteTombstonesAndHides(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	h.initialize()
	session := h.newSession()

	_, err := h.prompt(session.SessionId, "HELLO", nil)
	require.NoError(t, err)

	_, err = h.conn.UnstableDeleteSession(h.ctx(), wire.DeleteSessionRequest(session.SessionId))
	require.NoError(t, err)

	_, err = h.conn.UnstableDeleteSession(h.ctx(), wire.DeleteSessionRequest(session.SessionId))
	require.NoError(t, err)

	list, err := h.conn.ListSessions(h.ctx(), wire.ListSessionsRequest())
	require.NoError(t, err)
	require.Empty(t, list.Sessions)

	_, err = h.conn.LoadSession(h.ctx(), wire.LoadSessionRequest(session.SessionId, t.TempDir()))
	require.Equal(t, "unknown session", requestErrorData(t, err)["error"])

	_, err = h.conn.ResumeSession(h.ctx(), wire.ResumeSessionRequest(session.SessionId, t.TempDir()))
	require.Equal(t, "unknown session", requestErrorData(t, err)["error"])

	_, err = h.prompt(session.SessionId, "HELLO", nil)
	require.Equal(t, "unknown session", requestErrorData(t, err)["error"])
	require.NoError(t, h.conn.Cancel(h.ctx(), wire.CancelRequest(session.SessionId)))
}

func TestCloseSessionThenUnknown(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	h.initialize()
	session := h.newSession()

	_, err := h.conn.CloseSession(h.ctx(), acp.CloseSessionRequest{SessionId: session.SessionId})
	require.NoError(t, err)

	_, err = h.conn.CloseSession(h.ctx(), acp.CloseSessionRequest{SessionId: session.SessionId})
	require.Equal(t, "unknown session", requestErrorData(t, err)["error"])
}

func TestTwoSessionsStayIndependent(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	h.initialize()
	first := h.newSession()
	second := h.newSession()

	done := make(chan acp.PromptResponse, 1)

	go func() {
		resp, _ := h.prompt(first.SessionId, "SLOW", nil)
		done <- resp
	}()

	resp, err := h.prompt(second.SessionId, "HELLO", nil)
	require.NoError(t, err)
	require.Equal(t, acp.StopReasonEndTurn, resp.StopReason)

	require.NoError(t, h.conn.Cancel(h.ctx(), wire.CancelRequest(first.SessionId)))
	require.Equal(t, acp.StopReasonCancelled, (<-done).StopReason)
}

func TestSessionEnvironmentAndPath(t *testing.T) {
	dump := filepath.Join(t.TempDir(), "env.txt")
	t.Setenv("ACP_GO_PI_TEST_INHERITED", "from-process")
	t.Setenv("ACP_GO_PI_INTERNAL_LEAK", "dropped")

	dir := t.TempDir()
	h := newHarness(t, WithEnv(map[string]string{fakePiEnv: "1", fakePiEnvDump: dump, "ACP_GO_PI_TEST_AGENT": "agent", "ACP_GO_PI_TEST_OVERRIDDEN": "agent"}))
	h.initialize()
	h.newSession(WithSessionPiOptions(NewPiOptions(
		WithPiEnv(map[string]string{"ACP_GO_PI_TEST_OVERRIDDEN": "session", "ACP_GO_PI_TEST_EMPTY": ""}),
		WithPiExtraPathDirs(dir),
	)))

	data, err := os.ReadFile(dump)
	require.NoError(t, err)

	env := map[string]string{}
	for line := range strings.SplitSeq(string(data), "\n") {
		key, value, _ := strings.Cut(line, "=")
		env[key] = value
	}

	require.Equal(t, "from-process", env["ACP_GO_PI_TEST_INHERITED"])
	require.Equal(t, "agent", env["ACP_GO_PI_TEST_AGENT"])
	require.Equal(t, "session", env["ACP_GO_PI_TEST_OVERRIDDEN"])
	require.Contains(t, env, "ACP_GO_PI_TEST_EMPTY")
	require.NotContains(t, env, "ACP_GO_PI_INTERNAL_LEAK")
	require.Equal(t, "ask", env[pi.EnvPermissionMode])
	require.Equal(t, dir, env[pi.EnvExtraPathDirs])
	require.True(t, strings.HasPrefix(env["PATH"], dir+string(os.PathListSeparator)))
	require.NotEmpty(t, env[pi.EnvAgentDir])
}

func TestRestoreActiveSessionReuses(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	h.initialize()
	session := h.newSession()

	_, err := h.prompt(session.SessionId, "HELLO", nil)
	require.NoError(t, err)

	before := len(h.rec.snapshot())

	resp, err := h.conn.LoadSession(h.ctx(), wire.LoadSessionRequest(session.SessionId, sessionCwd(t, h, session.SessionId)))
	require.NoError(t, err)
	require.NotEmpty(t, resp.ConfigOptions)

	replayed := 0
	for _, update := range h.rec.snapshot()[before:] {
		if update.Update.UserMessageChunk != nil {
			replayed++
		}
	}

	require.Equal(t, 1, replayed)
}

func sessionCwd(t *testing.T, h *harness, id acp.SessionId) string {
	t.Helper()

	list, err := h.conn.ListSessions(h.ctx(), wire.ListSessionsRequest())
	require.NoError(t, err)

	for _, info := range list.Sessions {
		if info.SessionId == id {
			return info.Cwd
		}
	}

	t.Fatalf("session %s not listed", id)

	return ""
}

func TestRestoreChangedCarrierRestarts(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	h.initialize()
	session := h.newSession()

	_, err := h.prompt(session.SessionId, "HELLO", nil)
	require.NoError(t, err)

	cwd := sessionCwd(t, h, session.SessionId)

	_, err = h.conn.ResumeSession(h.ctx(), wire.ResumeSessionRequest(session.SessionId, cwd,
		WithSessionPiOptions(NewPiOptions(WithPiEnv(map[string]string{"ACP_GO_PI_TEST_ROTATED": "1"})))))
	require.NoError(t, err)

	resp, err := h.prompt(session.SessionId, "HELLO", nil)
	require.NoError(t, err)
	require.Equal(t, acp.StopReasonEndTurn, resp.StopReason)
}

func TestSetConfigOptions(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	h.initialize()
	session := h.newSession()

	require.Len(t, session.ConfigOptions, 2)
	require.Equal(t, configModel, session.ConfigOptions[0].Select.Id)
	require.Equal(t, acp.SessionConfigValueId("fake/vision"), session.ConfigOptions[0].Select.CurrentValue)
	require.Equal(t, configThoughtLevel, session.ConfigOptions[1].Select.Id)
	require.Equal(t, acp.SessionConfigValueId("medium"), session.ConfigOptions[1].Select.CurrentValue)

	resp, err := h.conn.SetSessionConfigOption(h.ctx(), SetModelRequest(session.SessionId, "fake/text-only"))
	require.NoError(t, err)
	require.Equal(t, acp.SessionConfigValueId("fake/text-only"), resp.ConfigOptions[0].Select.CurrentValue)

	_, err = h.conn.SetSessionConfigOption(h.ctx(), SetModelRequest(session.SessionId, "fake/nope"))
	require.Equal(t, "value", requestErrorData(t, err)["field"])

	_, err = h.conn.SetSessionConfigOption(h.ctx(), SetModelRequest(session.SessionId, "nomodel"))
	require.Equal(t, "value", requestErrorData(t, err)["field"])

	resp, err = h.conn.SetSessionConfigOption(h.ctx(), wire.SetConfigOptionRequest(session.SessionId, configThoughtLevel, "high"))
	require.NoError(t, err)
	require.Equal(t, acp.SessionConfigValueId("high"), resp.ConfigOptions[1].Select.CurrentValue)

	resp, err = h.conn.SetSessionConfigOption(h.ctx(), wire.SetConfigOptionRequest(session.SessionId, configThoughtLevel, "bogus"))
	require.NoError(t, err)
	require.Equal(t, acp.SessionConfigValueId("high"), resp.ConfigOptions[1].Select.CurrentValue)

	_, err = h.conn.SetSessionConfigOption(h.ctx(), wire.SetConfigOptionRequest(session.SessionId, "mode", "x"))
	require.Equal(t, "configId", requestErrorData(t, err)["field"])

	_, err = h.conn.SetSessionConfigOption(h.ctx(), acp.SetSessionConfigOptionRequest{Boolean: &acp.SetSessionConfigOptionBoolean{SessionId: session.SessionId, ConfigId: "x", Value: true}})
	require.Equal(t, "type", requestErrorData(t, err)["field"])

	h.rec.waitFor(t, func(updates []acp.SessionNotification) bool {
		for _, update := range updates {
			if update.Update.ConfigOptionUpdate != nil {
				return true
			}
		}

		return false
	})
}

func TestConfiguredModelsAppendAfterCatalog(t *testing.T) {
	t.Parallel()

	h := newHarness(t, WithConfiguredModels([]string{"fake/vision", "other/listed"}))
	h.initialize()
	session := h.newSession()

	values := *session.ConfigOptions[0].Select.Options.Ungrouped
	names := make([]string, 0, len(values))

	for _, value := range values {
		names = append(names, string(value.Value))
	}

	require.Equal(t, []string{"fake/vision", "fake/text-only", "other/listed"}, names)
	require.Equal(t, map[string]any{"pi": map[string]any{"modelId": "fake/vision", "contextWindow": float64(1000), "maxOutputTokens": float64(100)}}, values[0].Meta)
}

func TestLifecycleKeyRefusedOnOtherSurfaces(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	h.initialize(withLifecycle())
	session := h.newSession()

	meta := map[string]any{wire.LifecycleKey: map[string]any{"version": 1}}

	_, err := h.conn.ListSessions(h.ctx(), acp.ListSessionsRequest{Meta: meta})
	require.Equal(t, `_meta["`+wire.LifecycleKey+`"]`, requestErrorData(t, err)["field"])

	_, err = h.conn.CloseSession(h.ctx(), acp.CloseSessionRequest{SessionId: session.SessionId, Meta: meta})
	require.Equal(t, "unsupported", requestErrorData(t, err)["error"])

	_, err = h.conn.UnstableDeleteSession(h.ctx(), acp.UnstableDeleteSessionRequest{SessionId: session.SessionId, Meta: meta})
	require.Equal(t, "unsupported", requestErrorData(t, err)["error"])

	_, err = h.conn.SetSessionConfigOption(h.ctx(), acp.SetSessionConfigOptionRequest{ValueId: &acp.SetSessionConfigOptionValueId{SessionId: session.SessionId, ConfigId: configModel, Value: "fake/vision", Meta: meta}})
	require.Equal(t, "unsupported", requestErrorData(t, err)["error"])
}

func TestEmbeddedAgentPublishesInline(t *testing.T) {
	t.Parallel()

	agent := NewAgent(testOptions(t)...)
	t.Cleanup(func() { _ = agent.Close() })

	rec := newRecorder()
	agent.attach(rec, nil)

	ctx := context.Background()
	_, err := agent.Initialize(ctx, acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber, Meta: map[string]any{wire.LifecycleKey: map[string]any{"version": 1}}})
	require.NoError(t, err)

	session, err := agent.NewSession(ctx, wire.NewSessionRequest(t.TempDir()))
	require.NoError(t, err)

	updates := rec.snapshot()
	require.Len(t, updates, 2)
	require.NotNil(t, updates[0].Update.AvailableCommandsUpdate)
	require.Equal(t, []string{"lifecycle_snapshot"}, eventTypes(lifecycleEvents(updates)))

	_, err = agent.Prompt(ctx, wire.PromptRequest(session.SessionId, acp.TextBlock("HELLO")))
	require.Equal(t, "missing", requestErrorData(t, err)["error"])

	request := wire.TextPromptRequest(session.SessionId, "HELLO")
	request.Meta = promptMeta(1)
	resp, err := agent.Prompt(ctx, request)
	require.NoError(t, err)
	require.Equal(t, acp.StopReasonEndTurn, resp.StopReason)
}

type commitBarrier struct {
	acpcore.SessionStore
	block   atomic.Bool
	entered chan acpcore.SessionKey
	release chan struct{}
}

func (s *commitBarrier) Replace(ctx context.Context, key acpcore.SessionKey, replacements []acpcore.SessionStoreReplacement) error {
	if s.block.CompareAndSwap(true, false) {
		s.entered <- key
		select {
		case <-s.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return s.SessionStore.Replace(ctx, key, replacements)
}
func TestEstablishmentExcludesPrompt(t *testing.T) {
	t.Parallel()

	for _, phase := range []string{"new", "cold_load"} {
		t.Run(phase, func(t *testing.T) {
			t.Parallel()

			store := &commitBarrier{SessionStore: acpcore.NewInMemorySessionStore(), entered: make(chan acpcore.SessionKey, 1), release: make(chan struct{})}
			release := sync.OnceFunc(func() { close(store.release) })
			h := newHarness(t, WithSessionStore(store))
			h.initialize()
			t.Cleanup(release)
			cwd := t.TempDir()
			var id acp.SessionId
			if phase == "cold_load" {
				created, err := h.conn.NewSession(h.ctx(), wire.NewSessionRequest(cwd))
				require.NoError(t, err)
				id = created.SessionId
				_, err = h.conn.CloseSession(h.ctx(), acp.CloseSessionRequest{SessionId: id})
				require.NoError(t, err)
			}
			store.block.Store(true)
			done := make(chan error, 1)
			ctx := h.ctx()
			go func() {
				if phase == "new" {
					_, err := h.conn.NewSession(ctx, wire.NewSessionRequest(cwd))
					done <- err
				} else {
					_, err := h.conn.LoadSession(ctx, wire.LoadSessionRequest(id, cwd))
					done <- err
				}
			}()
			select {
			case key := <-store.entered:
				id = acp.SessionId(key.SessionID)
			case <-ctx.Done():
				t.Fatal("establishment never reached commit")
			}
			// An empty prompt cannot dispatch native work, but admission must still reject
			// it as busy before parsing content while establishment holds the session.
			_, err := h.conn.Prompt(ctx, wire.PromptRequest(id))
			data := requestErrorData(t, err)
			release()
			require.NoError(t, <-done)
			require.Equal(t, "session_prompt", data["limit"], "establishing session admitted a prompt into content validation: %v", data)
		})
	}
}

func TestFailedRestoreCloseReleasesSlot(t *testing.T) {
	for _, method := range []string{acp.AgentMethodSessionLoad, acp.AgentMethodSessionResume} {
		t.Run(method, func(t *testing.T) {
			store := &recoveryFaultStore{SessionStore: acpcore.NewInMemorySessionStore()}
			h := newHarness(t, WithSessionStore(store), WithConcurrencyLimits(ConcurrencyLimits{MaxActiveSessions: 1}))
			h.initialize()
			cwd := t.TempDir()
			created, err := h.conn.NewSession(h.ctx(), wire.NewSessionRequest(cwd))
			require.NoError(t, err)
			before, err := store.Load(h.ctx(), string(created.SessionId))
			require.NoError(t, err)
			option := wire.WithSessionMetaValue(map[string]any{"pi": map[string]any{"options": map[string]any{"env": map[string]string{"RESTORE_TEST": "changed"}}}})
			store.fail.Store(true)
			if method == acp.AgentMethodSessionLoad {
				_, err = h.conn.LoadSession(h.ctx(), wire.LoadSessionRequest(created.SessionId, cwd, option))
			} else {
				_, err = h.conn.ResumeSession(h.ctx(), wire.ResumeSessionRequest(created.SessionId, cwd, option))
			}
			store.fail.Store(false)
			require.Error(t, err, "store failure must fail restore")
			after, err := store.Load(h.ctx(), string(created.SessionId))
			require.NoError(t, err)
			require.Equal(t, before, after, "failed teardown must retain the durable generation")
			_, err = h.conn.NewSession(h.ctx(), wire.NewSessionRequest(t.TempDir()))
			require.NoError(t, err, "a failed restore-close leaked its active-session slot")
		})
	}
}
