package piacp

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"

	acpcore "github.com/savid/acp-go-core"
	"github.com/savid/acp-go-pi/internal/pi"
)

func TestMirrorCommitsRowsAndRecord(t *testing.T) {
	t.Parallel()

	store := acpcore.NewInMemorySessionStore()
	h := newHarness(t, WithSessionStore(store))
	h.initialize()
	session := h.newSession()

	_, err := h.prompt(session.SessionId, "HELLO", nil)
	require.NoError(t, err)

	rows, err := store.Load(context.Background(), acpcore.SessionKey{SessionID: string(session.SessionId)})
	require.NoError(t, err)
	require.Len(t, rows, 3)

	header, ok := pi.ParseHeader(rows[0])
	require.True(t, ok)
	require.Equal(t, string(session.SessionId), header.ID)

	records, err := store.Load(context.Background(), acpcore.SessionKey{SessionID: string(session.SessionId), Subpath: configSubpath})
	require.NoError(t, err)
	require.NotEmpty(t, records)

	var record sessionRecord
	require.NoError(t, json.Unmarshal(records[len(records)-1], &record))
	require.Equal(t, string(session.SessionId), record.SessionID)
	require.FileExists(t, record.SessionFile)
	require.Equal(t, "fake/vision", record.Model)
}

func TestLoadReplaysAndResumeDoesNot(t *testing.T) {
	t.Parallel()

	store := acpcore.NewInMemorySessionStore()
	h := newHarness(t, WithSessionStore(store))
	h.initialize()
	session := h.newSession()

	_, err := h.prompt(session.SessionId, "TOOL", nil)
	require.NoError(t, err)

	_, err = h.conn.CloseSession(h.ctx(), acp.CloseSessionRequest{SessionId: session.SessionId})
	require.NoError(t, err)

	before := len(h.rec.snapshot())

	resp, err := h.conn.LoadSession(h.ctx(), LoadSessionRequest(session.SessionId, t.TempDir()))
	require.NoError(t, err)
	require.NotEmpty(t, resp.ConfigOptions)

	replayed := h.rec.snapshot()[before:]

	var userText, agent string

	toolStatuses := 0

	for _, update := range replayed {
		switch {
		case update.Update.UserMessageChunk != nil:
			userText += update.Update.UserMessageChunk.Content.Text.Text
		case update.Update.AgentMessageChunk != nil:
			agent += update.Update.AgentMessageChunk.Content.Text.Text
		case update.Update.ToolCallUpdate != nil:
			toolStatuses++
		}
	}

	require.Equal(t, "TOOL", userText)
	require.Equal(t, "done", agent)
	require.Equal(t, 1, toolStatuses)

	_, err = h.conn.CloseSession(h.ctx(), acp.CloseSessionRequest{SessionId: session.SessionId})
	require.NoError(t, err)

	before = len(h.rec.snapshot())

	_, err = h.conn.ResumeSession(h.ctx(), ResumeSessionRequest(session.SessionId, t.TempDir()))
	require.NoError(t, err)

	for _, update := range h.rec.snapshot()[before:] {
		require.Nil(t, update.Update.UserMessageChunk)
		require.Nil(t, update.Update.AgentMessageChunk)
	}

	_, err = h.prompt(session.SessionId, "HELLO", nil)
	require.NoError(t, err)
}

func TestHydrateMaterializesFromStore(t *testing.T) {
	t.Parallel()

	store := acpcore.NewInMemorySessionStore()
	home := filepath.Join(t.TempDir(), "home")
	h := newHarness(t, WithSessionStore(store), WithHome(home))
	h.initialize()
	session := h.newSession()

	_, err := h.prompt(session.SessionId, "HELLO", nil)
	require.NoError(t, err)

	_, err = h.conn.CloseSession(h.ctx(), acp.CloseSessionRequest{SessionId: session.SessionId})
	require.NoError(t, err)

	require.NoError(t, os.RemoveAll(filepath.Join(home, "sessions")))

	cwd := t.TempDir()

	_, err = h.conn.ResumeSession(h.ctx(), ResumeSessionRequest(session.SessionId, cwd))
	require.NoError(t, err)

	matches, err := filepath.Glob(filepath.Join(pi.SessionDir(home, cwd), "*_"+string(session.SessionId)+".jsonl"))
	require.NoError(t, err)
	require.Len(t, matches, 1)

	rows, err := pi.ReadRows(matches[0])
	require.NoError(t, err)
	require.Len(t, rows, 3)

	_, err = h.prompt(session.SessionId, "HELLO", nil)
	require.NoError(t, err)
}

func TestHydrateAdoptsNativeRowsWrittenOutsideACP(t *testing.T) {
	t.Parallel()

	store := acpcore.NewInMemorySessionStore()
	h := newHarness(t, WithSessionStore(store))
	h.initialize()
	session := h.newSession()

	_, err := h.prompt(session.SessionId, "HELLO", nil)
	require.NoError(t, err)

	_, err = h.conn.CloseSession(h.ctx(), acp.CloseSessionRequest{SessionId: session.SessionId})
	require.NoError(t, err)

	records, err := store.Load(context.Background(), acpcore.SessionKey{SessionID: string(session.SessionId), Subpath: configSubpath})
	require.NoError(t, err)

	var record sessionRecord
	require.NoError(t, json.Unmarshal(records[len(records)-1], &record))

	native := `{"type":"message","id":"n1","parentId":"e3","timestamp":"2026-09-11T00:00:00Z","message":{"role":"user","content":[{"type":"text","text":"typed in a shell"}],"timestamp":1}}`
	file, err := os.OpenFile(record.SessionFile, os.O_APPEND|os.O_WRONLY, 0o600)
	require.NoError(t, err)
	_, err = file.WriteString(native + "\n")
	require.NoError(t, err)
	require.NoError(t, file.Close())

	before := len(h.rec.snapshot())

	_, err = h.conn.LoadSession(h.ctx(), LoadSessionRequest(session.SessionId, t.TempDir()))
	require.NoError(t, err)

	rows, err := store.Load(context.Background(), acpcore.SessionKey{SessionID: string(session.SessionId)})
	require.NoError(t, err)
	require.Len(t, rows, 4)
	require.JSONEq(t, native, string(rows[3]))

	var userText strings.Builder

	for _, update := range h.rec.snapshot()[before:] {
		if update.Update.UserMessageChunk != nil {
			userText.WriteString(update.Update.UserMessageChunk.Content.Text.Text + "|")
		}
	}

	require.Equal(t, "HELLO|typed in a shell|", userText.String())
}

func TestHydrateDisagreementFailsRestore(t *testing.T) {
	t.Parallel()

	store := acpcore.NewInMemorySessionStore()
	h := newHarness(t, WithSessionStore(store))
	h.initialize()
	session := h.newSession()

	_, err := h.prompt(session.SessionId, "HELLO", nil)
	require.NoError(t, err)

	_, err = h.conn.CloseSession(h.ctx(), acp.CloseSessionRequest{SessionId: session.SessionId})
	require.NoError(t, err)

	records, err := store.Load(context.Background(), acpcore.SessionKey{SessionID: string(session.SessionId), Subpath: configSubpath})
	require.NoError(t, err)

	var record sessionRecord
	require.NoError(t, json.Unmarshal(records[len(records)-1], &record))

	rows, err := pi.ReadRows(record.SessionFile)
	require.NoError(t, err)
	rows[1] = []byte(`{"type":"message","id":"x","parentId":null,"timestamp":"t","message":{"role":"user","content":"other"}}`)
	require.NoError(t, pi.WriteRows(record.SessionFile, rows))

	_, err = h.conn.ResumeSession(h.ctx(), ResumeSessionRequest(session.SessionId, t.TempDir()))
	require.Equal(t, -32603, requestErrorCode(t, err))
	require.Equal(t, "pi_restore_failed", requestErrorData(t, err)["error"])
}

func TestStoredTitle(t *testing.T) {
	t.Parallel()

	rows := [][]byte{
		[]byte(`{"type":"session","version":3,"id":"abc","timestamp":"t","cwd":"/work"}`),
		[]byte(`{"type":"message","id":"1","message":{"role":"assistant","content":[{"type":"text","text":"hi"}]}}`),
		[]byte(`{"type":"message","id":"2","message":{"role":"user","content":"  ask   me "}}`),
	}

	require.Equal(t, "ask me", storedTitle("abc", rows))
	require.Equal(t, "abc", storedTitle("abc", rows[:2]))
}

func TestLoadUnknownSession(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	h.initialize()

	_, err := h.conn.LoadSession(h.ctx(), LoadSessionRequest("00000000-0000-4000-8000-000000000000", t.TempDir()))
	require.Equal(t, "unknown session", requestErrorData(t, err)["error"])
}

func TestConfigurationCommitsWithoutNewNativeRows(t *testing.T) {
	t.Parallel()
	store := acpcore.NewInMemorySessionStore()
	h := newHarness(t, WithSessionStore(store))
	h.initialize()
	session := h.newSession()
	_, err := h.prompt(session.SessionId, "HELLO", nil)
	require.NoError(t, err)
	before, err := store.Load(t.Context(), acpcore.SessionKey{SessionID: string(session.SessionId)})
	require.NoError(t, err)
	_, err = h.conn.SetSessionConfigOption(h.ctx(), SetModelRequest(session.SessionId, "fake/text-only"))
	require.NoError(t, err)
	records, err := store.Load(t.Context(), acpcore.SessionKey{SessionID: string(session.SessionId), Subpath: configSubpath})
	require.NoError(t, err)
	require.Len(t, records, 1)
	var record sessionRecord
	require.NoError(t, json.Unmarshal(records[0], &record))
	require.Equal(t, "fake/text-only", record.Model)
	after, err := store.Load(t.Context(), acpcore.SessionKey{SessionID: string(session.SessionId)})
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(after), len(before))
}

func TestMalformedStoreRecordFailsRestore(t *testing.T) {
	t.Parallel()
	store := acpcore.NewInMemorySessionStore()
	h := newHarness(t, WithSessionStore(store))
	h.initialize()
	session := h.newSession()
	_, err := h.prompt(session.SessionId, "HELLO", nil)
	require.NoError(t, err)
	_, err = h.conn.CloseSession(h.ctx(), acp.CloseSessionRequest{SessionId: session.SessionId})
	require.NoError(t, err)
	rows, err := store.Load(t.Context(), acpcore.SessionKey{SessionID: string(session.SessionId)})
	require.NoError(t, err)
	main := acpcore.SessionKey{SessionID: string(session.SessionId)}
	require.NoError(t, store.Replace(t.Context(), main, []acpcore.SessionStoreReplacement{
		{Key: main, Entries: rows},
		{Key: acpcore.SessionKey{SessionID: main.SessionID, Subpath: configSubpath}, Entries: []acpcore.SessionStoreEntry{[]byte(`{"sessionId":"wrong"}`)}},
	}))
	_, err = h.conn.LoadSession(h.ctx(), LoadSessionRequest(session.SessionId, t.TempDir()))
	require.Equal(t, "pi_restore_failed", requestErrorData(t, err)["error"])
}

type mirrorFaultStore struct {
	acpcore.SessionStore
	fail atomic.Bool
}

func (s *mirrorFaultStore) Replace(ctx context.Context, key acpcore.SessionKey, replacements []acpcore.SessionStoreReplacement) error {
	if s.fail.Load() {
		return errors.New("mirror unavailable")
	}

	return s.SessionStore.Replace(ctx, key, replacements)
}

func TestMirrorFailureFencesTurnAndAllowsRetry(t *testing.T) {
	t.Parallel()
	store := &mirrorFaultStore{SessionStore: acpcore.NewInMemorySessionStore()}
	h := newHarness(t, WithSessionStore(store))
	h.initialize(withLifecycle())
	session := h.newSession()
	store.fail.Store(true)
	_, err := h.prompt(session.SessionId, "HELLO", promptMeta(1))
	require.Equal(t, "pi_turn_failed", requestErrorData(t, err)["error"])
	types := eventTypes(lifecycleEvents(h.rec.snapshot()))
	require.Equal(t, []string{"lifecycle_snapshot", "prompt_accepted", "state_update:running"}, types)
	store.fail.Store(false)
	_, err = h.prompt(session.SessionId, "HELLO", promptMeta(2))
	require.NoError(t, err)
	types = eventTypes(lifecycleEvents(h.rec.snapshot()))
	require.Equal(t, []string{"lifecycle_snapshot", "prompt_accepted", "state_update:running", "lifecycle_snapshot", "prompt_accepted", "state_update:running", "state_update:idle"}, types)
}

func TestRelativeNativeHomeUsesSessionCwd(t *testing.T) {
	t.Parallel()
	store := acpcore.NewInMemorySessionStore()
	h := newHarness(t, WithHome(""), WithSessionStore(store), WithEnv(map[string]string{fakePiEnv: "1", pi.EnvAgentDir: "native-home"}))
	h.initialize()
	session := h.newSession()
	_, err := h.prompt(session.SessionId, "HELLO", nil)
	require.NoError(t, err)
	rows, err := store.Load(t.Context(), acpcore.SessionKey{SessionID: string(session.SessionId), Subpath: "config"})
	require.NoError(t, err)
	require.Len(t, rows, 1)
	var record sessionRecord
	require.NoError(t, json.Unmarshal(rows[0], &record))
	require.True(t, filepath.IsAbs(record.SessionFile))
	require.Contains(t, record.SessionFile, filepath.Join(record.Cwd, "native-home"))
	require.FileExists(t, record.SessionFile)
}
