package piacp

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	acpcore "github.com/savid/acp-go-core"
	"github.com/savid/acp-go-core/sessionlog"
	"github.com/savid/acp-go-core/wire"
	"github.com/savid/acp-go-pi/internal/pi"
	"github.com/stretchr/testify/require"
)

func TestMirrorCommitsRowsAndRecord(t *testing.T) {
	t.Parallel()

	store := acpcore.NewInMemorySessionStore()
	h := newHarness(t, WithSessionStore(store))
	h.initialize()
	session := h.newSession()

	_, err := h.prompt(session.SessionId, "HELLO", nil)
	require.NoError(t, err)

	rows, err := loadEntries(context.Background(), store, acpcore.SessionKey{SessionID: string(session.SessionId)})
	require.NoError(t, err)
	require.Len(t, rows, 3)

	header, ok := pi.ParseHeader(rows[0])
	require.True(t, ok)
	require.Equal(t, string(session.SessionId), header.ID)

	records, err := loadEntries(context.Background(), store, acpcore.SessionKey{SessionID: string(session.SessionId), Subpath: configSubpath})
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

	resp, err := h.conn.LoadSession(h.ctx(), wire.LoadSessionRequest(session.SessionId, t.TempDir()))
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

	_, err = h.conn.ResumeSession(h.ctx(), wire.ResumeSessionRequest(session.SessionId, t.TempDir()))
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

	_, err = h.conn.ResumeSession(h.ctx(), wire.ResumeSessionRequest(session.SessionId, cwd))
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

	records, err := loadEntries(context.Background(), store, acpcore.SessionKey{SessionID: string(session.SessionId), Subpath: configSubpath})
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

	_, err = h.conn.LoadSession(h.ctx(), wire.LoadSessionRequest(session.SessionId, t.TempDir()))
	require.NoError(t, err)

	rows, err := loadEntries(context.Background(), store, acpcore.SessionKey{SessionID: string(session.SessionId)})
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

	records, err := loadEntries(context.Background(), store, acpcore.SessionKey{SessionID: string(session.SessionId), Subpath: configSubpath})
	require.NoError(t, err)

	var record sessionRecord
	require.NoError(t, json.Unmarshal(records[len(records)-1], &record))

	rows, err := pi.ReadRows(record.SessionFile)
	require.NoError(t, err)
	rows[1] = []byte(`{"type":"message","id":"x","parentId":null,"timestamp":"t","message":{"role":"user","content":"other"}}`)
	require.NoError(t, pi.WriteRows(record.SessionFile, rows))

	_, err = h.conn.ResumeSession(h.ctx(), wire.ResumeSessionRequest(session.SessionId, t.TempDir()))
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

	_, err := h.conn.LoadSession(h.ctx(), wire.LoadSessionRequest("00000000-0000-4000-8000-000000000000", t.TempDir()))
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
	before, err := loadEntries(t.Context(), store, acpcore.SessionKey{SessionID: string(session.SessionId)})
	require.NoError(t, err)
	_, err = h.conn.SetSessionConfigOption(h.ctx(), SetModelRequest(session.SessionId, "fake/text-only"))
	require.NoError(t, err)
	records, err := loadEntries(t.Context(), store, acpcore.SessionKey{SessionID: string(session.SessionId), Subpath: configSubpath})
	require.NoError(t, err)
	require.Len(t, records, 1)
	var record sessionRecord
	require.NoError(t, json.Unmarshal(records[0], &record))
	require.Equal(t, "fake/text-only", record.Model)
	after, err := loadEntries(t.Context(), store, acpcore.SessionKey{SessionID: string(session.SessionId)})
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
	rows, err := loadEntries(t.Context(), store, acpcore.SessionKey{SessionID: string(session.SessionId)})
	require.NoError(t, err)
	main := acpcore.SessionKey{SessionID: string(session.SessionId)}
	require.NoError(t, store.Replace(t.Context(), main, []acpcore.SessionStoreReplacement{
		{Key: main, Entries: rows},
		{Key: acpcore.SessionKey{SessionID: main.SessionID, Subpath: configSubpath}, Entries: []acpcore.SessionStoreEntry{[]byte(`{"sessionId":"wrong"}`)}},
	}))
	_, err = h.conn.LoadSession(h.ctx(), wire.LoadSessionRequest(session.SessionId, t.TempDir()))
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
	rows, err := loadEntries(t.Context(), store, acpcore.SessionKey{SessionID: string(session.SessionId), Subpath: configSubpath})
	require.NoError(t, err)
	require.Len(t, rows, 1)
	var record sessionRecord
	require.NoError(t, json.Unmarshal(rows[0], &record))
	require.True(t, filepath.IsAbs(record.SessionFile))
	require.Contains(t, record.SessionFile, filepath.Join(record.Cwd, "native-home"))
	require.FileExists(t, record.SessionFile)
}

func TestNewSessionFailsWhenInitialMirrorFails(t *testing.T) {
	t.Parallel()
	store := &mirrorFaultStore{SessionStore: acpcore.NewInMemorySessionStore()}
	store.fail.Store(true)
	h := newHarness(t, WithSessionStore(store))
	h.initialize()
	_, err := h.conn.NewSession(h.ctx(), wire.NewSessionRequest(t.TempDir()))
	require.Equal(t, "pi_internal_failure", requestErrorData(t, err)["error"])
	listed, err := h.conn.ListSessions(h.ctx(), wire.ListSessionsRequest())
	require.NoError(t, err)
	require.Empty(t, listed.Sessions)
	store.fail.Store(false)
	h.newSession()
}

type blockedLoadStore struct {
	acpcore.SessionStore
	block            atomic.Bool
	entered, release chan struct{}
}

func (s *blockedLoadStore) Load(ctx context.Context, sessionID string) (map[string][]acpcore.SessionStoreEntry, error) {
	rows, err := s.SessionStore.Load(ctx, sessionID)
	if err == nil && s.block.CompareAndSwap(true, false) {
		close(s.entered)
		select {
		case <-s.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	return rows, err
}

func TestConcurrentColdRestoreIsRefusedBeforeBinding(t *testing.T) {
	t.Parallel()
	store := &blockedLoadStore{SessionStore: acpcore.NewInMemorySessionStore(), entered: make(chan struct{}), release: make(chan struct{})}
	h := newHarness(t, WithSessionStore(store))
	h.initialize()
	cwd := t.TempDir()
	session, err := h.conn.NewSession(h.ctx(), wire.NewSessionRequest(cwd))
	require.NoError(t, err)
	_, err = h.prompt(session.SessionId, "HELLO", nil)
	require.NoError(t, err)
	_, err = h.conn.CloseSession(h.ctx(), acp.CloseSessionRequest{SessionId: session.SessionId})
	require.NoError(t, err)
	store.block.Store(true)
	done := make(chan error, 1)
	ctx := h.ctx()
	go func() {
		_, loadErr := h.conn.LoadSession(ctx, wire.LoadSessionRequest(session.SessionId, cwd))
		done <- loadErr
	}()
	select {
	case <-store.entered:
	case <-ctx.Done():
		t.Fatal("load did not reach configuration")
	}
	_, err = h.conn.ResumeSession(h.ctx(), wire.ResumeSessionRequest(session.SessionId, cwd))
	close(store.release)
	require.Equal(t, "session_restore", requestErrorData(t, err)["limit"])
	require.NoError(t, <-done)
	_, err = h.prompt(session.SessionId, "HELLO", nil)
	require.NoError(t, err)
}

func TestLiveResumeAppliesOptionsAndRetainsDirectories(t *testing.T) {
	t.Parallel()
	store := acpcore.NewInMemorySessionStore()
	h := newHarness(t, WithSessionStore(store))
	h.initialize()
	cwd, directory := t.TempDir(), t.TempDir()
	session, err := h.conn.NewSession(h.ctx(), wire.NewSessionRequest(cwd, wire.WithSessionAdditionalDirectories(directory), WithSessionPiOptions(NewPiOptions(WithPiAutoRetry(true)))))
	require.NoError(t, err)
	_, err = h.prompt(session.SessionId, "HELLO", nil)
	require.NoError(t, err)
	_, err = h.conn.ResumeSession(h.ctx(), wire.ResumeSessionRequest(session.SessionId, cwd,
		WithSessionPiOptions(NewPiOptions(WithPiModel("fake/text-only"), WithPiAutoRetry(false)))))
	require.NoError(t, err)
	rows, err := loadEntries(h.ctx(), store, acpcore.SessionKey{SessionID: string(session.SessionId), Subpath: configSubpath})
	require.NoError(t, err)
	require.Len(t, rows, 1)
	var record sessionRecord
	require.NoError(t, json.Unmarshal(rows[0], &record))
	require.Equal(t, "fake/text-only", record.Model)
	require.False(t, record.AutoRetry)
	require.Equal(t, []string{directory}, record.AdditionalDirectories)
	_, err = h.prompt(session.SessionId, "HELLO", nil)
	require.NoError(t, err)
}

func TestRestoreIntoDifferentNativeHome(t *testing.T) {
	t.Parallel()
	store := acpcore.NewInMemorySessionStore()
	h := newHarness(t, WithSessionStore(store))
	h.initialize()
	cwd := t.TempDir()
	session, err := h.conn.NewSession(h.ctx(), wire.NewSessionRequest(cwd))
	require.NoError(t, err)
	_, err = h.prompt(session.SessionId, "HELLO", nil)
	require.NoError(t, err)
	_, err = h.conn.CloseSession(h.ctx(), acp.CloseSessionRequest{SessionId: session.SessionId})
	require.NoError(t, err)
	home := t.TempDir()
	restored := newHarness(t, WithSessionStore(store), WithHome(home))
	restored.initialize()
	_, err = restored.conn.LoadSession(restored.ctx(), wire.LoadSessionRequest(session.SessionId, cwd))
	require.NoError(t, err)
	_, err = restored.prompt(session.SessionId, "HELLO", nil)
	require.NoError(t, err)
	rows, err := loadEntries(restored.ctx(), store, acpcore.SessionKey{SessionID: string(session.SessionId), Subpath: configSubpath})
	require.NoError(t, err)
	var record sessionRecord
	require.Len(t, rows, 1)
	require.NoError(t, json.Unmarshal(rows[0], &record))
	relative, err := filepath.Rel(home, record.SessionFile)
	require.NoError(t, err)
	require.True(t, filepath.IsLocal(relative))
	require.FileExists(t, record.SessionFile)
}

// An established conversation with no native rows commits an empty main record
// alongside its configuration, and restores from it.
func TestEmptyConversationCommitsAndRestores(t *testing.T) {
	t.Parallel()

	store := acpcore.NewInMemorySessionStore()
	agent := NewAgent(WithSessionStore(store), WithLogger(slog.New(slog.DiscardHandler)))
	cwd, home := t.TempDir(), t.TempDir()

	s := &session{
		agent:       agent,
		id:          "s1",
		nativeID:    "native-empty",
		cwd:         cwd,
		agentDir:    home,
		sessionFile: filepath.Join(home, "sessions", "empty.jsonl"),
	}
	require.NoError(t, s.commitMirror(t.Context()))

	generation, err := store.Load(t.Context(), "s1")
	require.NoError(t, err)

	rows, present := generation[acpcore.SessionStoreMainSubpath]
	require.True(t, present, "the empty main record is committed")
	require.Empty(t, rows)
	require.Len(t, generation[configSubpath], 1)

	stored, err := agent.loadStored(t.Context(), "s1")
	require.NoError(t, err)
	require.True(t, stored.found, "an empty native history is a found session, not a missing one")
	require.Empty(t, stored.rows)

	path, hydrated, err := agent.hydrate(t.Context(), "s1", stored, cwd, home)
	require.NoError(t, err)
	require.Equal(t, s.sessionFile, path, "the record's path is the identity a headerless generation has")
	require.Empty(t, hydrated)
}

// Residual native state with no store entry is neither listed nor adopted.
func TestResidualNativeStateIsNeverAdopted(t *testing.T) {
	t.Parallel()

	store := acpcore.NewInMemorySessionStore()
	home := filepath.Join(t.TempDir(), "home")
	cwd := t.TempDir()
	h := newHarness(t, WithSessionStore(store), WithHome(home))
	h.initialize()

	orphan := "00000000-0000-4000-8000-00000000abcd"
	path := pi.SessionFile(home, cwd, orphan, time.Now())
	require.NoError(t, pi.WriteRows(path, [][]byte{
		[]byte(`{"type":"session","version":3,"id":"` + orphan + `","timestamp":"2026-09-15T00:00:00Z","cwd":"` + cwd + `"}`),
	}))

	list, err := h.conn.ListSessions(h.ctx(), wire.ListSessionsRequest())
	require.NoError(t, err)
	require.Empty(t, list.Sessions)

	_, err = h.conn.LoadSession(h.ctx(), wire.LoadSessionRequest(acp.SessionId(orphan), cwd))
	require.Equal(t, "unknown session", requestErrorData(t, err)["error"])

	_, err = h.conn.ResumeSession(h.ctx(), wire.ResumeSessionRequest(acp.SessionId(orphan), cwd))
	require.Equal(t, "unknown session", requestErrorData(t, err)["error"])
}

// A delete that lands while a cold restore is in flight leaves nothing
// installed.
func TestDeleteDuringColdRestoreInstallsNothing(t *testing.T) {
	t.Parallel()

	store := &blockedLoadStore{SessionStore: acpcore.NewInMemorySessionStore(), entered: make(chan struct{}), release: make(chan struct{})}
	h := newHarness(t, WithSessionStore(store))
	h.initialize()
	cwd := t.TempDir()

	session, err := h.conn.NewSession(h.ctx(), wire.NewSessionRequest(cwd))
	require.NoError(t, err)
	_, err = h.prompt(session.SessionId, "HELLO", nil)
	require.NoError(t, err)
	_, err = h.conn.CloseSession(h.ctx(), acp.CloseSessionRequest{SessionId: session.SessionId})
	require.NoError(t, err)

	store.block.Store(true)

	done := make(chan error, 1)
	ctx := h.ctx()

	go func() {
		_, loadErr := h.conn.LoadSession(ctx, wire.LoadSessionRequest(session.SessionId, cwd))
		done <- loadErr
	}()

	select {
	case <-store.entered:
	case <-ctx.Done():
		t.Fatal("load did not reach the store")
	}

	_, err = h.conn.UnstableDeleteSession(h.ctx(), wire.DeleteSessionRequest(session.SessionId))
	require.NoError(t, err)
	close(store.release)

	require.Error(t, <-done, "a load past its entry check installs nothing once the id is tombstoned")

	_, err = h.prompt(session.SessionId, "HELLO", nil)
	require.Equal(t, "unknown session", requestErrorData(t, err)["error"])

	list, err := h.conn.ListSessions(h.ctx(), wire.ListSessionsRequest())
	require.NoError(t, err)
	require.Empty(t, list.Sessions)
}

type recoveryFaultStore struct {
	acpcore.SessionStore
	fail atomic.Bool
}

func (s *recoveryFaultStore) Replace(ctx context.Context, main acpcore.SessionKey, replacements []acpcore.SessionStoreReplacement) error {
	if s.fail.Load() {
		return errors.New("injected store failure")
	}

	return s.SessionStore.Replace(ctx, main, replacements)
}

func TestNativeBindingSurvivesLoadAndResume(t *testing.T) {
	t.Parallel()
	store := acpcore.NewInMemorySessionStore()
	h := newHarness(t, WithSessionStore(store))
	h.initialize()
	cwd := t.TempDir()
	created, err := h.conn.NewSession(h.ctx(), wire.NewSessionRequest(cwd))
	require.NoError(t, err)
	_, err = h.prompt(created.SessionId, "HELLO", nil)
	require.NoError(t, err)
	_, err = h.conn.CloseSession(h.ctx(), acp.CloseSessionRequest{SessionId: created.SessionId})
	require.NoError(t, err)
	var record sessionRecord
	rows, found, err := sessionlog.Load(t.Context(), store, string(created.SessionId), &record)
	require.NoError(t, err)
	require.True(t, found)
	require.NotEmpty(t, record.NativeSessionID)
	require.Equal(t, wire.NativeSessionMeta(vendor, record.NativeSessionID), created.Meta)
	id := acp.SessionId("acp-conversation-independent-of-native-id")
	record.SessionID = string(id)
	require.NoError(t, sessionlog.Commit(t.Context(), store, string(id), rows, record))
	require.NoError(t, store.Delete(t.Context(), acpcore.SessionKey{SessionID: string(created.SessionId)}))

	before := len(h.rec.snapshot())
	loaded, err := h.conn.LoadSession(h.ctx(), wire.LoadSessionRequest(id, cwd))
	require.NoError(t, err)
	require.Equal(t, wire.NativeSessionMeta(vendor, record.NativeSessionID), loaded.Meta)
	_, err = h.prompt(id, "HELLO", nil)
	require.NoError(t, err)
	for _, update := range h.rec.snapshot()[before:] {
		require.Equal(t, id, update.SessionId)
	}
	listed, err := h.conn.ListSessions(h.ctx(), wire.ListSessionsRequest())
	require.NoError(t, err)
	require.Len(t, listed.Sessions, 1)
	require.Equal(t, id, listed.Sessions[0].SessionId)
	require.Equal(t, loaded.Meta, listed.Sessions[0].Meta)
	_, err = h.conn.CloseSession(h.ctx(), acp.CloseSessionRequest{SessionId: id})
	require.NoError(t, err)
	listed, err = h.conn.ListSessions(h.ctx(), wire.ListSessionsRequest())
	require.NoError(t, err)
	require.Len(t, listed.Sessions, 1)
	require.Equal(t, loaded.Meta, listed.Sessions[0].Meta)
	resumed, err := h.conn.ResumeSession(h.ctx(), wire.ResumeSessionRequest(id, cwd))
	require.NoError(t, err)
	require.Equal(t, loaded.Meta, resumed.Meta)
	_, err = h.prompt(id, "HELLO", nil)
	require.NoError(t, err)
	var after sessionRecord
	_, found, err = sessionlog.Load(t.Context(), store, string(id), &after)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, record.NativeSessionID, after.NativeSessionID)
	require.Equal(t, string(id), after.SessionID)
}

type firstMirrorFailureStore struct {
	acpcore.SessionStore
	calls atomic.Int32
}

func (s *firstMirrorFailureStore) Replace(ctx context.Context, key acpcore.SessionKey, rows []acpcore.SessionStoreReplacement) error {
	if s.calls.Add(1) == 1 {
		return errors.New("initial mirror unavailable")
	}

	return s.SessionStore.Replace(ctx, key, rows)
}

func TestFailedNewSessionDoesNotPersistDuringCleanup(t *testing.T) {
	t.Parallel()
	store := &firstMirrorFailureStore{SessionStore: acpcore.NewInMemorySessionStore()}
	h := newHarness(t, WithSessionStore(store))
	h.initialize()
	response, err := h.conn.NewSession(h.ctx(), wire.NewSessionRequest(t.TempDir()))
	require.Error(t, err)
	require.Empty(t, response.SessionId)
	rows, err := store.ListSessions(h.ctx())
	require.NoError(t, err)
	require.Empty(t, rows)
	listed, err := h.conn.ListSessions(h.ctx(), wire.ListSessionsRequest())
	require.NoError(t, err)
	require.Empty(t, listed.Sessions)
}

type firstOpenFailureClient struct {
	*recorder
	failed atomic.Bool
}

func (c *firstOpenFailureClient) SessionUpdate(ctx context.Context, notification acp.SessionNotification) error {
	if c.failed.CompareAndSwap(false, true) {
		return errors.New("initial publication unavailable")
	}

	return c.recorder.SessionUpdate(ctx, notification)
}

func TestFailedSessionOpenReleasesActiveSlot(t *testing.T) {
	t.Parallel()
	a := NewAgent(testOptions(t, WithConcurrencyLimits(ConcurrencyLimits{MaxActiveSessions: 1}))...)
	t.Cleanup(func() { _ = a.Close() })
	a.attach(&firstOpenFailureClient{recorder: newRecorder()}, nil)
	request := acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber}
	withLifecycle()(&request)
	_, err := a.Initialize(t.Context(), request)
	require.NoError(t, err)
	first, err := a.NewSession(t.Context(), wire.NewSessionRequest(t.TempDir()))
	require.Error(t, err)
	require.Empty(t, first.SessionId)
	_, err = a.NewSession(t.Context(), wire.NewSessionRequest(t.TempDir()))
	require.NoError(t, err)
}
