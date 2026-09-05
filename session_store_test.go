package piacp

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"
)

func TestInMemorySessionStoreCompleteSemantics(t *testing.T) {
	ctx := t.Context()
	main := SessionKey{SessionID: "one"}
	sub := SessionKey{SessionID: "one", Subpath: "branch"}
	other := SessionKey{SessionID: "two"}
	store := NewInMemorySessionStore()

	require.NoError(t, store.Append(ctx, main, nil))
	require.Error(t, store.Append(ctx, SessionKey{}, []SessionStoreEntry{json.RawMessage(`{}`)}))
	entry := json.RawMessage(`{"type":"session"}`)
	require.NoError(t, store.Append(ctx, main, []SessionStoreEntry{entry}))
	require.NoError(t, store.Append(ctx, sub, []SessionStoreEntry{json.RawMessage(`{"sub":true}`)}))
	require.NoError(t, store.Append(ctx, other, []SessionStoreEntry{json.RawMessage(`{"other":true}`)}))
	entry[0] = 'X'

	loaded, err := store.Load(ctx, main)
	require.NoError(t, err)
	require.JSONEq(t, `{"type":"session"}`, string(loaded[0]))
	loaded[0][0] = 'X'
	reloaded, err := store.Load(ctx, main)
	require.NoError(t, err)
	require.JSONEq(t, `{"type":"session"}`, string(reloaded[0]))

	subkeys, err := store.ListSubkeys(ctx, main)
	require.NoError(t, err)
	require.Equal(t, []string{"branch"}, subkeys)
	summaries, err := store.ListSessions(ctx)
	require.NoError(t, err)
	require.Len(t, summaries, 2)

	require.EqualError(t, store.Replace(ctx, SessionKey{}, nil), "session id is required")
	require.EqualError(t, store.Replace(ctx, SessionKey{SessionID: "id", Subpath: "not-main"}, nil), "main key must use the main subpath")
	require.Error(t, store.Replace(ctx, main, nil))
	require.Error(t, store.Replace(ctx, main, []SessionStoreReplacement{{Key: other}}))
	require.Error(t, store.Replace(ctx, main, []SessionStoreReplacement{{Key: main}, {Key: main}}))
	require.NoError(t, store.Replace(ctx, main, []SessionStoreReplacement{
		{Key: main, Entries: []SessionStoreEntry{json.RawMessage(`{"replacement":true}`)}},
		{Key: sub, Entries: []SessionStoreEntry{json.RawMessage(`{"replacementSub":true}`)}},
	}))

	require.NoError(t, store.Delete(ctx, SessionKey{}))
	require.NoError(t, store.Delete(ctx, sub))
	subkeys, err = store.ListSubkeys(ctx, main)
	require.NoError(t, err)
	require.Empty(t, subkeys)
	require.NoError(t, store.Delete(ctx, main))
	loaded, err = store.Load(ctx, sub)
	require.NoError(t, err)
	require.Nil(t, loaded)
	require.NoError(t, store.Append(ctx, sub, []SessionStoreEntry{json.RawMessage(`{}`)}))
	loaded, err = store.Load(ctx, sub)
	require.NoError(t, err)
	require.Nil(t, loaded)
}

func TestInMemorySessionStoreNilAndContextErrors(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	store := NewInMemorySessionStore()
	key := SessionKey{SessionID: "id"}
	require.Error(t, store.Append(ctx, key, []SessionStoreEntry{json.RawMessage(`{}`)}))
	_, err := store.Load(ctx, key)
	require.Error(t, err)
	require.Error(t, store.Replace(ctx, key, nil))
	require.Error(t, store.Delete(ctx, key))
	_, err = store.ListSessions(ctx)
	require.Error(t, err)
	_, err = store.ListSubkeys(ctx, key)
	require.Error(t, err)

	var nilStore *InMemorySessionStore
	require.Error(t, nilStore.Append(t.Context(), key, []SessionStoreEntry{json.RawMessage(`{}`)}))
	_, err = nilStore.Load(t.Context(), key)
	require.Error(t, err)
	require.Error(t, nilStore.Replace(t.Context(), key, nil))
	require.Error(t, nilStore.Delete(t.Context(), key))
	_, err = nilStore.ListSessions(t.Context())
	require.Error(t, err)
	_, err = nilStore.ListSubkeys(t.Context(), key)
	require.Error(t, err)

	zero := &InMemorySessionStore{}
	require.NoError(t, zero.Append(t.Context(), key, []SessionStoreEntry{nil}))
	loaded, err := zero.Load(t.Context(), key)
	require.NoError(t, err)
	require.Len(t, loaded, 1)
	require.Nil(t, loaded[0])
}

func TestListSessionsSortsByUpdatedTime(t *testing.T) {
	store := &InMemorySessionStore{
		entries: map[SessionKey][]SessionStoreEntry{
			{SessionID: "older"}: {json.RawMessage(`{}`)},
			{SessionID: "newer"}: {json.RawMessage(`{}`)},
		},
		updatedAt: map[SessionKey]int64{
			{SessionID: "older"}: 1,
			{SessionID: "newer"}: 2,
		},
		tombstone: map[SessionKey]struct{}{},
	}

	summaries, err := store.ListSessions(t.Context())
	require.NoError(t, err)
	require.Equal(t, "newer", summaries[0].SessionID)
}

func TestListSessionsFallsBackToSessionIDOrder(t *testing.T) {
	store := &InMemorySessionStore{
		entries: map[SessionKey][]SessionStoreEntry{
			{SessionID: "b"}: {json.RawMessage(`{}`)},
			{SessionID: "a"}: {json.RawMessage(`{}`)},
		},
		updatedAt: map[SessionKey]int64{},
		tombstone: map[SessionKey]struct{}{},
	}
	summaries, err := store.ListSessions(t.Context())
	require.NoError(t, err)
	require.Equal(t, "a", summaries[0].SessionID)
}

// TestReplaceIsOneSessionsGeneration is the store-contract conformance proof
// that every Replace is exactly one session's. Both refusals name the offending
// key, and both land before the store lock is taken, so a rejected call writes
// nothing at all — not even the replacements it had already accepted when it
// reached the bad one.
func TestReplaceIsOneSessionsGeneration(t *testing.T) {
	ctx := t.Context()
	main := SessionKey{SessionID: "one"}
	sub := SessionKey{SessionID: "one", Subpath: SessionStoreLifecycleSubpath}
	foreign := SessionKey{SessionID: "two", Subpath: "branch"}
	foreignMain := SessionKey{SessionID: "two"}

	store := NewInMemorySessionStore()
	require.NoError(t, store.Append(ctx, main, []SessionStoreEntry{json.RawMessage(`{"row":"main-original"}`)}))
	require.NoError(t, store.Append(ctx, sub, []SessionStoreEntry{json.RawMessage(`{"row":"sub-original"}`)}))
	require.NoError(t, store.Append(ctx, foreign, []SessionStoreEntry{json.RawMessage(`{"row":"foreign-original"}`)}))
	require.NoError(t, store.Append(ctx, foreignMain, []SessionStoreEntry{json.RawMessage(`{"row":"foreign-main"}`)}))

	unchanged := func(t *testing.T, label string) {
		t.Helper()

		for key, want := range map[SessionKey]string{
			main:        `{"row":"main-original"}`,
			sub:         `{"row":"sub-original"}`,
			foreign:     `{"row":"foreign-original"}`,
			foreignMain: `{"row":"foreign-main"}`,
		} {
			loaded, err := store.Load(ctx, key)
			require.NoError(t, err)
			require.Len(t, loaded, 1, "%s wrote key %v", label, key)
			require.JSONEq(t, want, string(loaded[0]), "%s wrote key %v", label, key)
		}
	}

	// A replacement addressed to another session is refused naming that exact
	// key, including its subpath: one session's commit may never rewrite a
	// second session's rows under a single atomic generation.
	require.EqualError(t, store.Replace(ctx, main, []SessionStoreReplacement{
		{Key: main, Entries: []SessionStoreEntry{json.RawMessage(`{"row":"main-new"}`)}},
		{Key: sub, Entries: []SessionStoreEntry{json.RawMessage(`{"row":"sub-new"}`)}},
		{Key: foreign, Entries: []SessionStoreEntry{json.RawMessage(`{"row":"foreign-new"}`)}},
	}), `replacement key "two" subpath "branch" does not belong to session "one"`)
	unchanged(t, "the cross-session refusal")

	// The same rule with no subpath, so the refusal is about the session id
	// rather than about the subpath happening to differ.
	require.EqualError(t, store.Replace(ctx, main, []SessionStoreReplacement{
		{Key: main}, {Key: foreignMain},
	}), `replacement key "two" subpath "" does not belong to session "one"`)
	unchanged(t, "the cross-session refusal without a subpath")

	// A duplicate {SessionID, Subpath} is refused naming that key, on both a
	// subkey and the main key.
	require.EqualError(t, store.Replace(ctx, main, []SessionStoreReplacement{
		{Key: main, Entries: []SessionStoreEntry{json.RawMessage(`{"row":"main-new"}`)}},
		{Key: sub, Entries: []SessionStoreEntry{json.RawMessage(`{"row":"first"}`)}},
		{Key: sub, Entries: []SessionStoreEntry{json.RawMessage(`{"row":"last"}`)}},
	}), `duplicate replacement key "one" subpath "lifecycle"`)
	unchanged(t, "the duplicate-subkey refusal")

	require.EqualError(t, store.Replace(ctx, main, []SessionStoreReplacement{
		{Key: main, Entries: []SessionStoreEntry{json.RawMessage(`{"row":"main-new"}`)}},
		{Key: main, Entries: []SessionStoreEntry{json.RawMessage(`{"row":"main-newer"}`)}},
	}), `duplicate replacement key "one" subpath ""`)
	unchanged(t, "the duplicate-main refusal")

	// A well-formed call over the same keys still commits, so the refusals
	// above are the two rules rather than an unconditional failure.
	require.NoError(t, store.Replace(ctx, main, []SessionStoreReplacement{
		{Key: main, Entries: []SessionStoreEntry{json.RawMessage(`{"row":"main-new"}`)}},
		{Key: sub, Entries: []SessionStoreEntry{json.RawMessage(`{"row":"sub-new"}`)}},
	}))

	loaded, err := store.Load(ctx, main)
	require.NoError(t, err)
	require.JSONEq(t, `{"row":"main-new"}`, string(loaded[0]))

	loaded, err = store.Load(ctx, foreign)
	require.NoError(t, err)
	require.JSONEq(t, `{"row":"foreign-original"}`, string(loaded[0]),
		"an accepted replacement touched another session")
}

// TestReplaceRefusesDuplicateReplacementKeys pins that two replacements naming
// one key are refused rather than silently resolved. Each names a whole
// generation of that key and nothing in the call says which one the caller
// meant, so the refusal lands before any key of the call is written.
func TestReplaceRefusesDuplicateReplacementKeys(t *testing.T) {
	store := NewInMemorySessionStore()
	main := SessionKey{SessionID: "one"}
	sub := SessionKey{SessionID: "one", Subpath: "branch"}

	require.NoError(t, store.Append(t.Context(), sub, []SessionStoreEntry{json.RawMessage(`{"row":"original"}`)}))

	require.EqualError(t, store.Replace(t.Context(), main, []SessionStoreReplacement{
		{Key: main, Entries: []SessionStoreEntry{json.RawMessage(`{"row":"main"}`)}},
		{Key: sub, Entries: []SessionStoreEntry{json.RawMessage(`{"row":"first"}`)}},
		{Key: sub, Entries: []SessionStoreEntry{json.RawMessage(`{"row":"last"}`)}},
	}), `duplicate replacement key "one" subpath "branch"`)

	loaded, err := store.Load(t.Context(), sub)
	require.NoError(t, err)
	require.Len(t, loaded, 1)
	require.JSONEq(t, `{"row":"original"}`, string(loaded[0]), "a refused replacement installed a generation anyway")

	loaded, err = store.Load(t.Context(), main)
	require.NoError(t, err)
	require.Empty(t, loaded, "a refused replacement wrote the main key")

	require.EqualError(t, store.Replace(t.Context(), main, []SessionStoreReplacement{
		{Key: main}, {Key: main},
	}), `duplicate replacement key "one" subpath ""`)
}

// TestReplaceAfterDeleteStaysTombstoned pins that a delete is final: a
// replacement landing after it installs nothing, so no session the host was
// told is gone can answer again.
func TestReplaceAfterDeleteStaysTombstoned(t *testing.T) {
	store := NewInMemorySessionStore()
	main := SessionKey{SessionID: "one"}
	sub := SessionKey{SessionID: "one", Subpath: SessionStoreLifecycleSubpath}

	require.NoError(t, store.Append(t.Context(), main, []SessionStoreEntry{json.RawMessage(`{}`)}))
	require.NoError(t, store.Delete(t.Context(), main))
	require.NoError(t, store.Replace(t.Context(), main, []SessionStoreReplacement{
		{Key: main, Entries: []SessionStoreEntry{json.RawMessage(`{"replacement":true}`)}},
		{Key: sub, Entries: []SessionStoreEntry{json.RawMessage(`{}`)}},
	}))

	loaded, err := store.Load(t.Context(), main)
	require.NoError(t, err)
	require.Empty(t, loaded)
	loaded, err = store.Load(t.Context(), sub)
	require.NoError(t, err)
	require.Empty(t, loaded)
}

func TestStoreStartedSessionRecheckEdges(t *testing.T) {
	id := acp.SessionId("replacement")

	for name, mutate := range map[string]func(*Agent, *agentSession){
		"closed":  func(agent *Agent, _ *agentSession) { agent.closed = true },
		"deleted": func(agent *Agent, _ *agentSession) { agent.deleted[id] = struct{}{} },
		"changed": func(agent *Agent, previous *agentSession) {
			agent.sessions[id] = &agentSession{agent: agent, id: previous.id}
		},
	} {
		t.Run(name, func(t *testing.T) {
			agent := NewAgent()
			process := newStubProcess(false)
			previous := attachTestNativeBoundary(&agentSession{
				agent: agent, id: id, proc: process, sessionRoot: t.TempDir(),
			})
			process.closeFunc = func() error {
				agent.mu.Lock()
				mutate(agent, previous)
				agent.mu.Unlock()

				return nil
			}
			agent.sessions[id] = previous
			replacement := &agentSession{agent: agent, id: id}
			require.Error(t, agent.storeStartedSession(t.Context(), replacement))
		})
	}

	cancelledCtx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := NewAgent().restoreSession(cancelledCtx, id, sessionStart{Cwd: testCwd}, nil)
	require.ErrorIs(t, err, context.Canceled)

	agent := NewAgent()
	agent.sessions[id] = &agentSession{agent: agent, id: id, configuration: sessionConfigurationRecord{
		ExtraPathDirs: []string{"relative"},
	}}
	_, err = agent.restoreSession(t.Context(), id, sessionStart{Cwd: testCwd}, nil)
	require.Error(t, err)
}
