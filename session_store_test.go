package piacp

import (
	"context"
	"encoding/json"
	"testing"

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
