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

	require.Error(t, store.Replace(ctx, SessionKey{}, nil))
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

func TestSessionStoreAndValidationHelpers(t *testing.T) {
	require.True(t, validUUIDShape("01234567-89ab-cdef-0123-456789ABCDEF"))
	require.False(t, validUUIDShape("short"))
	require.False(t, validUUIDShape("01234567x89ab-cdef-0123-456789abcdef"))
	require.False(t, validUUIDShape("01234567-89ab-cdef-0123-456789abcdeg"))
	require.Equal(t, "second", firstNonEmptyString("", "second", "third"))
	require.Empty(t, firstNonEmptyString("", ""))

	require.Error(t, validateRequiredAbsolutePath("cwd", ""))
	require.Error(t, validateRequiredAbsolutePath("cwd", "relative"))
	require.NoError(t, validateRequiredAbsolutePath("cwd", "/absolute"))
	require.NoError(t, validateOptionalAbsolutePath("cwd", nil))
	empty := ""
	require.NoError(t, validateOptionalAbsolutePath("cwd", &empty))
	relative := "relative"
	require.Error(t, validateOptionalAbsolutePath("cwd", &relative))
	absolute := "/absolute"
	require.NoError(t, validateOptionalAbsolutePath("cwd", &absolute))
	require.Error(t, validateAbsolutePaths("paths", []string{""}))
	require.Error(t, validateAbsolutePaths("paths", []string{"relative"}))
	require.NoError(t, validateAbsolutePaths("paths", []string{"/one", "/two"}))
	require.Error(t, validateSessionStartPaths("", nil))
	require.Error(t, validateSessionStartPaths("/cwd", []string{"relative"}))
	require.NoError(t, validateSessionStartPaths("/cwd", []string{"/also"}))
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
