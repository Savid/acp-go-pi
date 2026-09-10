package piacp

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSessionInitializationBeforePublication(t *testing.T) {
	client := newStubPiClient()
	client.state.SessionID = "initialized"
	store := NewInMemorySessionStore()
	agent := newStubClientAgent(t, client, WithSessionStore(store))
	agent.setConnection(newDirectAgentClient())
	opened, err := agent.NewSession(t.Context(), NewSessionRequest(t.TempDir()))
	require.NoError(t, err)

	rows, err := store.Load(t.Context(), SessionKey{SessionID: string(opened.SessionId)})
	require.NoError(t, err)
	require.Len(t, rows, 1)
	boundary, found, err := agent.lastLifecycleBoundary(t.Context(), string(opened.SessionId))
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, 1, boundary.NativeRows)
	require.Equal(t, nativeStateCommitted, boundary.NativeState)
	require.NoError(t, agent.Close())
}

func TestSessionInitializationFailureDoesNotPublish(t *testing.T) {
	for _, subpath := range []string{SessionStoreMainSubpath, SessionStoreLifecycleSubpath} {
		t.Run("refuse "+subpath, func(t *testing.T) {
			client := newStubPiClient()
			client.state.SessionID = "initialized"
			want := errors.New("durable store unavailable")
			store := &initializationFailStore{SessionStore: NewInMemorySessionStore(), subpath: subpath, err: want}
			agent := newStubClientAgent(t, client, WithSessionStore(store))
			agent.setConnection(newDirectAgentClient())
			opened, err := agent.NewSession(t.Context(), NewSessionRequest(t.TempDir()))
			require.ErrorIs(t, err, want)
			require.Empty(t, opened.SessionId)
			require.Empty(t, agent.sessions)
			require.NoError(t, agent.Close())
		})
	}
}

type initializationFailStore struct {
	SessionStore
	subpath string
	err     error
}

func (s *initializationFailStore) Append(ctx context.Context, key SessionKey, rows []SessionStoreEntry) error {
	if key.Subpath == s.subpath {
		return s.err
	}

	return s.SessionStore.Append(ctx, key, rows)
}
