package piacp

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMirrorCommitAndRetryBranches(t *testing.T) {
	base := NewInMemorySessionStore()
	controlled := &appendControlledStore{SessionStore: base, failures: 1, err: errors.New("temporary")}
	agent := NewAgent(WithSessionStore(controlled), WithLogger(slog.New(slog.DiscardHandler)))
	path := filepath.Join(t.TempDir(), "session.jsonl")
	require.NoError(t, os.WriteFile(path, []byte(" \n{\"one\":1}\n\n{\"two\":2}\n"), 0o600))
	session := &agentSession{agent: agent, id: "id", sessionFilePath: path}
	require.NoError(t, session.commitMirror(t.Context()))
	require.Equal(t, 2, session.mirroredRows)
	require.GreaterOrEqual(t, controlled.calls, 2)
	require.NoError(t, session.commitMirror(t.Context()))

	session.sessionFilePath = filepath.Join(t.TempDir(), "missing")
	require.NoError(t, session.commitMirror(t.Context()))
	dir := t.TempDir()
	session.sessionFilePath = dir
	require.Error(t, session.commitMirror(t.Context()))
	session.sessionFilePath = ""
	require.NoError(t, session.commitMirror(t.Context()))

	controlled = &appendControlledStore{SessionStore: base, failures: 10, err: errors.New("persistent")}
	agent.options.SessionStore = controlled
	session.sessionFilePath = path
	session.mirroredRows = 0
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.Error(t, session.commitMirror(ctx))

	previousTimeout := sessionMirrorAppendTimeout
	sessionMirrorAppendTimeout = time.Nanosecond
	t.Cleanup(func() { sessionMirrorAppendTimeout = previousTimeout })
	require.Error(t, appendMirrorEntries(t.Context(), controlled, SessionKey{SessionID: "id"}, []SessionStoreEntry{json.RawMessage(`{}`)}))
	require.Len(t, splitJSONLRows([]byte("\n a \n\n b\n")), 2)
}
