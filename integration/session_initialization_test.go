//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	piacp "github.com/savid/acp-go-pi"
	"github.com/stretchr/testify/require"
)

func TestPiCLIColdResumeBeforeFirstPrompt(t *testing.T) {
	path := smokePiPath(t)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()

	cwd := integrationWorkspaceDir(t)
	cwd, err := filepath.EvalSymlinks(cwd)
	require.NoError(t, err)
	store := piacp.NewInMemorySessionStore()
	connect := func(store piacp.SessionStore) *acp.ClientSideConnection {
		return connectAgentForTest(t, ctx, &recordingClient{},
			piacp.WithExecutablePath(path), piacp.WithScratchDir(integrationScratchDir(t)),
			piacp.WithHome(t.TempDir()), piacp.WithSessionStore(store))
	}
	conn := connect(store)
	opened, err := conn.NewSession(ctx, piacp.NewSessionRequest(cwd))
	require.NoError(t, err)

	key := piacp.SessionKey{SessionID: string(opened.SessionId)}
	entries, err := store.Load(ctx, key)
	require.NoError(t, err)
	require.NotEmpty(t, entries, "native initialization must survive before a prompt or close")
	var header map[string]any
	require.NoError(t, json.Unmarshal(entries[0], &header))
	require.Equal(t, "session", header["type"])
	require.Equal(t, string(opened.SessionId), header["id"])
	require.Equal(t, cwd, header["cwd"])
	for _, entry := range entries {
		var row map[string]any
		require.NoError(t, json.Unmarshal(entry, &row))
		require.NotEqual(t, "message", row["type"])
	}

	snapshot := piacp.NewInMemorySessionStore()
	require.NoError(t, snapshot.Append(ctx, key, entries))
	subkeys, err := store.ListSubkeys(ctx, key)
	require.NoError(t, err)
	require.Contains(t, subkeys, piacp.SessionStoreLifecycleSubpath)
	for _, subpath := range subkeys {
		subkey := piacp.SessionKey{SessionID: key.SessionID, Subpath: subpath}
		rows, loadErr := store.Load(ctx, subkey)
		require.NoError(t, loadErr)
		require.NoError(t, snapshot.Append(ctx, subkey, rows))
	}
	_, err = conn.CloseSession(ctx, acp.CloseSessionRequest{SessionId: opened.SessionId})
	require.NoError(t, err)

	recovered := connect(snapshot)
	_, err = recovered.ResumeSession(ctx, piacp.ResumeSessionRequest(opened.SessionId, cwd))
	require.NoError(t, err)
	_, err = recovered.LoadSession(ctx, piacp.LoadSessionRequest(opened.SessionId, cwd))
	require.NoError(t, err)
	_, err = recovered.CloseSession(ctx, acp.CloseSessionRequest{SessionId: opened.SessionId})
	require.NoError(t, err)
	rows, err := snapshot.Load(ctx, key)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(rows), len(entries))
	require.Equal(t, entries, rows[:len(entries)])
	for _, entry := range rows[len(entries):] {
		var row map[string]any
		require.NoError(t, json.Unmarshal(entry, &row))
		require.Equal(t, "thinking_level_change", row["type"])
	}
}
