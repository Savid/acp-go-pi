package piacp

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"

	"github.com/savid/acp-go-pi/internal/pi"
)

func TestForkExtensionValidationAndConversionBranches(t *testing.T) {
	converted := stableMCPServers(unstableMCPServersFromStable([]acp.McpServer{
		HTTPMCPServer("http", "https://example.test/mcp", map[string]string{"X": "Y"}),
		{Sse: &acp.McpServerSseInline{Name: "sse", Url: "https://example.test/sse"}},
		{Acp: &acp.McpServerAcpInline{Name: "acp", Id: "server"}},
		StdioMCPServer("stdio", "tool", []string{"serve"}, map[string]string{"A": "B"}),
		{},
	}))
	require.Len(t, converted, 5)
	require.NotNil(t, converted[0].Http)
	require.NotNil(t, converted[1].Sse)
	require.NotNil(t, converted[2].Acp)
	require.NotNil(t, converted[3].Stdio)
	require.Equal(t, acp.McpServer{}, converted[4])

	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	_, err := agent.handleForkSession(t.Context(), json.RawMessage(`{`))
	requireInvalidParams(t, err)

	_, err = agent.handleForkSession(t.Context(), json.RawMessage(`{}`))
	requireInvalidParams(t, err)

	params := forkParams(t)
	params.Meta = map[string]any{piMetaKey: "invalid"}
	_, err = agent.handleForkSession(t.Context(), forkRaw(t, params))
	requireInvalidParams(t, err)

	params = forkParams(t)
	params.Cwd = "relative"
	_, err = agent.handleForkSession(t.Context(), forkRaw(t, params))
	requireInvalidParams(t, err)

	params = forkParams(t)
	agent.deleted[params.SessionId] = struct{}{}
	_, err = agent.handleForkSession(t.Context(), forkRaw(t, params))
	requireInvalidParams(t, err)

	loadStore := newFaultySessionStore()
	loadStore.loadErr = errors.New("load failed")
	loadAgent := NewAgent(
		WithSessionStore(loadStore),
		WithLogger(slog.New(slog.DiscardHandler)),
	)
	_, err = loadAgent.handleForkSession(t.Context(), forkRaw(t, forkParams(t)))
	require.ErrorContains(t, err, "load failed")

	unknownAgent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	_, err = unknownAgent.handleForkSession(t.Context(), forkRaw(t, forkParams(t)))
	requireInvalidParams(t, err)

	headerStore := newFaultySessionStore()
	appendForkParentRows(t, headerStore, json.RawMessage(`{"type":"session"}`))
	headerAgent := NewAgent(
		WithSessionStore(headerStore),
		WithLogger(slog.New(slog.DiscardHandler)),
	)
	_, err = headerAgent.handleForkSession(t.Context(), forkRaw(t, forkParams(t)))
	requireInvalidParams(t, err)

	contentStore := newFaultySessionStore()
	appendForkParentRows(t, contentStore,
		json.RawMessage(`{"type":"session"}`),
		json.RawMessage(`{"type":"message"}`),
	)
	startAgent := NewAgent(
		WithExecutablePath(filepath.Join(t.TempDir(), "missing-pi")),
		WithSessionStore(contentStore),
		WithLogger(slog.New(slog.DiscardHandler)),
	)
	_, err = startAgent.handleForkSession(t.Context(), forkRaw(t, forkParams(t)))
	require.Error(t, err)

	commitStore := newFaultySessionStore()
	commitStore.appendErr = errors.New("append failed")
	commitAgent := NewAgent(
		WithSessionStore(commitStore),
		WithLogger(slog.New(slog.DiscardHandler)),
	)
	sessionFile := filepath.Join(t.TempDir(), "session.jsonl")
	require.NoError(t, os.WriteFile(
		sessionFile,
		[]byte("{\"type\":\"session\"}\n{\"type\":\"message\"}\n"),
		0o600,
	))
	commitAgent.sessions[forkParentID] = &agentSession{
		agent:           commitAgent,
		id:              forkParentID,
		sessionFilePath: sessionFile,
	}
	_, err = commitAgent.handleForkSession(t.Context(), forkRaw(t, forkParams(t)))
	require.ErrorIs(t, err, errSessionMirrorAppend)
}

func TestForkExtensionStoreLimitAfterNativeClone(t *testing.T) {
	store := newFaultySessionStore()
	appendForkParentRows(t, store,
		json.RawMessage(`{"type":"session"}`),
		json.RawMessage(`{"type":"message"}`),
	)

	agent := NewAgent(
		testContainmentOption(),
		WithExecutablePath("/fake/pi"),
		WithScratchDir(t.TempDir()),
		WithSessionStore(store),
		WithConcurrencyLimits(ConcurrencyLimits{MaxActiveSessions: 1}),
		WithLogger(slog.New(slog.DiscardHandler)),
	)
	agent.probeVersion = func(context.Context, string, string, pi.ContainmentSpec) (string, error) {
		return pi.DefaultMinimumVersion, nil
	}
	agent.sessions["occupied"] = &agentSession{agent: agent, id: "occupied"}

	client := newStubPiClient()
	client.state = pi.SessionState{SessionID: forkChildID, ThinkingLevel: pi.ThinkingLevelMedium}
	process := newStubProcess(false)
	agent.startPiProcess = func(context.Context, pi.LaunchSpec) (piProcess, piClient, error) {
		return process, client, nil
	}

	_, err := agent.handleForkSession(t.Context(), forkRaw(t, forkParams(t)))
	requireInvalidRequest(t, err)

	close(client.events)
	close(client.uiRequests)
}
