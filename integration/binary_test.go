//go:build integration

package integration

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/coder/acp-go-sdk"
	acpcore "github.com/savid/acp-go-core"
	"github.com/stretchr/testify/require"

	piacp "github.com/savid/acp-go-pi"
)

func TestSmokeSessionLifecycle(t *testing.T) {
	requireIntegration(t)

	h := newHarness(t, false)
	ctx := h.ctx(t)

	init, err := h.conn.Initialize(ctx, acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber})
	require.NoError(t, err)
	require.Empty(t, init.AuthMethods)
	require.True(t, init.AgentCapabilities.LoadSession)

	cwd := t.TempDir()

	session, err := h.conn.NewSession(ctx, piacp.NewSessionRequest(cwd))
	require.NoError(t, err)
	require.NotEmpty(t, session.SessionId)

	list, err := h.conn.ListSessions(ctx, piacp.ListSessionsRequest())
	require.NoError(t, err)
	require.Len(t, list.Sessions, 1)

	_, err = h.conn.CloseSession(ctx, acp.CloseSessionRequest{SessionId: session.SessionId})
	require.NoError(t, err)

	_, err = h.conn.UnstableDeleteSession(ctx, piacp.DeleteSessionRequest(session.SessionId))
	require.NoError(t, err)

	_, err = h.conn.LoadSession(ctx, piacp.LoadSessionRequest(session.SessionId, cwd))
	require.Equal(t, "unknown session", requestErrorData(t, err)["error"])
}

func TestLivePromptResumeAndPath(t *testing.T) {
	requireLive(t)

	h := newHarness(t, true)
	ctx := h.ctx(t)

	_, err := h.conn.Initialize(ctx, acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber})
	require.NoError(t, err)

	cwd := t.TempDir()
	binDir := filepath.Join(t.TempDir(), "bin")
	require.NoError(t, os.MkdirAll(binDir, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(binDir, "acp-marker"), []byte("#!/bin/sh\necho MARKER_OK\n"), 0o700))

	session, err := h.conn.NewSession(ctx, piacp.NewSessionRequest(cwd, liveModel(),
		piacp.WithSessionPiOptions(piacp.NewPiOptions(piacp.WithPiExtraPathDirs(binDir), piacp.WithPiPermission("allow")))))
	require.NoError(t, err)

	resp, err := h.conn.Prompt(ctx, piacp.TextPromptRequest(session.SessionId, "Reply with exactly LIVE_OK and nothing else. Do not use tools."))
	require.NoError(t, err)
	require.Equal(t, acp.StopReasonEndTurn, resp.StopReason)
	require.Contains(t, h.rec.text(), "LIVE_OK")

	resp, err = h.conn.Prompt(ctx, piacp.TextPromptRequest(session.SessionId, "Run the shell command `acp-marker` with the bash tool and reply with its exact output and nothing else."))
	require.NoError(t, err)
	require.Equal(t, acp.StopReasonEndTurn, resp.StopReason)
	require.Contains(t, h.rec.text(), "MARKER_OK")

	_, err = h.conn.CloseSession(ctx, acp.CloseSessionRequest{SessionId: session.SessionId})
	require.NoError(t, err)

	matches, err := filepath.Glob(filepath.Join(h.home, "sessions", "*", "*_"+string(session.SessionId)+".jsonl"))
	require.NoError(t, err)
	require.Len(t, matches, 1, "pi keeps the session file in its own home after close")

	_, err = h.conn.ResumeSession(ctx, piacp.ResumeSessionRequest(session.SessionId, cwd, liveModel()))
	require.NoError(t, err)

	resp, err = h.conn.Prompt(ctx, piacp.TextPromptRequest(session.SessionId, "Reply with exactly RESUME_OK and nothing else."))
	require.NoError(t, err)
	require.Equal(t, acp.StopReasonEndTurn, resp.StopReason)
	require.Contains(t, h.rec.text(), "RESUME_OK")
}

func TestNativeContinuation(t *testing.T) {
	requireLive(t)
	store := acpcore.NewInMemorySessionStore()
	h := newHarness(t, true, piacp.WithSessionStore(store))
	ctx := h.ctx(t)
	_, err := h.conn.Initialize(ctx, acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber})
	require.NoError(t, err)
	cwd := t.TempDir()
	session, err := h.conn.NewSession(ctx, piacp.NewSessionRequest(cwd, liveModel()))
	require.NoError(t, err)
	response, err := h.conn.Prompt(ctx, piacp.TextPromptRequest(session.SessionId, "Remember that the project slug is apricot-orbit. Reply with exactly apricot-orbit. Do not use tools."))
	require.NoError(t, err)
	require.Equal(t, acp.StopReasonEndTurn, response.StopReason)
	require.Contains(t, h.rec.text(), "apricot-orbit")
	_, err = h.conn.CloseSession(ctx, acp.CloseSessionRequest{SessionId: session.SessionId})
	require.NoError(t, err)
	h.stop()
	args := []string{"--print", "--session", string(session.SessionId)}
	if model := os.Getenv(envModel); model != "" {
		args = append(args, "--model", model)
	}
	args = append(args, "Remember that the release label is cobalt-lantern. Reply with the project slug and release label, and nothing else. Do not use tools.")
	command := exec.CommandContext(ctx, harnessPath(t, true), args...)
	command.Dir = cwd
	command.Env = append(os.Environ(), "PI_CODING_AGENT_DIR="+h.home)
	output, err := command.Output()
	require.NoError(t, err, "native continuation")
	require.Contains(t, string(output), "apricot-orbit")
	require.Contains(t, string(output), "cobalt-lantern")
	resumed := newHarnessAt(t, true, h.home, piacp.WithSessionStore(store))
	_, err = resumed.conn.Initialize(ctx, acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber})
	require.NoError(t, err)
	_, err = resumed.conn.LoadSession(ctx, piacp.LoadSessionRequest(session.SessionId, cwd, liveModel()))
	require.NoError(t, err)
	require.Contains(t, resumed.rec.text(), "apricot-orbit")
	require.Contains(t, resumed.rec.text(), "cobalt-lantern")
	before := len(resumed.rec.text())
	response, err = resumed.conn.Prompt(ctx, piacp.TextPromptRequest(session.SessionId, "What project slug and release label did we choose? Reply with both and nothing else. Do not use tools."))
	require.NoError(t, err)
	require.Equal(t, acp.StopReasonEndTurn, response.StopReason)
	require.Contains(t, resumed.rec.text()[before:], "apricot-orbit")
	require.Contains(t, resumed.rec.text()[before:], "cobalt-lantern")
}
