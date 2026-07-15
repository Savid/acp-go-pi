//go:build integration

package integration

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	piacp "github.com/savid/acp-go-pi"
	"github.com/stretchr/testify/require"
)

// TestPiACPFakeStoreResumeAfterNativeDeletion proves the store is the source
// of truth: after every native session file is deleted, a resume hydrated
// purely from store rows keeps the session id and history usable. The fake
// harness keeps this deterministic and token-free.
func TestPiACPFakeStoreResumeAfterNativeDeletion(t *testing.T) {
	requireRunIntegration(t)
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	store := piacp.NewInMemorySessionStore()
	fakePath := fakePiExecutable(t, fakeTurnScenario())
	scratchDir := t.TempDir()
	cwd := t.TempDir()

	client := &recordingClient{}
	conn := connectAgentForTest(t, ctx, client,
		piacp.WithExecutablePath(fakePath),
		piacp.WithScratchDir(scratchDir),
		piacp.WithSessionStore(store),
	)

	session, err := conn.NewSession(ctx, piacp.NewSessionRequest(cwd))
	require.NoError(t, err)

	resp, err := conn.Prompt(ctx, piacp.TextPromptRequest(session.SessionId, "test-turn", "seed the store"))
	require.NoError(t, err)
	require.Equal(t, acp.StopReasonEndTurn, resp.StopReason)

	entries, err := store.Load(ctx, piacp.SessionKey{SessionID: string(session.SessionId)})
	require.NoError(t, err)
	require.NotEmpty(t, entries, "the mirror must be durable once the prompt response returned")

	_, err = conn.CloseSession(ctx, acp.CloseSessionRequest{SessionId: session.SessionId})
	require.NoError(t, err)

	// Wipe all ephemeral scratch state; only the store survives.
	require.NoError(t, os.RemoveAll(scratchDir))

	resumeClient := &recordingClient{}
	resumeConn := connectAgentForTest(t, ctx, resumeClient,
		piacp.WithExecutablePath(fakePath),
		piacp.WithScratchDir(t.TempDir()),
		piacp.WithSessionStore(store),
	)

	// Resume is addressed by the stored session id; success means that id
	// survived the wipe.
	_, err = resumeConn.ResumeSession(ctx,
		piacp.ResumeSessionRequest(session.SessionId, cwd))
	require.NoError(t, err)

	resp, err = resumeConn.Prompt(ctx, piacp.TextPromptRequest(session.SessionId, "test-turn", "after restore"))
	require.NoError(t, err)
	require.Equal(t, acp.StopReasonEndTurn, resp.StopReason)
}

func TestPiACPFakeStoreLoadReplaysHistory(t *testing.T) {
	requireRunIntegration(t)
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	store := piacp.NewInMemorySessionStore()
	fakePath := fakePiExecutable(t, fakeTurnScenario())
	cwd := t.TempDir()

	client := &recordingClient{}
	conn := connectAgentForTest(t, ctx, client,
		piacp.WithExecutablePath(fakePath),
		piacp.WithScratchDir(t.TempDir()),
		piacp.WithSessionStore(store),
	)

	session, err := conn.NewSession(ctx, piacp.NewSessionRequest(cwd))
	require.NoError(t, err)

	_, err = conn.Prompt(ctx, piacp.TextPromptRequest(session.SessionId, "test-turn", "history seed"))
	require.NoError(t, err)

	_, err = conn.CloseSession(ctx, acp.CloseSessionRequest{SessionId: session.SessionId})
	require.NoError(t, err)

	loadClient := &recordingClient{}
	loadConn := connectAgentForTest(t, ctx, loadClient,
		piacp.WithExecutablePath(fakePath),
		piacp.WithScratchDir(t.TempDir()),
		piacp.WithSessionStore(store),
	)

	_, err = loadConn.LoadSession(ctx, piacp.LoadSessionRequest(session.SessionId, cwd))
	require.NoError(t, err)
	require.Contains(t, loadClient.text(), fakeDefaultReply,
		"session/load must replay the stored transcript")

	list, err := loadConn.ListSessions(ctx, piacp.ListSessionsRequest())
	require.NoError(t, err)

	found := false
	for _, summary := range list.Sessions {
		if summary.SessionId == session.SessionId {
			found = true
		}
	}
	require.True(t, found, "stored sessions must be listed")
}

// TestPiACPLiveStoreResume is the token-spending variant: a real pi session
// is mirrored, all native state is deleted, and the restored session must
// still answer with its history intact.
func TestPiACPLiveStoreResume(t *testing.T) {
	requireLiveTokens(t)
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 2*liveTurnTimeout)
	defer cancel()

	store := piacp.NewInMemorySessionStore()
	scratchDir := t.TempDir()
	cwd := t.TempDir()

	options := append(livePiOptions(t), piacp.WithSessionStore(store))
	options = append(options, piacp.WithScratchDir(scratchDir))

	client := &recordingClient{}
	conn := connectAgentForTest(t, ctx, client, options...)

	session, err := conn.NewSession(ctx, piacp.NewSessionRequest(cwd))
	require.NoError(t, err)

	resp, err := conn.Prompt(ctx, piacp.TextPromptRequest(session.SessionId,
		"test-turn",
		"Remember the code word DRAGONFRUIT. Reply with exactly ACP_PI_STORED."))
	require.NoError(t, err)
	require.Equal(t, acp.StopReasonEndTurn, resp.StopReason)

	_, err = conn.CloseSession(ctx, acp.CloseSessionRequest{SessionId: session.SessionId})
	require.NoError(t, err)

	require.NoError(t, os.RemoveAll(scratchDir))

	resumeOptions := append(livePiOptions(t), piacp.WithSessionStore(store))

	resumeClient := &recordingClient{}
	resumeConn := connectAgentForTest(t, ctx, resumeClient, resumeOptions...)

	_, err = resumeConn.ResumeSession(ctx, piacp.ResumeSessionRequest(session.SessionId, cwd))
	require.NoError(t, err)

	resp, err = resumeConn.Prompt(ctx, piacp.TextPromptRequest(session.SessionId,
		"test-turn",
		"Reply with exactly the code word I asked you to remember, and nothing else."))
	require.NoError(t, err)
	require.Equal(t, acp.StopReasonEndTurn, resp.StopReason)
	require.Contains(t, resumeClient.text(), "DRAGONFRUIT")

	// Deletion tombstones the session against the real harness: hidden from
	// list, session-scoped requests rejected with the uniform error.
	_, err = resumeConn.UnstableDeleteSession(ctx, piacp.DeleteSessionRequest(session.SessionId))
	require.NoError(t, err)

	list, err := resumeConn.ListSessions(ctx, piacp.ListSessionsRequest())
	require.NoError(t, err)
	for _, summary := range list.Sessions {
		require.NotEqual(t, session.SessionId, summary.SessionId, "deleted sessions must not be listed")
	}

	_, err = resumeConn.Prompt(ctx, piacp.TextPromptRequest(session.SessionId, "test-turn", "gone"))
	var reqErr *acp.RequestError
	require.ErrorAs(t, err, &reqErr)
	require.Equal(t, -32602, reqErr.Code)
}
