package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/coder/acp-go-sdk"
	piacp "github.com/savid/acp-go-pi"
	"github.com/stretchr/testify/require"
)

func TestReadTranscriptJSONL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	require.NoError(t, os.WriteFile(path, []byte("\n"+
		`{"type":"session","id":"session-1","cwd":"/repo"}`+"\n"+
		`{"type":"assistant"}`+"\n"), 0o600))

	entries, sessionID, cwd, err := readTranscriptJSONL(path)
	require.NoError(t, err)
	require.Len(t, entries, 2)
	require.Equal(t, "session-1", sessionID)
	require.Equal(t, "/repo", cwd)

	missingEntries, missingSessionID, missingCwd, missingErr := readTranscriptJSONL(filepath.Join(t.TempDir(), "missing.jsonl"))
	require.Error(t, missingErr)
	require.Nil(t, missingEntries)
	require.Empty(t, missingSessionID)
	require.Empty(t, missingCwd)
}

func TestRunUsesInferredValuesAndLoadedSession(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	cwd := t.TempDir()
	require.NoError(t, os.WriteFile(path, []byte(
		fmt.Sprintf(`{"type":"session","id":"session-1","cwd":%q}`+"\n", cwd)+
			`{"type":"assistant"}`+"\n"), 0o600))
	authFile := filepath.Join(t.TempDir(), "auth.json")
	require.NoError(t, os.WriteFile(authFile, []byte(`{"provider":"token"}`), 0o600))

	previousRunLoaded := runLoaded
	expectedSessionID := "session-1"
	expectedCwd := cwd
	expectedPrompt := "prompt"
	expectedPath := "/bin/pi"
	expectedScratch := "/scratch/pi"
	expectedAuth := `{"provider":"token"}`
	runLoaded = func(_ context.Context, store piacp.SessionStore, sessionID string, gotCwd string, prompt string, piPath string, scratchDir string, authJSON string, stdout io.Writer) error {
		require.Equal(t, expectedSessionID, sessionID)
		require.Equal(t, expectedCwd, gotCwd)
		require.Equal(t, expectedPrompt, prompt)
		require.Equal(t, expectedPath, piPath)
		require.Equal(t, expectedScratch, scratchDir)
		require.Equal(t, expectedAuth, authJSON)
		entries, err := store.Load(context.Background(), piacp.SessionKey{SessionID: sessionID})
		require.NoError(t, err)
		require.Len(t, entries, 2)
		fmt.Fprint(stdout, "loaded")

		return nil
	}
	t.Cleanup(func() { runLoaded = previousRunLoaded })

	var stdout bytes.Buffer
	err := run(context.Background(), []string{"-file", path, "-prompt", "prompt", "-path", "/bin/pi", "-scratch-dir", "/scratch/pi", "-auth-file", authFile}, &stdout, io.Discard)
	require.NoError(t, err)
	require.Equal(t, "loaded", stdout.String())

	stdout.Reset()
	expectedSessionID = "explicit"
	expectedPrompt = defaultPrompt
	expectedPath = ""
	expectedScratch = ""
	expectedAuth = ""
	err = run(context.Background(), []string{"-file", path, "-session", "explicit", "-cwd", cwd}, &stdout, io.Discard)
	require.NoError(t, err)
}

func TestRunErrors(t *testing.T) {
	entries, sessionID, cwd, err := readTranscriptJSONL("")
	require.Error(t, err)
	require.Nil(t, entries)
	require.Empty(t, sessionID)
	require.Empty(t, cwd)

	err = run(context.Background(), []string{"-bad"}, io.Discard, io.Discard)
	require.Error(t, err)
	err = run(context.Background(), []string{"-file", filepath.Join(t.TempDir(), "missing.jsonl")}, io.Discard, io.Discard)
	require.Error(t, err)

	path := filepath.Join(t.TempDir(), "session.jsonl")
	require.NoError(t, os.WriteFile(path, []byte(`{"type":"assistant"}`+"\n"), 0o600))
	err = run(context.Background(), []string{"-file", path}, io.Discard, io.Discard)
	require.ErrorContains(t, err, "session id is required")

	previousGetwd := getwd
	previousRunLoaded := runLoaded
	getwd = func() (string, error) { return "", errors.New("getwd failed") }
	runLoaded = func(context.Context, piacp.SessionStore, string, string, string, string, string, string, io.Writer) error {
		return nil
	}
	t.Cleanup(func() {
		getwd = previousGetwd
		runLoaded = previousRunLoaded
	})
	require.NoError(t, os.WriteFile(path, []byte(`{"type":"session","id":"session-1"}`+"\n"), 0o600))
	err = run(context.Background(), []string{"-file", path}, io.Discard, io.Discard)
	require.ErrorContains(t, err, "getwd failed")

	getwd = func() (string, error) { return t.TempDir(), nil }
	runLoaded = func(context.Context, piacp.SessionStore, string, string, string, string, string, string, io.Writer) error {
		return errors.New("load failed")
	}
	err = run(context.Background(), []string{"-file", path}, io.Discard, io.Discard)
	require.ErrorContains(t, err, "load failed")

	err = run(context.Background(), []string{"-file", path, "-cwd", t.TempDir(), "-auth-file", filepath.Join(t.TempDir(), "missing.json")}, io.Discard, io.Discard)
	require.ErrorContains(t, err, "read auth file")

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	err = run(cancelled, []string{"-file", path, "-session", "session-1", "-cwd", t.TempDir()}, io.Discard, io.Discard)
	require.ErrorIs(t, err, context.Canceled)
}

func TestRunLoadedSessionWithFakeServe(t *testing.T) {
	previousServe := serve
	serve = fakeServe(t, "", nil)
	t.Cleanup(func() { serve = previousServe })

	var stdout bytes.Buffer
	err := runLoadedSession(context.Background(), piacp.NewInMemorySessionStore(), "session-1", t.TempDir(), "prompt", "", "", `{"provider":"token"}`, &stdout)
	require.NoError(t, err)
	require.Contains(t, stdout.String(), "== resume smoke test ==")
	require.Contains(t, stdout.String(), "stop reason: end_turn")
}

func TestRunLoadedSessionErrors(t *testing.T) {
	previousServe := serve
	t.Cleanup(func() { serve = previousServe })

	for _, tc := range []struct {
		name   string
		method string
		err    *acp.RequestError
	}{
		{name: "initialize", method: acp.AgentMethodInitialize, err: acp.NewInternalError(map[string]any{"error": "init"})},
		{name: "load", method: acp.AgentMethodSessionLoad, err: acp.NewInternalError(map[string]any{"error": "load"})},
		{name: "prompt", method: acp.AgentMethodSessionPrompt, err: acp.NewInternalError(map[string]any{"error": "prompt"})},
	} {
		t.Run(tc.name, func(t *testing.T) {
			serve = fakeServe(t, tc.method, tc.err)
			err := runLoadedSession(context.Background(), piacp.NewInMemorySessionStore(), "session-1", t.TempDir(), "prompt", "", "", "", io.Discard)
			require.Error(t, err)
		})
	}
}

func fakeServe(t *testing.T, failMethod string, failErr *acp.RequestError) func(context.Context, io.Reader, io.Writer, ...piacp.Option) error {
	t.Helper()

	return func(ctx context.Context, input io.Reader, output io.Writer, _ ...piacp.Option) error {
		_ = acp.NewConnection(func(_ context.Context, method string, params json.RawMessage) (any, *acp.RequestError) {
			if method == failMethod {
				return nil, failErr
			}

			switch method {
			case acp.AgentMethodInitialize:
				return acp.InitializeResponse{ProtocolVersion: acp.ProtocolVersionNumber}, nil
			case acp.AgentMethodSessionLoad:
				var req acp.LoadSessionRequest
				if err := json.Unmarshal(params, &req); err != nil {
					return nil, acp.NewInvalidParams(map[string]any{"error": err.Error()})
				}

				return acp.LoadSessionResponse{}, nil
			case acp.AgentMethodSessionPrompt:
				return acp.PromptResponse{StopReason: acp.StopReasonEndTurn}, nil
			case acp.AgentMethodSessionClose:
				return acp.CloseSessionResponse{}, nil
			default:
				return nil, acp.NewMethodNotFound(method)
			}
		}, output, input)
		<-ctx.Done()

		return nil
	}
}

func TestMainUsesRunMainAndExit(t *testing.T) {
	previousRunMain := runMain
	previousExit := exit
	previousArgs := os.Args
	runMain = func(context.Context, []string, io.Writer, io.Writer) error { return nil }
	exit = func(code int) { panic(fmt.Sprintf("exit %d", code)) }
	os.Args = []string{"resume-from-file"}
	main()

	runMain = func(context.Context, []string, io.Writer, io.Writer) error { return errors.New("boom") }
	require.PanicsWithValue(t, "exit 1", main)
	t.Cleanup(func() {
		runMain = previousRunMain
		exit = previousExit
		os.Args = previousArgs
	})
}

func TestClientMethods(t *testing.T) {
	var stdout bytes.Buffer
	c := &client{output: &stdout}
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "nested", "file.txt")
	_, err := c.WriteTextFile(ctx, acp.WriteTextFileRequest{Path: path, Content: "body"})
	require.NoError(t, err)
	read, err := c.ReadTextFile(ctx, acp.ReadTextFileRequest{Path: path})
	require.NoError(t, err)
	require.Equal(t, "body", read.Content)
	_, err = c.ReadTextFile(ctx, acp.ReadTextFileRequest{Path: filepath.Join(t.TempDir(), "missing")})
	require.Error(t, err)
	notDir := filepath.Join(t.TempDir(), "file")
	require.NoError(t, os.WriteFile(notDir, []byte("x"), 0o600))
	_, err = c.WriteTextFile(ctx, acp.WriteTextFileRequest{Path: filepath.Join(notDir, "child"), Content: "body"})
	require.Error(t, err)

	permission, err := c.RequestPermission(ctx, acp.RequestPermissionRequest{})
	require.NoError(t, err)
	require.NotNil(t, permission.Outcome.Cancelled)
	require.NoError(t, c.SessionUpdate(ctx, acp.SessionNotification{}))
	require.NoError(t, c.SessionUpdate(ctx, acp.SessionNotification{Update: acp.SessionUpdate{AgentMessageChunk: &acp.SessionUpdateAgentMessageChunk{Content: acp.TextBlock("hi")}}}))
	require.Equal(t, "hi", stdout.String())
	require.Equal(t, "hi", c.text.String())
	require.NoError(t, (&client{}).SessionUpdate(ctx, acp.SessionNotification{Update: acp.SessionUpdate{AgentMessageChunk: &acp.SessionUpdateAgentMessageChunk{Content: acp.TextBlock("stdout")}}}))

	terminal, err := c.CreateTerminal(ctx, acp.CreateTerminalRequest{})
	require.NoError(t, err)
	require.Equal(t, "terminal-1", terminal.TerminalId)
	_, err = c.KillTerminal(ctx, acp.KillTerminalRequest{})
	require.NoError(t, err)
	output, err := c.TerminalOutput(ctx, acp.TerminalOutputRequest{})
	require.NoError(t, err)
	require.False(t, output.Truncated)
	_, err = c.ReleaseTerminal(ctx, acp.ReleaseTerminalRequest{})
	require.NoError(t, err)
	_, err = c.WaitForTerminalExit(ctx, acp.WaitForTerminalExitRequest{})
	require.NoError(t, err)
}

func TestReadTranscriptScannerError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "long.jsonl")
	require.NoError(t, os.WriteFile(path, []byte(strings.Repeat("x", bufio.MaxScanTokenSize+1)), 0o600))
	entries, sessionID, cwd, err := readTranscriptJSONL(path)
	require.Error(t, err)
	require.Nil(t, entries)
	require.Empty(t, sessionID)
	require.Empty(t, cwd)
}
