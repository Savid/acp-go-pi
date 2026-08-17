package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"
)

type fakeAgentConnection struct {
	initErr   error
	newErr    error
	promptErr error

	cwd       string
	prompt    string
	closed    bool
	sessionID acp.SessionId
}

func (f *fakeAgentConnection) Initialize(context.Context, acp.InitializeRequest) (acp.InitializeResponse, error) {
	return acp.InitializeResponse{}, f.initErr
}

func (f *fakeAgentConnection) NewSession(_ context.Context, params acp.NewSessionRequest) (acp.NewSessionResponse, error) {
	f.cwd = params.Cwd
	if f.sessionID == "" {
		f.sessionID = "session-1"
	}

	return acp.NewSessionResponse{SessionId: f.sessionID}, f.newErr
}

func (f *fakeAgentConnection) Prompt(_ context.Context, params acp.PromptRequest) (acp.PromptResponse, error) {
	if len(params.Prompt) > 0 && params.Prompt[0].Text != nil {
		f.prompt = params.Prompt[0].Text.Text
	}

	return acp.PromptResponse{StopReason: acp.StopReasonEndTurn}, f.promptErr
}

func (f *fakeAgentConnection) CloseSession(context.Context, acp.CloseSessionRequest) (acp.CloseSessionResponse, error) {
	f.closed = true

	return acp.CloseSessionResponse{}, nil
}

func TestClientFileMethods(t *testing.T) {
	t.Parallel()

	c := client{}
	path := filepath.Join(t.TempDir(), "note.txt")

	_, err := c.WriteTextFile(context.Background(), acp.WriteTextFileRequest{
		Path:    path,
		Content: "hello",
	})
	require.NoError(t, err)

	read, err := c.ReadTextFile(context.Background(), acp.ReadTextFileRequest{Path: path})
	require.NoError(t, err)
	require.Equal(t, "hello", read.Content)

	_, err = c.WriteTextFile(context.Background(), acp.WriteTextFileRequest{Path: "relative"})
	require.Error(t, err)

	_, err = c.ReadTextFile(context.Background(), acp.ReadTextFileRequest{Path: "relative"})
	require.Error(t, err)

	_, err = c.ReadTextFile(context.Background(), acp.ReadTextFileRequest{Path: filepath.Join(t.TempDir(), "missing.txt")})
	require.Error(t, err)

	parentFile := filepath.Join(t.TempDir(), "parent")
	require.NoError(t, os.WriteFile(parentFile, []byte("file"), 0o600))
	_, err = c.WriteTextFile(context.Background(), acp.WriteTextFileRequest{
		Path: filepath.Join(parentFile, "child.txt"),
	})
	require.Error(t, err)
}

func TestClientPermissionMethods(t *testing.T) {
	t.Parallel()

	c := client{}
	resp, err := c.RequestPermission(context.Background(), acp.RequestPermissionRequest{
		Options: []acp.PermissionOption{
			{OptionId: "reject", Kind: acp.PermissionOptionKindRejectOnce},
			{OptionId: "allow", Kind: acp.PermissionOptionKindAllowOnce},
		},
	})
	require.NoError(t, err)
	require.Equal(t, acp.PermissionOptionId("allow"), resp.Outcome.Selected.OptionId)

	resp, err = c.RequestPermission(context.Background(), acp.RequestPermissionRequest{})
	require.NoError(t, err)
	require.NotNil(t, resp.Outcome.Cancelled)
}

func TestClientSessionUpdatePrintsVisibleEvents(t *testing.T) {
	c := client{}
	status := acp.ToolCallStatusCompleted

	output := captureStdout(t, func() {
		require.NoError(t, c.SessionUpdate(context.Background(), acp.SessionNotification{
			Update: acp.SessionUpdate{
				AgentMessageChunk: &acp.SessionUpdateAgentMessageChunk{Content: acp.TextBlock("hello")},
			},
		}))
		require.NoError(t, c.SessionUpdate(context.Background(), acp.SessionNotification{
			Update: acp.SessionUpdate{
				AgentThoughtChunk: &acp.SessionUpdateAgentThoughtChunk{Content: acp.TextBlock("thinking")},
			},
		}))
		require.NoError(t, c.SessionUpdate(context.Background(), acp.SessionNotification{
			Update: acp.SessionUpdate{
				ToolCall: &acp.SessionUpdateToolCall{ToolCallId: "tool-1", Title: "Read file"},
			},
		}))
		require.NoError(t, c.SessionUpdate(context.Background(), acp.SessionNotification{
			Update: acp.SessionUpdate{
				ToolCallUpdate: &acp.SessionToolCallUpdate{ToolCallId: "tool-1", Status: &status},
			},
		}))
		require.NoError(t, c.SessionUpdate(context.Background(), acp.SessionNotification{}))
	})

	require.Contains(t, output, "hello")
	require.Contains(t, output, "[thought] thinking")
	require.Contains(t, output, "[tool] tool-1 Read file")
	require.Contains(t, output, "[tool] tool-1 completed")
}

func TestClientSessionUpdateReconcilesFinalMessageSnapshot(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	c := client{output: &output}

	c.fallback.writeText(&output, "")
	require.NoError(t, c.SessionUpdate(context.Background(), acp.SessionNotification{
		Update: acp.SessionUpdate{
			AgentMessageChunk: &acp.SessionUpdateAgentMessageChunk{
				Content: acp.TextBlock("Hello"),
			},
		},
	}))
	require.NoError(t, c.SessionUpdate(context.Background(), acp.SessionNotification{
		Update: acp.SessionUpdate{
			AgentMessageChunk: &acp.SessionUpdateAgentMessageChunk{
				Content: acp.TextBlock(" from ACP"),
			},
		},
	}))
	require.NoError(t, c.SessionUpdate(context.Background(), acp.SessionNotification{
		Update: acp.SessionUpdate{
			AgentMessageChunk: &acp.SessionUpdateAgentMessageChunk{
				Content: acp.TextBlock("Hello from ACP"),
			},
		},
	}))

	require.Equal(t, "Hello from ACP", output.String())
}

func TestClientSessionUpdateCompletesPartialFinalSnapshot(t *testing.T) {
	t.Parallel()

	messageID := "33333333-3333-4333-8333-333333333333"
	var output bytes.Buffer
	c := client{output: &output}

	require.NoError(t, c.SessionUpdate(context.Background(), acp.SessionNotification{
		Update: acp.SessionUpdate{
			AgentMessageChunk: &acp.SessionUpdateAgentMessageChunk{
				MessageId: &messageID,
				Content:   acp.TextBlock("Hello from"),
			},
		},
	}))
	require.NoError(t, c.SessionUpdate(context.Background(), acp.SessionNotification{
		Update: acp.SessionUpdate{
			AgentMessageChunk: &acp.SessionUpdateAgentMessageChunk{
				MessageId: &messageID,
				Content:   acp.TextBlock("Hello from ACP"),
			},
		},
	}))

	require.Equal(t, "Hello from ACP", output.String())
}

func TestClientSessionUpdateUsesConfiguredWriter(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	c := client{output: &output}

	require.Same(t, &output, c.writer())
	require.NoError(t, c.SessionUpdate(context.Background(), acp.SessionNotification{
		Update: acp.SessionUpdate{
			AgentMessageChunk: &acp.SessionUpdateAgentMessageChunk{Content: acp.TextBlock("hello")},
		},
	}))

	require.Equal(t, "hello", output.String())
}

func TestClientTerminalMethods(t *testing.T) {
	t.Parallel()

	c := client{}
	terminal, err := c.CreateTerminal(context.Background(), acp.CreateTerminalRequest{})
	require.NoError(t, err)
	require.Equal(t, "terminal-1", terminal.TerminalId)

	output, err := c.TerminalOutput(context.Background(), acp.TerminalOutputRequest{})
	require.NoError(t, err)
	require.False(t, output.Truncated)

	_, err = c.KillTerminal(context.Background(), acp.KillTerminalRequest{})
	require.NoError(t, err)

	_, err = c.ReleaseTerminal(context.Background(), acp.ReleaseTerminalRequest{})
	require.NoError(t, err)

	_, err = c.WaitForTerminalExit(context.Background(), acp.WaitForTerminalExitRequest{})
	require.NoError(t, err)
}

func TestRunConversation(t *testing.T) {
	t.Parallel()

	conn := &fakeAgentConnection{}
	var output bytes.Buffer

	err := runConversation(context.Background(), conn, "hello", "/repo", &output)
	require.NoError(t, err)
	require.Equal(t, "/repo", conn.cwd)
	require.Equal(t, "hello", conn.prompt)
	require.True(t, conn.closed)
	require.Contains(t, output.String(), "stop reason: end_turn")
}

func TestRunConversationErrors(t *testing.T) {
	t.Parallel()

	for _, conn := range []*fakeAgentConnection{
		{initErr: errors.New("init")},
		{newErr: errors.New("new")},
		{promptErr: errors.New("prompt")},
	} {
		var output bytes.Buffer
		err := runConversation(context.Background(), conn, "hello", "/repo", &output)
		require.Error(t, err)
	}
}

func TestRun(t *testing.T) {
	originalStartAgent := startAgent
	originalGetwd := getwd
	t.Cleanup(func() {
		startAgent = originalStartAgent
		getwd = originalGetwd
	})

	conn := &fakeAgentConnection{}
	var closed bool
	var waited bool
	var gotAgentArgs []string
	startAgent = func(_ context.Context, agentArgs []string, _ io.Writer, _ io.Writer) (*startedAgent, error) {
		gotAgentArgs = append([]string(nil), agentArgs...)

		return &startedAgent{
			conn: conn,
			close: func() {
				closed = true
			},
			wait: func() error {
				waited = true

				return nil
			},
		}, nil
	}
	getwd = func() (string, error) {
		return "/repo", nil
	}

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := run(context.Background(), nil, &stdout, &stderr)
	require.Equal(t, 0, code)
	require.Equal(t, "Reply with a short hello from ACP.", conn.prompt)
	require.True(t, closed)
	require.True(t, waited)
	require.Empty(t, stderr.String())

	startAgent = func(_ context.Context, agentArgs []string, _ io.Writer, _ io.Writer) (*startedAgent, error) {
		gotAgentArgs = append([]string(nil), agentArgs...)

		return &startedAgent{conn: &fakeAgentConnection{}}, nil
	}

	stdout.Reset()
	stderr.Reset()
	code = run(context.Background(), []string{"-auth-file", "/tmp/auth.json", "hello"}, &stdout, &stderr)
	require.Equal(t, 0, code)
	require.Equal(t, []string{"-seed-file", "auth.json=/tmp/auth.json"}, gotAgentArgs)
	require.Empty(t, stderr.String())
}

func TestMain(t *testing.T) {
	originalStartAgent := startAgent
	originalGetwd := getwd
	originalExit := exit
	originalArgs := os.Args
	t.Cleanup(func() {
		startAgent = originalStartAgent
		getwd = originalGetwd
		exit = originalExit
		os.Args = originalArgs
	})

	startAgent = func(context.Context, []string, io.Writer, io.Writer) (*startedAgent, error) {
		return &startedAgent{
			conn:  &fakeAgentConnection{},
			close: func() {},
			wait:  func() error { return nil },
		}, nil
	}
	getwd = func() (string, error) { return "/repo", nil }

	var gotCode int
	exit = func(code int) {
		gotCode = code
	}
	os.Args = []string{"minimal-client", "hello"}

	main()

	require.Equal(t, 0, gotCode)
}

func TestStartAgentProcess(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("test uses a POSIX shell script")
	}

	binDir := t.TempDir()
	goPath := filepath.Join(binDir, "go")
	err := os.WriteFile(goPath, []byte("#!/bin/sh\nwhile IFS= read -r _; do :; done\n"), 0o755)
	require.NoError(t, err)

	t.Setenv("PATH", binDir)

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	agent, err := startAgentProcess(context.Background(), nil, &stdout, &stderr)
	require.NoError(t, err)
	require.NotNil(t, agent.conn)

	agent.close()
	require.NoError(t, agent.wait())
}

func TestStartAgentProcessUsesModuleEntrypoint(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("test uses POSIX shell")
	}

	originalCommandContext := commandContext
	t.Cleanup(func() {
		commandContext = originalCommandContext
	})

	var gotName string
	var gotArgs []string
	commandContext = func(ctx context.Context, name string, args ...string) *exec.Cmd {
		gotName = name
		gotArgs = append([]string(nil), args...)

		return exec.CommandContext(ctx, "sh", "-c", "while IFS= read -r _; do :; done")
	}

	agentArgs := []string{"-seed-file", "auth.json=/tmp/auth.json"}
	agent, err := startAgentProcess(context.Background(), agentArgs, io.Discard, io.Discard)
	require.NoError(t, err)
	require.NotNil(t, agent.conn)

	agent.close()
	require.NoError(t, agent.wait())
	require.Equal(t, "go", gotName)
	require.Equal(t, []string{"run", agentPackage, "-seed-file", "auth.json=/tmp/auth.json"}, gotArgs)
}

func TestStartAgentProcessStartError(t *testing.T) {
	t.Setenv("PATH", t.TempDir())

	agent, err := startAgentProcess(context.Background(), nil, io.Discard, io.Discard)
	require.Error(t, err)
	require.Nil(t, agent)
}

func TestStartAgentProcessPipeErrors(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("test uses POSIX shell")
	}

	originalCommandContext := commandContext
	t.Cleanup(func() {
		commandContext = originalCommandContext
	})

	commandContext = func(ctx context.Context, _ string, _ ...string) *exec.Cmd {
		cmd := exec.CommandContext(ctx, "sh", "-c", "cat")
		cmd.Stdin = strings.NewReader("")

		return cmd
	}
	agent, err := startAgentProcess(context.Background(), nil, io.Discard, io.Discard)
	require.Error(t, err)
	require.Nil(t, agent)

	commandContext = func(ctx context.Context, _ string, _ ...string) *exec.Cmd {
		cmd := exec.CommandContext(ctx, "sh", "-c", "cat")
		cmd.Stdout = io.Discard

		return cmd
	}
	agent, err = startAgentProcess(context.Background(), nil, io.Discard, io.Discard)
	require.Error(t, err)
	require.Nil(t, agent)
}

func TestRunErrors(t *testing.T) {
	originalStartAgent := startAgent
	originalGetwd := getwd
	t.Cleanup(func() {
		startAgent = originalStartAgent
		getwd = originalGetwd
	})

	getwd = func() (string, error) {
		return "", errors.New("cwd")
	}

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	require.Equal(t, 2, run(context.Background(), []string{"-bad"}, &stdout, &stderr))
	require.Contains(t, stderr.String(), "flag provided but not defined")

	stderr.Reset()
	require.Equal(t, 1, run(context.Background(), []string{"hello"}, &stdout, &stderr))
	require.Contains(t, stderr.String(), "cwd")

	getwd = func() (string, error) {
		return "/repo", nil
	}
	startAgent = func(context.Context, []string, io.Writer, io.Writer) (*startedAgent, error) {
		return nil, errors.New("start")
	}

	stderr.Reset()
	require.Equal(t, 1, run(context.Background(), []string{"hello"}, &stdout, &stderr))
	require.Contains(t, stderr.String(), "start")

	startAgent = func(context.Context, []string, io.Writer, io.Writer) (*startedAgent, error) {
		return &startedAgent{conn: &fakeAgentConnection{initErr: errors.New("init")}}, nil
	}

	stderr.Reset()
	require.Equal(t, 1, run(context.Background(), []string{"hello"}, &stdout, &stderr))
	require.Contains(t, stderr.String(), "init")
}

func captureStdout(t *testing.T, fn func()) string {
	t.Helper()

	original := os.Stdout
	reader, writer, err := os.Pipe()
	require.NoError(t, err)

	os.Stdout = writer
	defer func() {
		os.Stdout = original
	}()

	fn()

	require.NoError(t, writer.Close())
	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())

	return strings.TrimSpace(string(data))
}
