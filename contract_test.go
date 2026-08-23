package piacp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"

	"github.com/savid/acp-go-pi/internal/lifecycle"
	"github.com/savid/acp-go-pi/internal/pi"
)

type unitFakeModel struct {
	Provider      string `json:"provider"`
	ID            string `json:"id"`
	Name          string `json:"name"`
	ContextWindow int64  `json:"contextWindow"`
	MaxTokens     int64  `json:"maxTokens"`
}

type unitFakeCommand struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	Source      string `json:"source"`
	Path        string `json:"path,omitempty"`
}

type unitFakeScenario struct {
	Models            []unitFakeModel   `json:"models,omitempty"`
	ReplyText         string            `json:"replyText,omitempty"`
	DeltaTexts        []string          `json:"deltaTexts,omitempty"`
	StreamDelayMs     int               `json:"streamDelayMs,omitempty"`
	PromptBehavior    string            `json:"promptBehavior,omitempty"`
	ProviderError     string            `json:"providerError,omitempty"`
	GarbageLines      int               `json:"garbageLines,omitempty"`
	ToolName          string            `json:"toolName,omitempty"`
	ToolArgs          map[string]any    `json:"toolArgs,omitempty"`
	ToolOutput        string            `json:"toolOutput,omitempty"`
	ElicitMethod      string            `json:"elicitMethod,omitempty"`
	ElicitTitle       string            `json:"elicitTitle,omitempty"`
	Commands          []unitFakeCommand `json:"commands,omitempty"`
	MCPStartupFailure string            `json:"mcpStartupFailure,omitempty"`
}

func successfulUnitScenario() unitFakeScenario {
	return unitFakeScenario{
		Models: []unitFakeModel{{
			Provider: "fake", ID: "fake-model", Name: "Fake Model",
			ContextWindow: 200000, MaxTokens: 16384,
		}},
		ReplyText: "FAKE_PI_REPLY",
	}
}

var (
	unitFakeBinaryOnce sync.Once
	unitFakeBinaryPath string
	errUnitFakeBinary  error
)

func integrationFakeBinary(t *testing.T) string {
	t.Helper()

	unitFakeBinaryOnce.Do(func() {
		_, file, _, ok := runtime.Caller(0)
		if !ok {
			errUnitFakeBinary = errors.New("resolve repository root")

			return
		}

		dir := filepath.Join("/tmp/pilfg/b3", fmt.Sprintf("unit-fake-%d", os.Getpid()))
		if err := os.MkdirAll(dir, 0o750); err != nil {
			errUnitFakeBinary = err

			return
		}

		unitFakeBinaryPath = filepath.Join(dir, "integration.test")
		cmd := exec.Command("go", "test", "-c", "-tags=integration", "-o", unitFakeBinaryPath, "./integration")
		cmd.Dir = filepath.Dir(file)
		if output, err := cmd.CombinedOutput(); err != nil {
			errUnitFakeBinary = fmt.Errorf("build fake harness: %w: %s", err, strings.TrimSpace(string(output)))
		}
	})

	require.NoError(t, errUnitFakeBinary)

	return unitFakeBinaryPath
}

func unitFakeExecutable(t *testing.T, scenario unitFakeScenario) string {
	t.Helper()

	dir := t.TempDir()
	scenarioPath := filepath.Join(dir, "scenario.json")
	data, err := json.Marshal(scenario)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(scenarioPath, data, 0o600))

	path := filepath.Join(dir, "pi")
	script := fmt.Sprintf(
		"#!/bin/sh\nACP_GO_PI_FAKE_HELPER=1 ACP_GO_PI_FAKE_MODE=%q exec %q -test.run '^TestFakePiExecutable$' -- \"$@\"\n",
		scenarioPath,
		integrationFakeBinary(t),
	)
	require.NoError(t, os.WriteFile(path, []byte(script), 0o700))

	return path
}

type conformanceClient struct {
	mu sync.Mutex

	permissionChoice string
	elicitationValue string
	updateErr        error
	extensionErr     error

	updates      []acp.SessionUpdate
	permissions  []acp.RequestPermissionRequest
	elicitations []acp.UnstableCreateElicitationRequest
	extensions   []conformanceExtension
}

type conformanceExtension struct {
	method string
	params map[string]any
}

var _ acp.Client = (*conformanceClient)(nil)
var _ acp.ExtensionMethodHandler = (*conformanceClient)(nil)

func (*conformanceClient) ReadTextFile(context.Context, acp.ReadTextFileRequest) (acp.ReadTextFileResponse, error) {
	return acp.ReadTextFileResponse{}, nil
}

func (*conformanceClient) WriteTextFile(context.Context, acp.WriteTextFileRequest) (acp.WriteTextFileResponse, error) {
	return acp.WriteTextFileResponse{}, nil
}

func (c *conformanceClient) RequestPermission(
	_ context.Context,
	params acp.RequestPermissionRequest,
) (acp.RequestPermissionResponse, error) {
	c.mu.Lock()
	c.permissions = append(c.permissions, params)
	choice := c.permissionChoice
	c.mu.Unlock()

	if choice == "cancel" {
		return acp.RequestPermissionResponse{Outcome: acp.NewRequestPermissionOutcomeCancelled()}, nil
	}

	for _, option := range params.Options {
		allow := option.Kind == acp.PermissionOptionKindAllowAlways ||
			option.Kind == acp.PermissionOptionKindAllowOnce
		reject := option.Kind == acp.PermissionOptionKindRejectAlways ||
			option.Kind == acp.PermissionOptionKindRejectOnce
		if (choice == "deny" && reject) || (choice != "deny" && allow) {
			return acp.RequestPermissionResponse{
				Outcome: acp.NewRequestPermissionOutcomeSelected(option.OptionId),
			}, nil
		}
	}

	return acp.RequestPermissionResponse{Outcome: acp.NewRequestPermissionOutcomeCancelled()}, nil
}

func (c *conformanceClient) SessionUpdate(_ context.Context, params acp.SessionNotification) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.updates = append(c.updates, params.Update)

	return c.updateErr
}

func (*conformanceClient) CreateTerminal(context.Context, acp.CreateTerminalRequest) (acp.CreateTerminalResponse, error) {
	return acp.CreateTerminalResponse{TerminalId: "terminal"}, nil
}

func (*conformanceClient) KillTerminal(context.Context, acp.KillTerminalRequest) (acp.KillTerminalResponse, error) {
	return acp.KillTerminalResponse{}, nil
}

func (*conformanceClient) TerminalOutput(context.Context, acp.TerminalOutputRequest) (acp.TerminalOutputResponse, error) {
	return acp.TerminalOutputResponse{}, nil
}

func (*conformanceClient) ReleaseTerminal(context.Context, acp.ReleaseTerminalRequest) (acp.ReleaseTerminalResponse, error) {
	return acp.ReleaseTerminalResponse{}, nil
}

func (*conformanceClient) WaitForTerminalExit(context.Context, acp.WaitForTerminalExitRequest) (acp.WaitForTerminalExitResponse, error) {
	return acp.WaitForTerminalExitResponse{}, nil
}

func (c *conformanceClient) UnstableCreateElicitation(
	_ context.Context,
	params acp.UnstableCreateElicitationRequest,
) (acp.UnstableCreateElicitationResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.elicitations = append(c.elicitations, params)

	value := c.elicitationValue
	if value == "" {
		value = "answer"
	}

	return acp.UnstableCreateElicitationResponse{
		Accept: &acp.UnstableCreateElicitationAccept{
			Action: "accept", Content: map[string]any{"value": value},
		},
	}, nil
}

func (c *conformanceClient) HandleExtensionMethod(
	_ context.Context,
	method string,
	params json.RawMessage,
) (any, error) {
	var decoded map[string]any
	if len(params) > 0 {
		if err := json.Unmarshal(params, &decoded); err != nil {
			return nil, err
		}
	}

	c.mu.Lock()
	c.extensions = append(c.extensions, conformanceExtension{method: method, params: decoded})
	err := c.extensionErr
	c.mu.Unlock()

	return map[string]any{}, err
}

func (c *conformanceClient) snapshot() ([]acp.SessionUpdate, []conformanceExtension) {
	c.mu.Lock()
	defer c.mu.Unlock()

	return append([]acp.SessionUpdate(nil), c.updates...),
		append([]conformanceExtension(nil), c.extensions...)
}

func (c *conformanceClient) text() string {
	updates, _ := c.snapshot()
	var result strings.Builder
	for _, update := range updates {
		if update.AgentMessageChunk != nil && update.AgentMessageChunk.Content.Text != nil {
			result.WriteString(update.AgentMessageChunk.Content.Text.Text)
		}
	}

	return result.String()
}

func connectConformanceAgent(
	t *testing.T,
	ctx context.Context,
	client acp.Client,
	init acp.InitializeRequest,
	scenario unitFakeScenario,
	opts ...Option,
) *acp.ClientSideConnection {
	t.Helper()

	c2aR, c2aW := io.Pipe()
	a2cR, a2cW := io.Pipe()
	serveCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)

	options := append([]Option{
		testContainmentOption(),
		WithExecutablePath(unitFakeExecutable(t, scenario)),
		WithScratchDir(t.TempDir()),
		WithLogger(slog.New(slog.DiscardHandler)),
	}, opts...)
	go func() { done <- Serve(serveCtx, c2aR, a2cW, options...) }()

	t.Cleanup(func() {
		stop()
		_ = c2aR.Close()
		_ = c2aW.Close()
		_ = a2cR.Close()
		_ = a2cW.Close()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("agent did not stop")
		}
	})

	conn := acp.NewClientSideConnection(client, c2aW, a2cR)

	// The handshake writes to an io.Pipe, and io.Pipe.Write blocks until a
	// reader appears: it observes neither ctx nor the subtest deadline. So when
	// Serve returns early nothing ever reads, the write blocks forever, and the
	// package dies on the global test timeout with the agent's real startup
	// error still sitting unread in done. Race the handshake against Serve's
	// exit so that error is what fails the case.
	initialized := make(chan error, 1)
	go func() {
		_, initErr := conn.Initialize(ctx, init)
		initialized <- initErr
	}()

	select {
	case err := <-initialized:
		require.NoError(t, err)
	case err := <-done:
		done <- err

		require.FailNowf(t, "agent stopped before the handshake completed", "Serve: %v", err)
	case <-ctx.Done():
		require.FailNowf(t, "handshake did not complete", "%v", ctx.Err())
	}

	return conn
}

func defaultInitializeRequest() acp.InitializeRequest {
	return acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber}
}

func newConformanceSession(t *testing.T, ctx context.Context, conn *acp.ClientSideConnection, opts ...SessionRequestOption) acp.SessionId {
	t.Helper()
	response, err := conn.NewSession(ctx, NewSessionRequest(t.TempDir(), opts...))
	require.NoError(t, err)

	return response.SessionId
}

func requirePiTurnFailure(t *testing.T, err error, cause string) map[string]any {
	t.Helper()
	var requestError *acp.RequestError
	require.ErrorAs(t, err, &requestError)
	require.Equal(t, -32603, requestError.Code)
	encoded, marshalErr := json.Marshal(requestError.Data)
	require.NoError(t, marshalErr)
	var data map[string]any
	require.NoError(t, json.Unmarshal(encoded, &data))
	require.Equal(t, "pi_turn_failed", data["error"])
	require.Equal(t, cause, data["cause"])

	return data
}

func TestConformanceInitializeShapeAndEncoding(t *testing.T) {
	tests := []struct {
		name      string
		encodings []acp.PositionEncodingKind
		want      acp.PositionEncodingKind
	}{
		{name: "prefer utf8", encodings: []acp.PositionEncodingKind{acp.PositionEncodingKindUtf16, acp.PositionEncodingKindUtf8}, want: acp.PositionEncodingKindUtf8},
		{name: "utf16", encodings: []acp.PositionEncodingKind{acp.PositionEncodingKindUtf32, acp.PositionEncodingKindUtf16}, want: acp.PositionEncodingKindUtf16},
		{name: "default", encodings: []acp.PositionEncodingKind{acp.PositionEncodingKindUtf32}, want: acp.PositionEncodingKindUtf16},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			client := &conformanceClient{}
			init := defaultInitializeRequest()
			init.ClientCapabilities.Fs = acp.FileSystemCapabilities{}
			init.ClientCapabilities.Fs.ReadTextFile = true
			init.ClientCapabilities.Fs.WriteTextFile = true
			init.ClientCapabilities.Terminal = true
			init.ClientCapabilities.PositionEncodings = test.encodings

			conn := connectConformanceAgent(t, ctx, client, init, successfulUnitScenario())
			response, err := conn.Initialize(ctx, init)
			require.NoError(t, err)
			require.Equal(t, "acp-go-pi", response.AgentInfo.Name)
			require.NotNil(t, response.AgentCapabilities.PositionEncoding)
			require.Equal(t, test.want, *response.AgentCapabilities.PositionEncoding)
			require.Empty(t, response.AuthMethods)
			require.True(t, response.AgentCapabilities.LoadSession)
			require.NotNil(t, response.AgentCapabilities.PromptCapabilities.Image)
			require.False(t, response.AgentCapabilities.PromptCapabilities.Audio)
			require.False(t, response.AgentCapabilities.McpCapabilities.Sse)
		})
	}
}

func TestClientElicitationCapabilityGating(t *testing.T) {
	t.Parallel()

	var explicitNull acp.ElicitationCapabilities
	require.NoError(t, json.Unmarshal([]byte(`{"form":null,"url":null}`), &explicitNull))

	for _, test := range []struct {
		name     string
		caps     *acp.ElicitationCapabilities
		wantForm bool
	}{
		{name: "nil or omitted top level", caps: nil, wantForm: false},
		{name: "empty object", caps: &acp.ElicitationCapabilities{}, wantForm: false},
		{name: "both modes explicit null", caps: &explicitNull, wantForm: false},
		{name: "url only", caps: &acp.ElicitationCapabilities{Url: &acp.ElicitationUrlCapabilities{}}, wantForm: false},
		{name: "form only", caps: &acp.ElicitationCapabilities{Form: &acp.ElicitationFormCapabilities{}}, wantForm: true},
		{name: "form and url", caps: &acp.ElicitationCapabilities{Form: &acp.ElicitationFormCapabilities{}, Url: &acp.ElicitationUrlCapabilities{}}, wantForm: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			agent := NewAgent()
			agent.clientCapabilities.Elicitation = test.caps
			require.Equal(t, test.wantForm, agent.clientSupportsFormElicitation())
		})
	}
}

func TestConformanceTurnFailuresT1ThroughT6(t *testing.T) {
	t.Run("T1 provider error", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		scenario := successfulUnitScenario()
		scenario.PromptBehavior = "providerError"
		scenario.ProviderError = "429 injected provider failure"
		conn := connectConformanceAgent(t, ctx, &conformanceClient{}, defaultInitializeRequest(), scenario)
		sessionID := newConformanceSession(t, ctx, conn)
		_, err := conn.Prompt(ctx, TextPromptRequest(sessionID, "test-turn", "fail"))
		data := requirePiTurnFailure(t, err, "provider")
		require.Contains(t, data["message"], "injected provider failure")
		_, err = conn.Prompt(ctx, TextPromptRequest(sessionID, "test-turn", "fail again"))
		requirePiTurnFailure(t, err, "provider")
	})

	t.Run("T3 process exit", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		scenario := successfulUnitScenario()
		scenario.PromptBehavior = "die"
		conn := connectConformanceAgent(t, ctx, &conformanceClient{}, defaultInitializeRequest(), scenario)
		sessionID := newConformanceSession(t, ctx, conn)
		_, err := conn.Prompt(ctx, TextPromptRequest(sessionID, "test-turn", "die"))
		requirePiTurnFailure(t, err, "process_exit")
		_, err = conn.Prompt(ctx, TextPromptRequest(sessionID, "test-turn", "addressable"))
		require.Error(t, err)
		var requestError *acp.RequestError
		if errors.As(err, &requestError) {
			require.NotEqual(t, -32602, requestError.Code)
		}
	})

	t.Run("T4 malformed record terminalizes generation", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		scenario := successfulUnitScenario()
		scenario.PromptBehavior = "garbageBurst"
		scenario.GarbageLines = 3
		conn := connectConformanceAgent(t, ctx, &conformanceClient{}, defaultInitializeRequest(), scenario)
		sessionID := newConformanceSession(t, ctx, conn)
		_, err := conn.Prompt(ctx, TextPromptRequest(sessionID, "test-turn", "garbage"))
		requirePiTurnFailure(t, err, failureCauseTransport)
		_, err = conn.Prompt(ctx, TextPromptRequest(sessionID, "test-turn", "later"))
		require.Error(t, err)
	})

	t.Run("T5 cancel guard", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		scenario := successfulUnitScenario()
		scenario.DeltaTexts = []string{"one", "two", "three"}
		scenario.StreamDelayMs = 100
		client := &conformanceClient{}
		conn := connectConformanceAgent(t, ctx, client, defaultInitializeRequest(), scenario)
		sessionID := newConformanceSession(t, ctx, conn)
		type result struct {
			response acp.PromptResponse
			err      error
		}
		done := make(chan result, 1)
		go func() {
			response, err := conn.Prompt(ctx, TextPromptRequest(sessionID, "test-turn", "cancel"))
			done <- result{response: response, err: err}
		}()
		require.Eventually(t, func() bool { return client.text() != "" }, 5*time.Second, 10*time.Millisecond)
		require.NoError(t, conn.Cancel(ctx, CancelRequest(sessionID, "test-turn")))
		turn := <-done
		require.NoError(t, turn.err)
		require.Equal(t, acp.StopReasonCancelled, turn.response.StopReason)
	})

	t.Run("T6 timeout", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		scenario := successfulUnitScenario()
		scenario.PromptBehavior = "hang"
		conn := connectConformanceAgent(t, ctx, &conformanceClient{}, defaultInitializeRequest(), scenario, WithTurnTimeout(100*time.Millisecond))
		sessionID := newConformanceSession(t, ctx, conn)
		_, err := conn.Prompt(ctx, TextPromptRequest(sessionID, "test-turn", "hang"))
		requirePiTurnFailure(t, err, "timeout")
	})
}

func TestConformanceMCPAcceptRejectShapes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn := connectConformanceAgent(t, ctx, &conformanceClient{}, defaultInitializeRequest(), successfulUnitScenario())

	sse := acp.McpServer{Sse: &acp.McpServerSseInline{Type: "sse", Name: "events", Url: "http://localhost/sse"}}
	_, err := conn.NewSession(ctx, NewSessionRequest(t.TempDir(), WithSessionMCPServers(sse)))
	requireInvalidParams(t, err)
	_, err = conn.NewSession(ctx, NewSessionRequest(t.TempDir(), WithSessionMCPServers(
		StdioMCPServer("dup", "/bin/true", nil, nil),
		StdioMCPServer("dup", "/bin/true", nil, nil),
	)))
	requireInvalidParams(t, err)
	_, err = conn.NewSession(ctx, NewSessionRequest(t.TempDir(), WithSessionMCPServers(
		StdioMCPServer("", "/bin/true", nil, nil),
	)))
	requireInvalidParams(t, err)

	sessionID := newConformanceSession(t, ctx, conn, WithSessionMCPServers(
		StdioMCPServer("stdio", "/bin/cat", []string{"--help"}, map[string]string{"A": "B"}),
		HTTPMCPServer("http", "http://127.0.0.1/mcp", map[string]string{"X-Test": "yes"}),
	))
	_, err = conn.CloseSession(ctx, acp.CloseSessionRequest{SessionId: sessionID})
	require.NoError(t, err)
}

func TestConformancePermissionAndElicitationSeparation(t *testing.T) {
	t.Run("permission outcomes", func(t *testing.T) {
		for _, choice := range []string{"allow", "deny", "cancel"} {
			t.Run(choice, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				scenario := successfulUnitScenario()
				scenario.ToolName = "bash"
				scenario.ToolArgs = map[string]any{"command": "echo test"}
				client := &conformanceClient{permissionChoice: choice}
				conn := connectConformanceAgent(t, ctx, client, defaultInitializeRequest(), scenario)
				sessionID := newConformanceSession(t, ctx, conn)
				response, err := conn.Prompt(ctx, TextPromptRequest(sessionID, "test-turn", "tool"))
				require.NoError(t, err)
				require.Equal(t, acp.StopReasonEndTurn, response.StopReason)
				client.mu.Lock()
				require.Len(t, client.permissions, 1)
				require.Empty(t, client.elicitations)
				client.mu.Unlock()
			})
		}
	})

	t.Run("form elicitation", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		scenario := successfulUnitScenario()
		scenario.ElicitMethod = "input"
		scenario.ElicitTitle = "Question"
		client := &conformanceClient{elicitationValue: "accepted"}
		init := defaultInitializeRequest()
		init.ClientCapabilities.Elicitation = &acp.ElicitationCapabilities{
			Form: &acp.ElicitationFormCapabilities{},
		}
		conn := connectConformanceAgent(t, ctx, client, init, scenario)
		sessionID := newConformanceSession(t, ctx, conn)
		response, err := conn.Prompt(ctx, TextPromptRequest(sessionID, "test-turn", "elicit"))
		require.NoError(t, err)
		require.Equal(t, acp.StopReasonEndTurn, response.StopReason)
		require.Contains(t, client.text(), "accepted")
		client.mu.Lock()
		require.Empty(t, client.permissions)
		require.Len(t, client.elicitations, 1)
		client.mu.Unlock()
	})

	t.Run("nil empty and url-only capabilities decline form", func(t *testing.T) {
		capabilities := []*acp.ElicitationCapabilities{
			nil,
			{},
			{Url: &acp.ElicitationUrlCapabilities{}},
		}
		for _, capability := range capabilities {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			scenario := successfulUnitScenario()
			scenario.ElicitMethod = "input"
			scenario.ElicitTitle = "Question"
			client := &conformanceClient{}
			init := defaultInitializeRequest()
			init.ClientCapabilities.Elicitation = capability
			conn := connectConformanceAgent(t, ctx, client, init, scenario)
			sessionID := newConformanceSession(t, ctx, conn)
			response, err := conn.Prompt(ctx, TextPromptRequest(sessionID, "test-turn", "elicit"))
			require.NoError(t, err)
			require.Equal(t, acp.StopReasonEndTurn, response.StopReason)
			client.mu.Lock()
			require.Empty(t, client.elicitations)
			client.mu.Unlock()
			cancel()
		}
	})
}

func TestConformanceConfigOptions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn := connectConformanceAgent(t, ctx, &conformanceClient{}, defaultInitializeRequest(), successfulUnitScenario())
	sessionID := newConformanceSession(t, ctx, conn)

	response, err := conn.SetSessionConfigOption(ctx, SetModelRequest(sessionID, "fake/fake-model"))
	require.NoError(t, err)
	require.NotEmpty(t, response.ConfigOptions)
	response, err = conn.SetSessionConfigOption(ctx, SetConfigOptionRequest(sessionID, configThoughtLevel, "high"))
	require.NoError(t, err)
	require.Len(t, response.ConfigOptions, 2)

	_, err = conn.SetSessionConfigOption(ctx, SetModelRequest(sessionID, "invalid"))
	requireInvalidParams(t, err)
	_, err = conn.SetSessionConfigOption(ctx, SetModelRequest(sessionID, "fake/missing"))
	requireInvalidParams(t, err)
	_, err = conn.SetSessionConfigOption(ctx, SetConfigOptionRequest(sessionID, "unsupported", "value"))
	requireInvalidParams(t, err)
	// The boolean variant carries no wire field of its own: the SDK selects it
	// from "type", so that discriminator is the path the rejection must name.
	_, err = conn.SetSessionConfigOption(ctx, acp.SetSessionConfigOptionRequest{
		Boolean: &acp.SetSessionConfigOptionBoolean{SessionId: sessionID, ConfigId: "bool", Value: true},
	})
	requireUnsupportedField(t, err, jsonFieldType)
	// An empty union has no variant to marshal, so this one never reaches the
	// wire; the in-process rejection it would earn is pinned in
	// TestConfigSelectionFailureBranches.
	_, err = conn.SetSessionConfigOption(ctx, acp.SetSessionConfigOptionRequest{})
	requireInvalidParams(t, err)

	_, err = conn.SetSessionMode(ctx, acp.SetSessionModeRequest{SessionId: sessionID, ModeId: "mode"})
	var requestError *acp.RequestError
	require.ErrorAs(t, err, &requestError)
	require.Equal(t, -32601, requestError.Code)
}

// TestConformanceThinkingLevelPassesThrough drives the read-back over the real
// wire against a pi that acknowledges every level and applies only the ones it
// knows. What the host reads back is the level pi runs, so a value pi declined
// reports the level pi kept instead of the host's own request.
func TestConformanceThinkingLevelPassesThrough(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn := connectConformanceAgent(t, ctx, &conformanceClient{}, defaultInitializeRequest(), successfulUnitScenario())
	sessionID := newConformanceSession(t, ctx, conn)

	currentThoughtLevel := func(response acp.SetSessionConfigOptionResponse) acp.SessionConfigValueId {
		t.Helper()
		require.Len(t, response.ConfigOptions, 2)
		require.NotNil(t, response.ConfigOptions[1].Select)
		require.Equal(t, configThoughtLevel, response.ConfigOptions[1].Select.Id)

		return response.ConfigOptions[1].Select.CurrentValue
	}

	response, err := conn.SetSessionConfigOption(ctx,
		SetConfigOptionRequest(sessionID, configThoughtLevel, pi.ThinkingLevelHigh))
	require.NoError(t, err)
	require.Equal(t, acp.SessionConfigValueId(pi.ThinkingLevelHigh), currentThoughtLevel(response))

	for _, declined := range []acp.SessionConfigValueId{"registry-unknown", acp.SessionConfigValueId(" " + pi.ThinkingLevelMax + " ")} {
		response, err = conn.SetSessionConfigOption(ctx,
			SetConfigOptionRequest(sessionID, configThoughtLevel, declined))
		require.NoError(t, err, "a value pi acknowledges is not a refusal")
		require.Equal(t, acp.SessionConfigValueId(pi.ThinkingLevelHigh), currentThoughtLevel(response),
			"pi kept the level it was already running, and that is what the host reads back")
	}
}

func TestConformanceStoreResumeLoadAndPagination(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	store := NewInMemorySessionStore()
	scenario := successfulUnitScenario()
	client := &conformanceClient{}
	conn := connectConformanceAgent(t, ctx, client, defaultInitializeRequest(), scenario, WithSessionStore(store))
	cwd := t.TempDir()
	session, err := conn.NewSession(ctx, NewSessionRequest(cwd))
	require.NoError(t, err)
	_, err = conn.Prompt(ctx, TextPromptRequest(session.SessionId, "test-turn", "history"))
	require.NoError(t, err)
	_, err = conn.CloseSession(ctx, acp.CloseSessionRequest{SessionId: session.SessionId})
	require.NoError(t, err)

	loadClient := &conformanceClient{}
	loadConn := connectConformanceAgent(t, ctx, loadClient, defaultInitializeRequest(), scenario, WithSessionStore(store))
	_, err = loadConn.LoadSession(ctx, LoadSessionRequest(session.SessionId, cwd))
	require.NoError(t, err)
	require.Contains(t, loadClient.text(), "FAKE_PI_REPLY")
	_, err = loadConn.CloseSession(ctx, acp.CloseSessionRequest{SessionId: session.SessionId})
	require.NoError(t, err)

	resumeConn := connectConformanceAgent(t, ctx, &conformanceClient{}, defaultInitializeRequest(), scenario, WithSessionStore(store))
	_, err = resumeConn.ResumeSession(ctx, ResumeSessionRequest(session.SessionId, cwd))
	require.NoError(t, err)

	list, err := resumeConn.ListSessions(ctx, ListSessionsRequest(WithListSessionsCwd(cwd)))
	require.NoError(t, err)
	require.NotEmpty(t, list.Sessions)
	// An empty cwd is an absent filter, never a filter that matches nothing.
	list, err = resumeConn.ListSessions(ctx, ListSessionsRequest(WithListSessionsCwd("")))
	require.NoError(t, err)
	require.NotEmpty(t, list.Sessions)
	list, err = resumeConn.ListSessions(ctx, ListSessionsRequest(WithListSessionsCursor("bad")))
	requireInvalidParams(t, err)
}

// lifecycleInitializeRequest offers the session lifecycle extension, which is
// what makes the sessions on that connection open a real incarnation. The
// default handshake offers nothing, so a session under it mints no stream, no
// cycle, and commits its boundaries with an empty identity — which is why a
// resume across the extension has to be exercised through its own handshake.
func lifecycleInitializeRequest() acp.InitializeRequest {
	return acp.InitializeRequest{
		ProtocolVersion: acp.ProtocolVersionNumber,
		Meta: map[string]any{lifecycleMetaKey: map[string]any{
			"versions": []any{lifecycle.Version},
		}},
	}
}

// lifecyclePromptRequest carries the submission correlation a negotiated prompt
// owes alongside the route nonce every prompt carries.
func lifecyclePromptRequest(sessionID acp.SessionId, turnNonce, text string) acp.PromptRequest {
	request := TextPromptRequest(sessionID, turnNonce, text)
	request.Meta[lifecycleMetaKey] = map[string]any{
		"version": lifecycle.Version,
		"submission": map[string]any{
			"submissionId": turnNonce + "-submission",
			"clientNonce":  turnNonce + "-nonce",
		},
	}

	return request
}

// TestConformanceLifecycleSessionResumesAfterItsCloseBoundary pins the durable
// half of the lifecycle extension across a real rotation, over the wire and
// through a fresh connection.
//
// A negotiated session opens its incarnation on the establishing response, so
// every boundary it commits names the cycle that stream minted. The turn that
// ran under it is finished by the time the close boundary is written — the
// terminal idle already cleared the turn, and turn identity is present only
// while a turn is open — so the close records a cycle and no turn. The next
// incarnation must read that boundary back and resume from it. A reader that
// refused the shape its own writer produces would strand every session on its
// first rotation, which is the whole session rather than an edge of it.
func TestConformanceLifecycleSessionResumesAfterItsCloseBoundary(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	store := NewInMemorySessionStore()
	scenario := successfulUnitScenario()
	cwd := t.TempDir()

	conn := connectConformanceAgent(
		t, ctx, &conformanceClient{}, lifecycleInitializeRequest(), scenario, WithSessionStore(store))
	session, err := conn.NewSession(ctx, NewSessionRequest(cwd))
	require.NoError(t, err)

	_, err = conn.Prompt(ctx, lifecyclePromptRequest(session.SessionId, "first-turn", "history"))
	require.NoError(t, err)
	_, err = conn.CloseSession(ctx, acp.CloseSessionRequest{SessionId: session.SessionId})
	require.NoError(t, err)

	journal, err := store.Load(ctx, SessionKey{
		SessionID: string(session.SessionId), Subpath: SessionStoreLifecycleSubpath,
	})
	require.NoError(t, err)
	require.NotEmpty(t, journal, "a negotiated close commits the boundary its resume stands on")

	var boundary lifecycleBoundaryRecord

	require.NoError(t, json.Unmarshal(journal[len(journal)-1], &boundary))
	require.NotEmpty(t, boundary.StreamID)
	require.NotEmpty(t, boundary.CycleID, "the incarnation names the cycle its stream minted")
	require.Empty(t, boundary.TurnID, "the settled turn is over before the close boundary is written")

	// The rotation: a fresh connection resumes the same id from that boundary.
	loadClient := &conformanceClient{}
	loadConn := connectConformanceAgent(
		t, ctx, loadClient, lifecycleInitializeRequest(), scenario, WithSessionStore(store))
	_, err = loadConn.LoadSession(ctx, LoadSessionRequest(session.SessionId, cwd))
	require.NoError(t, err)
	require.Contains(t, loadClient.text(), "FAKE_PI_REPLY", "the resumed session replayed its history")

	// A resumed session is a working one, not merely a load that returned: it
	// takes a second turn and closes on a boundary of its own.
	_, err = loadConn.Prompt(ctx, lifecyclePromptRequest(session.SessionId, "second-turn", "again"))
	require.NoError(t, err)
	_, err = loadConn.CloseSession(ctx, acp.CloseSessionRequest{SessionId: session.SessionId})
	require.NoError(t, err)
}

// lockedLogBuffer collects log records written from the agent's own goroutines.
type lockedLogBuffer struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (b *lockedLogBuffer) Write(record []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.Write(record)
}

func (b *lockedLogBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.String()
}

// TestConformanceSessionClosedImmediatelyAfterOpen pins the sequence a host
// uses to read an agent's metadata and nothing else: open a session, close it,
// keep the connection. Closing is legal at any point, including while the
// snapshot this agent defers behind its own establishing response is still
// landing, so the close reports no fault — and the connection it ran on stays
// usable for the session that follows.
func TestConformanceSessionClosedImmediatelyAfterOpen(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	logs := &lockedLogBuffer{}
	client := &conformanceClient{}
	conn := connectConformanceAgent(
		t, ctx, client, lifecycleInitializeRequest(), successfulUnitScenario(),
		WithLogger(slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelError}))),
	)

	for range 3 {
		probe, err := conn.NewSession(ctx, NewSessionRequest(t.TempDir()))
		require.NoError(t, err)
		_, err = conn.CloseSession(ctx, acp.CloseSessionRequest{SessionId: probe.SessionId})
		require.NoError(t, err)
	}

	session, err := conn.NewSession(ctx, NewSessionRequest(t.TempDir()))
	require.NoError(t, err)
	_, err = conn.Prompt(ctx, lifecyclePromptRequest(session.SessionId, "after-probe", "hello"))
	require.NoError(t, err)
	require.Contains(t, client.text(), "FAKE_PI_REPLY")

	require.Empty(t, logs.String(), "a host close is not an error this agent reports")
}

func TestConformanceDeleteForkAndUnknownSession(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	store := NewInMemorySessionStore()
	conn := connectConformanceAgent(t, ctx, &conformanceClient{}, defaultInitializeRequest(), successfulUnitScenario(), WithSessionStore(store))

	emptyID := newConformanceSession(t, ctx, conn)
	_, err := CallForkSession(ctx, conn, ForkSessionRequest(emptyID, t.TempDir()))
	requireInvalidParams(t, err)

	sessionID := newConformanceSession(t, ctx, conn)
	_, err = conn.Prompt(ctx, TextPromptRequest(sessionID, "test-turn", "persist"))
	require.NoError(t, err)
	forked, err := CallForkSession(ctx, conn, ForkSessionRequest(sessionID, t.TempDir()))
	require.NoError(t, err)
	require.NotEqual(t, sessionID, forked.SessionId)

	_, err = conn.UnstableDeleteSession(ctx, DeleteSessionRequest(sessionID))
	require.NoError(t, err)
	_, err = conn.UnstableDeleteSession(ctx, DeleteSessionRequest(sessionID))
	require.NoError(t, err)
	_, err = conn.LoadSession(ctx, LoadSessionRequest(sessionID, t.TempDir()))
	requireInvalidParams(t, err)
	_, err = conn.ResumeSession(ctx, ResumeSessionRequest(sessionID, t.TempDir()))
	requireInvalidParams(t, err)
	_, err = conn.Prompt(ctx, TextPromptRequest(sessionID, "test-turn", "gone"))
	requireInvalidParams(t, err)
	require.NoError(t, conn.Cancel(ctx, acp.CancelNotification{SessionId: sessionID}))

	list, err := conn.ListSessions(ctx, ListSessionsRequest())
	require.NoError(t, err)
	for _, session := range list.Sessions {
		require.NotEqual(t, sessionID, session.SessionId)
	}
}

func requireInvalidParams(t *testing.T, err error) {
	t.Helper()
	var requestError *acp.RequestError
	require.ErrorAs(t, err, &requestError)
	require.Equal(t, -32602, requestError.Code)
}

// requireUnsupportedField asserts the family's uniform unsupported-field
// rejection: -32602 whose data carries exactly the two contracted keys.
func requireUnsupportedField(t *testing.T, err error, field string) {
	t.Helper()

	var requestError *acp.RequestError

	require.ErrorAs(t, err, &requestError)
	require.Equal(t, -32602, requestError.Code)
	require.Equal(t, map[string]any{"error": "unsupported", "field": field}, requestError.Data)
}

// requireUnsupportedOption asserts a construction-time option rejection: the
// same two contracted keys naming the option, but -32603, because the caller's
// params were valid and the fault is in the agent the host built.
func requireUnsupportedOption(t *testing.T, err error, field string) {
	t.Helper()

	var requestError *acp.RequestError

	require.ErrorAs(t, err, &requestError)
	require.Equal(t, -32603, requestError.Code)
	require.Equal(t, map[string]any{"error": "unsupported", "field": field}, requestError.Data)
}

func anyMap(t *testing.T, value any) map[string]any {
	t.Helper()
	nested, ok := value.(map[string]any)
	require.True(t, ok)

	return nested
}
