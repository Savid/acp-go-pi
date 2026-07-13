//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"errors"
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
	piacp "github.com/savid/acp-go-pi"
	"github.com/stretchr/testify/require"
)

const (
	envRunIntegration = "ACP_GO_PI_RUN_INTEGRATION"
	envRunLiveTokens  = "ACP_GO_PI_RUN_LIVE_TOKENS" //nolint:gosec // Environment variable name, not a credential value.
	envAgentBinary    = "ACP_GO_PI_AGENT_BINARY"
	envPiHome         = "ACP_GO_PI_HOME"
	envPiModel        = "ACP_GO_PI_MODEL"
	envHarnessPath    = "ACP_GO_PI_HARNESS_PATH"
)

var integrationLogger = slog.New(slog.DiscardHandler)

func TestMain(m *testing.M) {
	previousLogger := slog.Default()
	slog.SetDefault(integrationLogger)

	code := m.Run()

	slog.SetDefault(previousLogger)
	os.Exit(code)
}

func requireRunIntegration(t *testing.T) {
	t.Helper()

	if os.Getenv(envRunIntegration) != "1" {
		t.Skipf("set %s=1 to run pi integration tests", envRunIntegration)
	}
}

// requireLiveTokens gates tests that spend model tokens. Only
// `make test-integration-live` sets this variable.
func requireLiveTokens(t *testing.T) {
	t.Helper()

	requireRunIntegration(t)

	if os.Getenv(envRunLiveTokens) != "1" {
		t.Skipf("set %s=1 to run live pi tests that spend model tokens", envRunLiveTokens)
	}
}

// smokePiPath resolves the real pi binary for token-free smoke tests,
// skipping cleanly when it is not installed.
func smokePiPath(t *testing.T) string {
	t.Helper()

	requireRunIntegration(t)

	path, err := resolvePiPath()
	if err != nil {
		t.Skipf("pi CLI not found (%v); install pi or set %s", err, envHarnessPath)
	}

	return path
}

// livePiPath resolves the real pi binary for the live tier. Live runs were
// requested explicitly, so a missing binary fails instead of skipping.
func livePiPath(t *testing.T) string {
	t.Helper()

	path, err := resolvePiPath()
	if err != nil {
		t.Fatalf("live pi tests requested but the pi CLI is missing (%v); install pi or set %s",
			err, envHarnessPath)
	}

	return path
}

func resolvePiPath() (string, error) {
	path := os.Getenv(envHarnessPath)
	if path == "" {
		path = "pi"
	}

	return exec.LookPath(path)
}

func piSourceHome(t *testing.T) string {
	t.Helper()

	if source := os.Getenv(envPiHome); source != "" {
		return source
	}

	home, err := os.UserHomeDir()
	require.NoError(t, err)

	return filepath.Join(home, ".pi")
}

// livePiAuth reads portable auth material from the source pi home. The
// source is only ever read; live sessions run against isolated temp homes.
func livePiAuth(t *testing.T) []byte {
	t.Helper()

	source := piSourceHome(t)

	data, err := os.ReadFile(filepath.Join(source, "agent", "auth.json")) // #nosec G304 -- reads the operator-designated auth source.
	if errors.Is(err, os.ErrNotExist) {
		t.Fatalf("live pi tests requested but %s/agent/auth.json is missing; set %s to a pi home with credentials",
			source, envPiHome)
	}
	require.NoError(t, err)

	return data
}

// livePiAuthSeed injects the portable auth material into each session's
// isolated pi agent directory; seed files are the adapter's credential
// injection surface, so the source pi home itself is never read by pi.
func livePiAuthSeed(t *testing.T) piacp.Option {
	t.Helper()

	return piacp.WithSeedFiles(map[string]string{"auth.json": string(livePiAuth(t))})
}

type agentPipes struct {
	clientInput io.Writer
	agentOutput io.Reader
}

func serveAgentRawForTest(t *testing.T, ctx context.Context, opts ...piacp.Option) agentPipes {
	t.Helper()

	c2aR, c2aW := io.Pipe()
	a2cR, a2cW := io.Pipe()
	serveCtx, stopServe := context.WithCancel(ctx)

	serveErr := make(chan error, 1)
	go func() {
		options := append([]piacp.Option{piacp.WithLogger(integrationLogger)}, opts...)
		serveErr <- piacp.Serve(serveCtx, c2aR, a2cW, options...)
	}()

	t.Cleanup(func() {
		stopServe()
		_ = c2aR.Close()
		_ = c2aW.Close()
		_ = a2cR.Close()
		_ = a2cW.Close()

		select {
		case err := <-serveErr:
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Logf("agent serve returned: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Log("agent serve did not stop within cleanup timeout")
		}
	})

	return agentPipes{clientInput: c2aW, agentOutput: a2cR}
}

func connectAgentForTest(
	t *testing.T,
	ctx context.Context,
	client acp.Client,
	opts ...piacp.Option,
) *acp.ClientSideConnection {
	t.Helper()

	return connectAgentWithInitForTest(t, ctx, client,
		acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber}, opts...)
}

func connectAgentWithInitForTest(
	t *testing.T,
	ctx context.Context,
	client acp.Client,
	init acp.InitializeRequest,
	opts ...piacp.Option,
) *acp.ClientSideConnection {
	t.Helper()

	pipes := serveAgentRawForTest(t, ctx, opts...)
	conn := acp.NewClientSideConnection(client, pipes.clientInput, pipes.agentOutput)

	_, err := conn.Initialize(ctx, init)
	require.NoError(t, err)

	return conn
}

// formElicitationInit advertises form elicitation support, which the agent
// requires before relaying native dialogs as elicitations.
func formElicitationInit() acp.InitializeRequest {
	return acp.InitializeRequest{
		ProtocolVersion: acp.ProtocolVersionNumber,
		ClientCapabilities: acp.ClientCapabilities{
			Elicitation: &acp.ElicitationCapabilities{Form: &acp.ElicitationFormCapabilities{}},
		},
	}
}

// connectFakeAgentForTest serves the wrapper in-process against a fake pi
// harness scenario, on an isolated scratch parent.
func connectFakeAgentForTest(
	t *testing.T,
	ctx context.Context,
	client acp.Client,
	scenario fakeScenario,
	opts ...piacp.Option,
) *acp.ClientSideConnection {
	t.Helper()

	options := append([]piacp.Option{
		piacp.WithExecutablePath(fakePiExecutable(t, scenario)),
		piacp.WithScratchDir(t.TempDir()),
	}, opts...)

	return connectAgentForTest(t, ctx, client, options...)
}

func repoRoot() string {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return ".."
	}

	return filepath.Dir(filepath.Dir(file))
}

var (
	agentBinaryOnce   sync.Once
	agentBinaryCached string
	agentBinaryErr    error
)

// agentBinaryPath returns the wrapper command binary: the operator-supplied
// override (used by test-integration-cover for its GOCOVERDIR-instrumented
// build), or one built on demand. A real binary — never `go run` — so
// killing it in crash tests kills the wrapper itself.
func agentBinaryPath(t *testing.T) string {
	t.Helper()

	if binary := os.Getenv(envAgentBinary); binary != "" {
		return binary
	}

	agentBinaryOnce.Do(func() {
		dir := filepath.Join(repoRoot(), ".tmp", "integration-agent")
		if err := os.MkdirAll(dir, 0o750); err != nil {
			agentBinaryErr = err

			return
		}

		path := filepath.Join(dir, "acp-go-pi")
		build := exec.Command("go", "build", "-o", path, "./cmd/acp-go-pi")
		build.Dir = repoRoot()

		if output, err := build.CombinedOutput(); err != nil {
			agentBinaryErr = errors.New(strings.TrimSpace(string(output)))

			return
		}

		agentBinaryCached = path
	})

	if agentBinaryErr != nil {
		t.Fatalf("build acp-go-pi binary: %v", agentBinaryErr)
	}

	return agentBinaryCached
}

func agentCommand(t *testing.T, ctx context.Context, args ...string) *exec.Cmd {
	t.Helper()

	return exec.CommandContext(ctx, agentBinaryPath(t), args...) // #nosec G204,G702 -- test-built wrapper binary.
}

type liveAgent struct {
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	stdout io.Reader
	stderr lockedBuffer
	wait   chan error
}

func startAgentBinary(t *testing.T, ctx context.Context, args ...string) *liveAgent {
	t.Helper()

	cmd := agentCommand(t, ctx, args...)

	stdin, err := cmd.StdinPipe()
	require.NoError(t, err)
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)

	agent := &liveAgent{cmd: cmd, stdin: stdin, stdout: stdout, wait: make(chan error, 1)}
	cmd.Stderr = &agent.stderr

	require.NoError(t, cmd.Start())

	go func() { agent.wait <- cmd.Wait() }()

	t.Cleanup(func() {
		_ = stdin.Close()

		select {
		case <-agent.wait:
		case <-time.After(5 * time.Second):
			_ = cmd.Process.Kill()
			<-agent.wait
		}
	})

	return agent
}

func (a *liveAgent) stderrString() string {
	return a.stderr.String()
}

type lockedBuffer struct {
	mu   sync.Mutex
	data []byte
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.data = append(b.data, p...)

	return len(p), nil
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()

	return string(b.data)
}

const (
	permissionChoiceAllow  = "allow"
	permissionChoiceDeny   = "deny"
	permissionChoiceCancel = "cancel"
)

type recordedExtension struct {
	Method string
	Params map[string]any
}

type recordingClient struct {
	mu sync.Mutex

	permissionChoice string
	elicitationValue string

	textChunks   []string
	updates      []acp.SessionUpdate
	usageUpdates []acp.SessionUsageUpdate
	permissions  []acp.RequestPermissionRequest
	elicitations []acp.UnstableCreateElicitationRequest
	extensions   []recordedExtension
}

var _ acp.Client = (*recordingClient)(nil)

var _ interface {
	acp.ExtensionMethodHandler
	UnstableCreateElicitation(
		context.Context,
		acp.UnstableCreateElicitationRequest,
	) (acp.UnstableCreateElicitationResponse, error)
} = (*recordingClient)(nil)

func (c *recordingClient) ReadTextFile(context.Context, acp.ReadTextFileRequest) (acp.ReadTextFileResponse, error) {
	return acp.ReadTextFileResponse{}, nil
}

func (c *recordingClient) WriteTextFile(context.Context, acp.WriteTextFileRequest) (acp.WriteTextFileResponse, error) {
	return acp.WriteTextFileResponse{}, nil
}

func (c *recordingClient) RequestPermission(
	_ context.Context,
	params acp.RequestPermissionRequest,
) (acp.RequestPermissionResponse, error) {
	c.mu.Lock()
	c.permissions = append(c.permissions, params)
	choice := c.permissionChoice
	c.mu.Unlock()

	if choice == permissionChoiceCancel {
		return acp.RequestPermissionResponse{Outcome: acp.NewRequestPermissionOutcomeCancelled()}, nil
	}

	for _, option := range params.Options {
		allow := option.Kind == acp.PermissionOptionKindAllowOnce ||
			option.Kind == acp.PermissionOptionKindAllowAlways
		deny := option.Kind == acp.PermissionOptionKindRejectOnce ||
			option.Kind == acp.PermissionOptionKindRejectAlways

		if (choice == permissionChoiceDeny && deny) || (choice != permissionChoiceDeny && allow) {
			return acp.RequestPermissionResponse{
				Outcome: acp.NewRequestPermissionOutcomeSelected(option.OptionId),
			}, nil
		}
	}

	return acp.RequestPermissionResponse{Outcome: acp.NewRequestPermissionOutcomeCancelled()}, nil
}

func (c *recordingClient) SessionUpdate(_ context.Context, params acp.SessionNotification) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.updates = append(c.updates, params.Update)

	switch {
	case params.Update.UsageUpdate != nil:
		c.usageUpdates = append(c.usageUpdates, *params.Update.UsageUpdate)
	case params.Update.AgentMessageChunk != nil && params.Update.AgentMessageChunk.Content.Text != nil:
		c.textChunks = append(c.textChunks, params.Update.AgentMessageChunk.Content.Text.Text)
	}

	return nil
}

func (c *recordingClient) CreateTerminal(context.Context, acp.CreateTerminalRequest) (acp.CreateTerminalResponse, error) {
	return acp.CreateTerminalResponse{TerminalId: "terminal-1"}, nil
}

func (c *recordingClient) KillTerminal(context.Context, acp.KillTerminalRequest) (acp.KillTerminalResponse, error) {
	return acp.KillTerminalResponse{}, nil
}

func (c *recordingClient) TerminalOutput(context.Context, acp.TerminalOutputRequest) (acp.TerminalOutputResponse, error) {
	return acp.TerminalOutputResponse{}, nil
}

func (c *recordingClient) ReleaseTerminal(
	context.Context,
	acp.ReleaseTerminalRequest,
) (acp.ReleaseTerminalResponse, error) {
	return acp.ReleaseTerminalResponse{}, nil
}

func (c *recordingClient) WaitForTerminalExit(
	context.Context,
	acp.WaitForTerminalExitRequest,
) (acp.WaitForTerminalExitResponse, error) {
	return acp.WaitForTerminalExitResponse{}, nil
}

func (c *recordingClient) UnstableCreateElicitation(
	_ context.Context,
	params acp.UnstableCreateElicitationRequest,
) (acp.UnstableCreateElicitationResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.elicitations = append(c.elicitations, params)

	value := c.elicitationValue
	if value == "" {
		value = "Go"
	}

	content := map[string]any{"question_1": value}
	if params.Form != nil {
		for _, required := range params.Form.RequestedSchema.Required {
			content[required] = value
		}
	}

	return acp.UnstableCreateElicitationResponse{
		Accept: &acp.UnstableCreateElicitationAccept{Action: "accept", Content: content},
	}, nil
}

func (c *recordingClient) HandleExtensionMethod(
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
	defer c.mu.Unlock()

	c.extensions = append(c.extensions, recordedExtension{Method: method, Params: decoded})

	return map[string]any{}, nil
}

func (c *recordingClient) text() string {
	c.mu.Lock()
	defer c.mu.Unlock()

	return strings.Join(c.textChunks, "")
}

func (c *recordingClient) permissionCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return len(c.permissions)
}

func (c *recordingClient) permissionSnapshot() []acp.RequestPermissionRequest {
	c.mu.Lock()
	defer c.mu.Unlock()

	return append([]acp.RequestPermissionRequest(nil), c.permissions...)
}

func (c *recordingClient) elicitationSnapshot() []acp.UnstableCreateElicitationRequest {
	c.mu.Lock()
	defer c.mu.Unlock()

	return append([]acp.UnstableCreateElicitationRequest(nil), c.elicitations...)
}

func (c *recordingClient) usageUpdateCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return len(c.usageUpdates)
}

func (c *recordingClient) extensionSnapshot() []recordedExtension {
	c.mu.Lock()
	defer c.mu.Unlock()

	return append([]recordedExtension(nil), c.extensions...)
}

func (c *recordingClient) rawEventCount() int {
	count := 0
	for _, extension := range c.extensionSnapshot() {
		if extension.Method == piacp.RawEventMethod {
			count++
		}
	}

	return count
}

// blockingPermissionClient parks the first permission request until its
// context ends, so tests can crash or cancel the wrapper while a native
// permission dialog is pending.
type blockingPermissionClient struct {
	recordingClient

	permissionRequested chan struct{}
	requestOnce         sync.Once
}

func newBlockingPermissionClient() *blockingPermissionClient {
	return &blockingPermissionClient{permissionRequested: make(chan struct{})}
}

func (c *blockingPermissionClient) RequestPermission(
	ctx context.Context,
	params acp.RequestPermissionRequest,
) (acp.RequestPermissionResponse, error) {
	c.mu.Lock()
	c.permissions = append(c.permissions, params)
	c.mu.Unlock()

	c.requestOnce.Do(func() { close(c.permissionRequested) })

	<-ctx.Done()

	return acp.RequestPermissionResponse{Outcome: acp.NewRequestPermissionOutcomeCancelled()}, nil
}
