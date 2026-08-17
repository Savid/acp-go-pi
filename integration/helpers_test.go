//go:build integration

package integration

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	piacp "github.com/savid/acp-go-pi"
	internalpi "github.com/savid/acp-go-pi/internal/pi"
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

type integrationTargetIdentity struct {
	uid      uint32
	gid      uint32
	username string
	home     string
}

func integrationLinuxTargetIdentity(t *testing.T) integrationTargetIdentity {
	t.Helper()

	if runtime.GOOS != "linux" || os.Geteuid() != 0 {
		t.Skip("production process-isolation integration requires a Linux root supervisor")
	}
	account, err := user.LookupId(strconv.FormatUint(uint64(nativeBrowserCanaryUID), 10))
	if err != nil {
		t.Skip("production process-isolation integration requires the provisioned native-canary account")
	}
	accountGID, err := strconv.ParseUint(account.Gid, 10, 32)
	require.NoError(t, err)
	require.Equal(t, nativeBrowserCanaryGID, uint32(accountGID))
	require.Equal(t, "native-canary", account.Username)
	require.Equal(t, "/home/native-canary", filepath.Clean(account.HomeDir))

	return integrationTargetIdentity{
		uid: nativeBrowserCanaryUID, gid: nativeBrowserCanaryGID,
		username: account.Username, home: filepath.Clean(account.HomeDir),
	}
}

func integrationProductionEnvironment(target integrationTargetIdentity) map[string]string {
	return map[string]string{
		"HOME":    target.home,
		"LANG":    "C.UTF-8",
		"LOGNAME": target.username,
		"PATH":    "/usr/local/bin:/usr/local/go/bin:/usr/bin:/bin",
		"USER":    target.username,
	}
}

func integrationScratchDir(t *testing.T) string {
	t.Helper()

	dir, err := os.MkdirTemp("/tmp", "acp-go-pi-integration-scratch-")
	require.NoError(t, err)
	require.NoError(t, os.Chmod(dir, 0o755))
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(dir)) })

	return dir
}

func integrationWorkspaceDir(t *testing.T) string {
	t.Helper()

	dir, err := os.MkdirTemp("/tmp", "acp-go-pi-integration-workspace-")
	require.NoError(t, err)
	if runtime.GOOS == "linux" && os.Geteuid() == 0 {
		target := integrationLinuxTargetIdentity(t)
		require.NoError(t, os.Chown(dir, int(target.uid), int(target.gid)))
	}
	require.NoError(t, os.Chmod(dir, 0o700))
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(dir)) })

	return dir
}

func integrationBaseEnvironment(t *testing.T) map[string]string {
	t.Helper()

	account, err := user.LookupId(strconv.Itoa(os.Geteuid()))
	require.NoError(t, err)
	home := t.TempDir()

	return map[string]string{
		"HOME":    home,
		"LANG":    "C.UTF-8",
		"LOGNAME": account.Username,
		"PATH":    os.Getenv("PATH"),
		"USER":    account.Username,
	}
}

func integrationProcessIsolationOption(t *testing.T) piacp.Option {
	t.Helper()

	target := integrationLinuxTargetIdentity(t)

	return piacp.WithProcessIsolation(piacp.ProcessIsolation{
		UID:                 target.uid,
		GID:                 target.gid,
		BaseEnvironment:     integrationProductionEnvironment(target),
		StandaloneOwnerID:   "acp-go-pi-integration",
		StandaloneStateRoot: target.home,
	})
}

func integrationContainmentSpec(t *testing.T) internalpi.ContainmentSpec {
	t.Helper()
	parent := t.TempDir()
	root, err := os.MkdirTemp(parent, "acp-go-pi-runtime-*")
	require.NoError(t, err)
	identity := make([]byte, 16)
	_, err = rand.Read(identity)
	require.NoError(t, err)

	// Explicit isolation is a Linux-only mode with no fallback, so off Linux the
	// spec carries the ordinary environment instead. Combining it with Darwin
	// best effort would ask for two containment modes at once, which the launch
	// boundary refuses rather than silently picking one.
	base := integrationBaseEnvironment(t)

	var isolation *internalpi.ProcessIsolation

	if runtime.GOOS == "linux" {
		uid, gid := os.Geteuid(), os.Getegid()
		if uid == 0 || gid == 0 {
			uid, gid = 65534, 65534
		}
		isolation = &internalpi.ProcessIsolation{
			UID: uint32(uid), GID: uint32(gid),
			BaseEnvironment:      base,
			TestOnlyNoCredential: true,
		}
	}

	return internalpi.ContainmentSpec{
		DarwinBestEffort:    runtime.GOOS == "darwin",
		ScratchParent:       parent,
		GenerationRoot:      root,
		RuntimeID:           hex.EncodeToString(identity),
		LifecycleKind:       "discovery",
		Isolation:           isolation,
		OrdinaryEnvironment: base,
	}
}

// integrationContainmentEnvironment is the base environment a spec hands its
// child, whichever containment mode the platform selected. Tests that pin a
// native home ask for it here rather than reaching into one mode's field, which
// is nil on the platforms that use the other.
func integrationContainmentEnvironment(containment internalpi.ContainmentSpec) map[string]string {
	if containment.Isolation != nil {
		return containment.Isolation.BaseEnvironment
	}

	return containment.OrdinaryEnvironment
}

func integrationVersionProbeSpec(t *testing.T) (string, internalpi.ContainmentSpec) {
	t.Helper()
	containment := integrationContainmentSpec(t)
	agentDir := filepath.Join(containment.GenerationRoot, "probe-agent")
	require.NoError(t, (internalpi.AgentDir{Root: agentDir}).Write())

	return agentDir, containment
}

func expectedBuiltinCommandNames(t *testing.T, executable string) []string {
	t.Helper()

	agentDir, containment := integrationVersionProbeSpec(t)
	version, err := internalpi.ProbeVersion(t.Context(), executable, agentDir, containment)
	require.NoError(t, err)
	if internalpi.CheckMinimumVersion(version, "0.81.0") == nil {
		return []string{"llama"}
	}

	return []string{}
}

// integrationContainmentOption opts the in-process agent into Darwin
// containment. Darwin containment fails closed without it, and the flag the
// binary tier passes is the same opt-in on the other side of the process
// boundary.
func integrationContainmentOption() piacp.Option {
	if runtime.GOOS == "darwin" {
		return piacp.WithDarwinBestEffortContainment()
	}

	return func(*piacp.Options) {}
}

func TestMain(m *testing.M) {
	previousLogger := slog.Default()
	slog.SetDefault(integrationLogger)

	code := m.Run()

	slog.SetDefault(previousLogger)
	if fakePiTargetBinaryRoot != "" {
		_ = os.RemoveAll(fakePiTargetBinaryRoot)
	}
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
	baseOptions := []piacp.Option{
		piacp.WithLogger(integrationLogger), integrationContainmentOption(), integrationProcessIsolationOption(t),
	}

	return serveAgentWithBaseOptionsForTest(t, ctx, baseOptions, opts...)
}

// serveEmbeddedAgentRawForTest runs the same real adapter boundary without
// the Linux-only distinct-identity policy. Darwin still uses the adapter's
// explicit best-effort containment because native launches fail closed there.
func serveEmbeddedAgentRawForTest(t *testing.T, ctx context.Context, opts ...piacp.Option) agentPipes {
	t.Helper()
	baseOptions := []piacp.Option{piacp.WithLogger(integrationLogger), integrationContainmentOption()}

	return serveAgentWithBaseOptionsForTest(t, ctx, baseOptions, opts...)
}

func serveAgentWithBaseOptionsForTest(
	t *testing.T,
	ctx context.Context,
	baseOptions []piacp.Option,
	opts ...piacp.Option,
) agentPipes {
	t.Helper()

	c2aR, c2aW := io.Pipe()
	a2cR, a2cW := io.Pipe()
	serveCtx, stopServe := context.WithCancel(ctx)

	serveErr := make(chan error, 1)
	go func() {
		options := append(baseOptions, opts...)
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

func connectEmbeddedAgentForTest(
	t *testing.T,
	ctx context.Context,
	client acp.Client,
	opts ...piacp.Option,
) *acp.ClientSideConnection {
	t.Helper()

	return initializeAgentPipesForTest(t, ctx, client,
		acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber},
		serveEmbeddedAgentRawForTest(t, ctx, opts...),
	)
}

func connectAgentWithInitForTest(
	t *testing.T,
	ctx context.Context,
	client acp.Client,
	init acp.InitializeRequest,
	opts ...piacp.Option,
) *acp.ClientSideConnection {
	t.Helper()

	return initializeAgentPipesForTest(t, ctx, client, init, serveAgentRawForTest(t, ctx, opts...))
}

func initializeAgentPipesForTest(
	t *testing.T,
	ctx context.Context,
	client acp.Client,
	init acp.InitializeRequest,
	pipes agentPipes,
) *acp.ClientSideConnection {
	t.Helper()
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
		piacp.WithScratchDir(integrationScratchDir(t)),
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

func standaloneAgentCommand(t *testing.T, ctx context.Context, args ...string) *exec.Cmd {
	t.Helper()

	target := integrationLinuxTargetIdentity(t)
	policyRoot, err := os.MkdirTemp("/root", "acp-go-pi-integration-policy-")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(policyRoot)) })
	policyPath := filepath.Join(policyRoot, "policy.json")
	policy, err := json.Marshal(struct {
		UID                 uint32            `json:"uid"`
		GID                 uint32            `json:"gid"`
		BaseEnvironment     map[string]string `json:"baseEnvironment"`
		InheritEnvironment  []string          `json:"inheritEnvironment"`
		StandaloneOwnerID   string            `json:"standaloneOwnerId"`
		StandaloneStateRoot string            `json:"standaloneStateRoot"`
	}{
		UID: target.uid, GID: target.gid,
		BaseEnvironment: integrationProductionEnvironment(target), InheritEnvironment: []string{},
		StandaloneOwnerID: "acp-go-pi-integration", StandaloneStateRoot: target.home,
	})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(policyPath, policy, 0o600))

	args = append(args, "-process-isolation-config", policyPath)

	return agentCommand(t, ctx, args...)
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

	hasPolicy := false
	for index, arg := range args {
		if arg == "-process-isolation-config" && index+1 < len(args) {
			hasPolicy = true

			break
		}
	}

	var cmd *exec.Cmd
	if hasPolicy {
		cmd = agentCommand(t, ctx, args...)
	} else {
		cmd = standaloneAgentCommand(t, ctx, args...)
	}

	return startAgentProcess(t, cmd)
}

// startOrdinaryAgentBinary launches the wrapper in ordinary mode: the pi child
// runs as the identity the adapter already runs as. That is the containment
// mode this repository's provider-auth surface is defined for — explicit
// isolation refuses to broker it — and it is also the only mode the binary
// tier has on Darwin, so fixing it here keeps a provider-auth test from
// silently degrading into a skip off Linux.
func startOrdinaryAgentBinary(t *testing.T, ctx context.Context, args ...string) *liveAgent {
	t.Helper()

	for _, arg := range args {
		require.NotEqual(t, "-process-isolation-config", arg,
			"the ordinary launch helper is the no-isolation mode; pass a policy through startAgentBinary instead")
	}

	return startAgentProcess(t, agentCommand(t, ctx, args...))
}

func startAgentProcess(t *testing.T, cmd *exec.Cmd) *liveAgent {
	t.Helper()

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

	textChunks    []string
	updates       []acp.SessionUpdate
	notifications []acp.SessionNotification
	usageUpdates  []acp.SessionUsageUpdate
	permissions   []acp.RequestPermissionRequest
	elicitations  []acp.UnstableCreateElicitationRequest
	extensions    []recordedExtension
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
	c.notifications = append(c.notifications, params)

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

func (c *recordingClient) notificationSnapshot() []acp.SessionNotification {
	c.mu.Lock()
	defer c.mu.Unlock()

	return append([]acp.SessionNotification(nil), c.notifications...)
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
