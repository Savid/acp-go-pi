//go:build integration && browsercanary

package integration

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	"github.com/moby/moby/api/types/container"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
	"github.com/testcontainers/testcontainers-go/wait"

	piacp "github.com/savid/acp-go-pi"
)

const (
	nativeBrowserFixtureDir  = "native-browser"
	nativeBrowserProbePath   = "/usr/local/bin/native-browser.test"
	nativeBrowserAdapterPath = "/usr/local/bin/acp-go-pi.test"
	nativeBrowserPiPath      = "/usr/local/bin/pi"
	nativeBrowserPiVersion   = "0.82.1"
	nativeBrowserTracePath   = "/tmp/native-browser.trace"
	nativeBrowserHostname    = "native-browser-canary"
	nativeBrowserInsideEnv   = "ACP_GO_PI_NATIVE_BROWSER_INSIDE"
	nativeBrowserTestName    = "TestNativeBrowserLinuxOrdinaryProviderAuthReachesNoUnshimmedLauncher"

	// nativeBrowserOAuthProvider is a device-flow login: the leg that makes pi
	// reach for a browser. nativeBrowserSecretProvider is an api-key login,
	// which runs to a terminal stored result without any network at all.
	nativeBrowserOAuthProvider  = "github-copilot"
	nativeBrowserSecretProvider = "anthropic"

	// nativeBrowserCanaryKey is fixture material. It is not a credential for
	// anything, and the container it is stored in is destroyed with the run.
	nativeBrowserCanaryKey = "acp-go-pi-native-browser-canary-not-a-real-key"
)

// nativeBrowserCanaryUID and nativeBrowserCanaryGID are the fixture's unprivileged
// account. The ordinary launch this canary exercises runs pi as the identity the
// adapter already runs as and claims no other one, but the account stays
// provisioned because the shared integration helpers resolve it.
const (
	nativeBrowserCanaryUID uint32 = 10001
	nativeBrowserCanaryGID uint32 = 10001
)

// TestNativeBrowserLinuxOrdinaryProviderAuthReachesNoUnshimmedLauncher is the
// release canary for Pi's provider-auth browser boundary.
//
// Pi brokers provider auth in ordinary mode against its real native binary, so
// the honest canary is the positive one: the pinned pi release executes, the
// production seven-leg broker enumerates the real native catalog, drives an
// api-key login to its terminal stored result, and starts the oauth leg that
// makes pi reach for a browser. The container installs no browser launcher and
// the syscall trace decides the boundary — every launcher reached must be one
// the adapter's own shim created, because that is the only interception between
// a worker-initiated login and an operator's desktop.
func TestNativeBrowserLinuxOrdinaryProviderAuthReachesNoUnshimmedLauncher(t *testing.T) {
	requireRunIntegration(t)

	if os.Getenv(nativeBrowserInsideEnv) == "1" {
		runOrdinaryProviderAuthCanary(t)

		return
	}
	requireNativeBrowserRuntime(t)

	if runtime.GOOS != "linux" {
		t.Skip("the required CI canary runs the Linux integration binary natively")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	fixture, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			FromDockerfile: testcontainers.FromDockerfile{
				Context:    nativeBrowserFixtureDir,
				Dockerfile: "Dockerfile",
				KeepImage:  true,
			},
			Cmd: []string{"sleep", "infinity"},
			ConfigModifier: func(config *container.Config) {
				config.Hostname = nativeBrowserHostname
			},
			HostConfigModifier: func(config *container.HostConfig) {
				config.ExtraHosts = []string{nativeBrowserHostname + ":127.0.0.1"}
				config.NetworkMode = container.NetworkMode("none")
			},
			WaitingFor: wait.ForExec([]string{"/bin/true"}).WithStartupTimeout(10 * time.Minute),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("start network-disabled native browser fixture: %v", err)
	}
	t.Cleanup(func() {
		if terminateErr := fixture.Terminate(context.WithoutCancel(ctx)); terminateErr != nil {
			t.Errorf("terminate native browser fixture: %v", terminateErr)
		}
	})
	requireNativeBrowserContainment(ctx, t, fixture)

	if copyErr := fixture.CopyFileToContainer(ctx, buildNativeBrowserProbe(t), nativeBrowserProbePath, 0o755); copyErr != nil {
		t.Fatalf("copy native browser probe: %v", copyErr)
	}
	if copyErr := fixture.CopyFileToContainer(ctx, buildNativeBrowserAdapter(t), nativeBrowserAdapterPath, 0o755); copyErr != nil {
		t.Fatalf("copy adapter binary: %v", copyErr)
	}

	code, output, err := fixture.Exec(ctx, []string{
		"/usr/bin/env",
		nativeBrowserInsideEnv + "=1",
		envRunIntegration + "=1",
		envAgentBinary + "=" + nativeBrowserAdapterPath,
		"/usr/bin/strace",
		"-f",
		"-qq",
		"-e", "trace=execve,execveat",
		"-o", nativeBrowserTracePath,
		nativeBrowserProbePath,
		"-test.v",
		"-test.run", "^" + nativeBrowserTestName + "$",
	}, tcexec.Multiplexed())
	if err != nil {
		t.Fatalf("run native browser canary: %v", err)
	}

	logs, err := io.ReadAll(output)
	if err != nil {
		t.Fatalf("read native browser canary output: %v", err)
	}
	t.Log(string(logs))
	if code != 0 {
		t.Fatalf("native browser canary exited %d", code)
	}
	if got := strings.Count(string(logs), "--- PASS: "+nativeBrowserTestName); got != 1 {
		t.Fatalf("native browser canary pass count = %d, want exactly 1: %s", got, logs)
	}
	if strings.Contains(string(logs), "SKIP") || strings.Contains(string(logs), "no tests to run") {
		t.Fatalf("required native browser canary skipped or selected nothing: %s", logs)
	}

	trace := readNativeBrowserTrace(ctx, t, fixture)
	if !traceExecsBase(trace, filepath.Base(nativeBrowserAdapterPath)) {
		t.Fatalf("trace lacks positive adapter exec evidence:\n%s", trace)
	}
	if !traceExecsBase(trace, filepath.Base(nativeBrowserPiPath)) {
		t.Fatalf("the production broker never executed the pinned native pi binary:\n%s", trace)
	}
	if !traceResolvesThroughShim(trace) {
		t.Fatalf(
			"no traced lookup resolved through an %s directory, so the shim did not precede the pi child's PATH "+
				"and a no-launcher result would prove nothing:\n%s",
			nativeBrowserShimPrefix, trace,
		)
	}
	if unshimmed := unshimmedLauncherExecs(trace); len(unshimmed) > 0 {
		t.Fatalf("production provider auth reached browser launchers outside the adapter's shim: %q\n%s",
			unshimmed, trace)
	}
}

// runOrdinaryProviderAuthCanary is the production path itself, executed inside
// the networkless container under the syscall tracer.
func runOrdinaryProviderAuthCanary(t *testing.T) {
	t.Helper()

	requireNativeBrowserPiVersion(t)

	scratch := t.TempDir()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()

	agent := startOrdinaryAgentBinary(t, ctx,
		"-path", nativeBrowserPiPath,
		"-scratch-dir", scratch,
		"-home", t.TempDir(),
		"-provider-auth-root", t.TempDir(),
	)

	conn := acp.NewClientSideConnection(&recordingClient{}, agent.stdin, agent.stdout)
	initialized, err := conn.Initialize(ctx, acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber})
	require.NoError(t, err, "stderr: %s", agent.stderrString())

	piMeta, ok := initialized.AgentCapabilities.Meta["pi"].(map[string]any)
	require.True(t, ok, "stderr: %s", agent.stderrString())
	providerAuth, ok := piMeta["providerAuth"].(map[string]any)
	require.True(t, ok, "ordinary mode must advertise the provider-auth broker: %#v", piMeta)
	require.ElementsMatch(t, []any{
		piacp.AuthMethodsMethod, piacp.AuthAuthorizeMethod, piacp.AuthCallbackMethod,
		piacp.AuthStatusMethod, piacp.AuthCancelMethod, piacp.AuthInventoryMethod,
		piacp.AuthDisconnectMethod,
	}, providerAuth["methods"])

	session, err := conn.NewSession(ctx, piacp.NewSessionRequest(t.TempDir()))
	require.NoError(t, err, "stderr: %s", agent.stderrString())
	sessionID := string(session.SessionId)

	requireSessionBrowserShim(t, scratch)

	methods := nativeBrowserMethods(t, ctx, conn, sessionID)
	requireNativeCatalogEntry(t, methods, nativeBrowserSecretProvider, "api")
	requireNativeCatalogEntry(t, methods, nativeBrowserOAuthProvider, "oauth")

	nativeBrowserOAuthLeg(t, ctx, conn, sessionID, methods)
	nativeBrowserSecretLeg(t, ctx, conn, sessionID, methods)
}

// nativeBrowserOAuthLeg starts the login that makes pi reach for a browser. The
// container has no route to the provider, so the leg has to fail; what matters
// is that it fails through the adapter's closed provider-auth error and that
// the trace records no launcher the adapter did not shadow.
func nativeBrowserOAuthLeg(
	t *testing.T,
	ctx context.Context,
	conn *acp.ClientSideConnection,
	sessionID string,
	methods attendedMethodsWire,
) {
	t.Helper()

	_, err := conn.CallExtension(ctx, piacp.AuthAuthorizeMethod, map[string]any{
		"sessionId":          sessionID,
		"providerId":         nativeBrowserOAuthProvider,
		"connectionId":       "native-browser-canary",
		"methodsGeneration":  methods.Generation,
		"method":             "oauth",
		"authorizeRequestId": "native-browser-oauth",
	})
	require.Error(t, err, "a networkless device-flow login must not report an authorization")

	var requestErr *acp.RequestError
	require.ErrorAs(t, err, &requestErr)

	data, ok := requestErr.Data.(map[string]any)
	require.True(t, ok, "provider-auth failures carry structured data: %#v", requestErr.Data)
	require.Equal(t, "pi_auth_failed", data["error"])
	require.Equal(t, nativeBrowserOAuthProvider, data["providerId"])
}

// nativeBrowserSecretLeg drives the api-key login all the way to its terminal
// result: the boundary a login crosses when no browser is involved at all.
func nativeBrowserSecretLeg(
	t *testing.T,
	ctx context.Context,
	conn *acp.ClientSideConnection,
	sessionID string,
	methods attendedMethodsWire,
) {
	t.Helper()

	var authorization attendedAuthorizeWire
	nativeBrowserCall(t, ctx, conn, piacp.AuthAuthorizeMethod, map[string]any{
		"sessionId":          sessionID,
		"providerId":         nativeBrowserSecretProvider,
		"connectionId":       "native-browser-canary",
		"methodsGeneration":  methods.Generation,
		"method":             "api",
		"authorizeRequestId": "native-browser-secret",
	}, &authorization)
	require.Equal(t, "secret", authorization.Interaction)
	require.NotEmpty(t, authorization.FlowID)
	require.Empty(t, authorization.URL, "an api-key login presents no URL to open")

	nativeBrowserCall(t, ctx, conn, piacp.AuthCallbackMethod, map[string]any{
		"sessionId": sessionID, "providerId": nativeBrowserSecretProvider,
		"method": "api", "flowId": authorization.FlowID, "input": nativeBrowserCanaryKey,
	}, nil)

	var status attendedStatusWire
	nativeBrowserCall(t, ctx, conn, piacp.AuthStatusMethod, map[string]any{
		"sessionId": sessionID, "providerId": nativeBrowserSecretProvider,
		"flowId": authorization.FlowID,
	}, &status)
	require.Equal(t, "saved", status.State)

	// The stored key is confirmed against the native residence, while the
	// browser leg that never completed is still addressable and still proves
	// nothing was stored for it.
	inventory := nativeBrowserInventory(t, ctx, conn, sessionID)
	require.Equal(t, "confirmed_present", inventory[nativeBrowserSecretProvider])
	require.Equal(t, "not_confirmed", inventory[nativeBrowserOAuthProvider])

	nativeBrowserCall(t, ctx, conn, piacp.AuthDisconnectMethod, map[string]any{
		"sessionId": sessionID, "providerId": nativeBrowserSecretProvider,
		"connectionId": "native-browser-canary", "bindingGeneration": 1,
	}, nil)

	afterRemoval := nativeBrowserInventory(t, ctx, conn, sessionID)
	require.NotContains(t, afterRemoval, nativeBrowserSecretProvider)
	require.Equal(t, "not_confirmed", afterRemoval[nativeBrowserOAuthProvider])
}

// nativeBrowserInventory reads the values-free ledger as provider to proof.
func nativeBrowserInventory(
	t *testing.T,
	ctx context.Context,
	conn *acp.ClientSideConnection,
	sessionID string,
) map[string]string {
	t.Helper()

	var inventory attendedInventoryWire
	nativeBrowserCall(t, ctx, conn, piacp.AuthInventoryMethod,
		map[string]any{"sessionId": sessionID}, &inventory)

	proofs := make(map[string]string, len(inventory.Entries))
	for _, entry := range inventory.Entries {
		proofs[entry.ProviderID] = entry.ProofSource
	}
	require.Len(t, proofs, len(inventory.Entries), "the ledger addressed one provider twice")

	return proofs
}

func nativeBrowserMethods(
	t *testing.T,
	ctx context.Context,
	conn *acp.ClientSideConnection,
	sessionID string,
) attendedMethodsWire {
	t.Helper()

	var methods attendedMethodsWire
	nativeBrowserCall(t, ctx, conn, piacp.AuthMethodsMethod,
		map[string]any{"sessionId": sessionID}, &methods)
	require.NotEmpty(t, methods.Generation)

	return methods
}

func nativeBrowserCall(
	t *testing.T,
	ctx context.Context,
	conn *acp.ClientSideConnection,
	method string,
	params any,
	out any,
) {
	t.Helper()

	raw, err := conn.CallExtension(ctx, method, params)
	require.NoError(t, err, method)

	if out != nil {
		require.NoError(t, json.Unmarshal(raw, out), method)
	}
}

// requireNativeCatalogEntry proves the enumerated methods came from the real pi
// release rather than an adapter-side stub.
func requireNativeCatalogEntry(t *testing.T, methods attendedMethodsWire, providerID, methodType string) {
	t.Helper()

	entries, ok := methods.Providers[providerID]
	require.True(t, ok, "native catalog has no %q: %#v", providerID, methods.Providers)

	for _, entry := range entries {
		if entry.Type == methodType {
			require.NotEmpty(t, entry.Label, "native catalog entries carry the harness's own labels")

			return
		}
	}

	t.Fatalf("native catalog %q offers no %q method: %#v", providerID, methodType, entries)
}

// requireSessionBrowserShim proves the interception exists before the login
// legs run: a session that started without its shim would make the trace's
// no-launcher result meaningless rather than reassuring.
func requireSessionBrowserShim(t *testing.T, scratch string) {
	t.Helper()

	shims := make([]string, 0, 1)
	err := filepath.WalkDir(scratch, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() && strings.HasPrefix(entry.Name(), nativeBrowserShimPrefix) {
			shims = append(shims, path)

			return filepath.SkipDir
		}

		return nil
	})
	require.NoError(t, err)
	require.Len(t, shims, 1, "the session did not start with exactly one browser-launcher shim in %s", scratch)
	shim := shims[0]

	shadowed := make([]string, 0, len(nativeBrowserLauncherNames))
	for _, entry := range readNativeBrowserDir(t, shim) {
		info, err := entry.Info()
		require.NoError(t, err)
		require.NotZero(t, info.Mode().Perm()&0o111, "shimmed launcher %q is not executable", entry.Name())
		shadowed = append(shadowed, entry.Name())
	}
	require.Subset(t, nativeBrowserLauncherNames, shadowed,
		"the shim shadows a name the canary does not trace")
	require.NotEmpty(t, shadowed)
}

func readNativeBrowserDir(t *testing.T, dir string) []os.DirEntry {
	t.Helper()

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)

	return entries
}

func requireNativeBrowserPiVersion(t *testing.T) {
	t.Helper()

	output, err := exec.CommandContext(t.Context(), nativeBrowserPiPath, "--version").CombinedOutput()
	require.NoError(t, err, "run the pinned pi release: %s", output)
	require.Equal(t, nativeBrowserPiVersion, strings.TrimSpace(string(output)))
}

// requireNativeBrowserContainment fails before the canary runs if the fixture
// is not the credential-free, networkless one the evidence assumes.
func requireNativeBrowserContainment(ctx context.Context, t *testing.T, fixture testcontainers.Container) {
	t.Helper()

	inspection, err := fixture.Inspect(ctx)
	require.NoError(t, err)
	require.NotNil(t, inspection.Config)
	require.Equal(t, nativeBrowserHostname, inspection.Config.Hostname)
	require.NotNil(t, inspection.HostConfig)
	require.Equal(t, container.NetworkMode("none"), inspection.HostConfig.NetworkMode)
	require.False(t, inspection.HostConfig.Privileged)
	require.Empty(t, inspection.HostConfig.Binds, "the canary mounts no host path")
	require.Empty(t, inspection.Mounts, "the canary mounts no host home or credential store")
}

func requireNativeBrowserRuntime(t *testing.T) {
	t.Helper()
	requireRunIntegration(t)
	if _, err := exec.LookPath("docker"); err != nil {
		t.Fatalf("%s=1 requires a container runtime: %v", envRunIntegration, err)
	}
}

func buildNativeBrowserProbe(t *testing.T) string {
	t.Helper()

	out := filepath.Join(t.TempDir(), "native-browser.test")
	command := exec.CommandContext(t.Context(), "go", "test", "-c", "-tags=integration,browsercanary", "-o", out, "./integration")
	command.Dir = repoRoot()
	command.Env = append(os.Environ(), "GOWORK=off", "GOOS=linux", "GOARCH="+runtime.GOARCH, "CGO_ENABLED=0")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("build native browser probe: %v: %s", err, output)
	}

	return out
}

func buildNativeBrowserAdapter(t *testing.T) string {
	t.Helper()

	out := filepath.Join(t.TempDir(), "acp-go-pi")
	command := exec.CommandContext(t.Context(), "go", "build", "-o", out, "./cmd/acp-go-pi")
	command.Dir = repoRoot()
	command.Env = append(os.Environ(), "GOWORK=off", "GOOS=linux", "GOARCH="+runtime.GOARCH, "CGO_ENABLED=0")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("build adapter binary: %v: %s", err, output)
	}

	return out
}

func readNativeBrowserTrace(ctx context.Context, t *testing.T, fixture testcontainers.Container) string {
	t.Helper()

	code, output, err := fixture.Exec(ctx, []string{"/bin/sh", "-c", "exec /bin/cat " + nativeBrowserTracePath}, tcexec.Multiplexed())
	if err != nil {
		t.Fatalf("read native browser trace: %v", err)
	}
	contents, err := io.ReadAll(output)
	if err != nil {
		t.Fatalf("read native browser trace output: %v", err)
	}
	if code != 0 {
		t.Fatalf("read native browser trace exited %d: %s", code, contents)
	}

	return string(contents)
}
