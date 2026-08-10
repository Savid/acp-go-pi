//go:build integration

package integration

import (
	"context"
	"fmt"
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
)

const (
	nativeBrowserFixtureDir         = "native-browser"
	nativeBrowserProbePath          = "/usr/local/bin/native-browser.test"
	nativeBrowserAdapterPath        = "/usr/local/bin/acp-go-pi.test"
	nativeBrowserPiPath             = "/opt/pi/pi"
	nativeBrowserTracePath          = "/tmp/native-browser.trace"
	nativeBrowserHostname           = "native-browser-canary"
	nativeBrowserInsideEnv          = "ACP_GO_PI_NATIVE_BROWSER_INSIDE"
	nativeBrowserTestName           = "TestNativeLinuxExplicitIsolationDoesNotDispatchProviderAuth"
	nativeBrowserCanaryUID   uint32 = 10001
	nativeBrowserCanaryGID   uint32 = 10001
)

var nativeBrowserLauncherNames = []string{
	"open",
	"xdg-open",
	"x-www-browser",
	"www-browser",
	"sensible-browser",
	"gio",
	"firefox",
	"google-chrome",
	"google-chrome-stable",
	"chromium",
	"chromium-browser",
}

func TestNativeLinuxExplicitIsolationDoesNotDispatchProviderAuth(t *testing.T) {
	requireRunIntegration(t)

	if os.Getenv(nativeBrowserInsideEnv) == "1" {
		runExplicitIsolationAuthCanary(t)

		return
	}
	requireNativeBrowserRuntime(t)

	if runtime.GOOS != "linux" {
		t.Skip("the required CI canary runs the Linux integration binary natively")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	authorityVolume := fmt.Sprintf("acp-go-pi-browser-%d-%d", os.Getpid(), time.Now().UnixNano())
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
				config.PidMode = container.PidMode("host")
			},
			Mounts: testcontainers.ContainerMounts{
				testcontainers.VolumeMount(authorityVolume, "/var/lib/acp-go/agent-identities"),
			},
			WaitingFor: wait.ForExec([]string{"/bin/true"}).WithStartupTimeout(5 * time.Minute),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("start network-disabled native browser fixture: %v", err)
	}
	t.Cleanup(func() {
		if terminateErr := fixture.Terminate(
			context.WithoutCancel(ctx), testcontainers.RemoveVolumes(authorityVolume),
		); terminateErr != nil {
			t.Errorf("terminate native browser fixture: %v", terminateErr)
		}
	})
	code, output, err := fixture.Exec(ctx, []string{
		"/usr/bin/install", "-d", "-o", "root", "-g", "root", "-m", "0700",
		"/var/lib/acp-go", "/var/lib/acp-go/agent-identities",
	}, tcexec.Multiplexed())
	if err != nil {
		t.Fatalf("prepare native identity authority: %v", err)
	}
	authorityOutput, readErr := io.ReadAll(output)
	if readErr != nil {
		t.Fatalf("read native identity authority preparation output: %v", readErr)
	}
	if code != 0 {
		t.Fatalf("prepare native identity authority exited %d: %s", code, authorityOutput)
	}

	if copyErr := fixture.CopyFileToContainer(ctx, buildNativeBrowserProbe(t), nativeBrowserProbePath, 0o755); copyErr != nil {
		t.Fatalf("copy native browser probe: %v", copyErr)
	}
	if copyErr := fixture.CopyFileToContainer(ctx, buildNativeBrowserAdapter(t), nativeBrowserAdapterPath, 0o755); copyErr != nil {
		t.Fatalf("copy adapter binary: %v", copyErr)
	}

	code, output, err = fixture.Exec(ctx, []string{
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
	if traceExecsBase(trace, filepath.Base(nativeBrowserPiPath)) {
		t.Fatalf("unadvertised isolated-auth dispatch launched the native Pi binary:\n%s", trace)
	}
	for _, launcher := range nativeBrowserLauncherNames {
		if traceExecsBase(trace, launcher) {
			t.Fatalf("unadvertised isolated-auth dispatch attempted browser launcher %q:\n%s", launcher, trace)
		}
	}
}

func runExplicitIsolationAuthCanary(t *testing.T) {
	t.Helper()

	root, err := os.MkdirTemp("/tmp", "acp-go-pi-native-browser-")
	if err != nil {
		t.Fatalf("create native browser root: %v", err)
	}
	t.Cleanup(func() {
		if removeErr := os.RemoveAll(root); removeErr != nil {
			t.Errorf("remove native browser root: %v", removeErr)
		}
	})
	if chmodErr := os.Chmod(root, 0o755); chmodErr != nil {
		t.Fatalf("make native browser root traversable: %v", chmodErr)
	}

	policyRoot, err := os.MkdirTemp("/root", "acp-go-pi-native-policy-")
	if err != nil {
		t.Fatalf("create trusted policy directory: %v", err)
	}
	t.Cleanup(func() {
		if removeErr := os.RemoveAll(policyRoot); removeErr != nil {
			t.Errorf("remove trusted policy directory: %v", removeErr)
		}
	})
	scratch := filepath.Join(root, "scratch")
	if mkdirErr := os.Mkdir(scratch, 0o755); mkdirErr != nil {
		t.Fatalf("create trusted scratch directory: %v", mkdirErr)
	}

	policyPath := filepath.Join(policyRoot, "policy.json")
	policy := fmt.Sprintf(
		`{"uid":%d,"gid":%d,"standaloneOwnerId":"acp-go-pi-native-browser-canary","standaloneStateRoot":"/home/native-canary","baseEnvironment":{"HOME":"/home/native-canary","LANG":"C.UTF-8","LOGNAME":"native-canary","PATH":"/usr/local/bin:/usr/bin:/bin","USER":"native-canary"},"inheritEnvironment":[]}`,
		nativeBrowserCanaryUID,
		nativeBrowserCanaryGID,
	)
	if writeErr := os.WriteFile(policyPath, []byte(policy), 0o600); writeErr != nil {
		t.Fatalf("write process-isolation policy: %v", writeErr)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	agent := startAgentBinary(t, ctx,
		"-path", nativeBrowserPiPath,
		"-process-isolation-config", policyPath,
		"-scratch-dir", scratch,
	)

	conn := acp.NewClientSideConnection(&recordingClient{}, agent.stdin, agent.stdout)
	_, err = conn.Initialize(ctx, acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber})
	require.NoError(t, err, "stderr: %s", agent.stderrString())

	_, err = conn.CallExtension(ctx, "_pi/auth/methods", map[string]any{"sessionId": "no-native-session"})
	var requestErr *acp.RequestError
	require.ErrorAs(t, err, &requestErr)
	require.Equal(t, -32601, requestErr.Code)
	require.Equal(t, "Method not found", requestErr.Message)
	require.NoError(t, agent.stdin.Close())
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
	command := exec.CommandContext(t.Context(), "go", "test", "-c", "-tags=integration", "-o", out, "./integration")
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

func traceExecsBase(trace, name string) bool {
	for line := range strings.Lines(trace) {
		executable, ok := traceExecPath(line)
		if ok && filepath.Base(executable) == name {
			return true
		}
	}

	return false
}

func traceExecPath(line string) (string, bool) {
	call := strings.TrimSpace(line)
	index := strings.Index(call, "execve(")
	if execveat := strings.Index(call, "execveat("); index < 0 || execveat >= 0 && execveat < index {
		index = execveat
	}
	if index < 0 {
		return "", false
	}

	arguments := call[index:]
	start := strings.IndexByte(arguments, '"')
	if start < 0 {
		return "", false
	}
	arguments = arguments[start+1:]
	end := strings.IndexByte(arguments, '"')
	if end < 0 {
		return "", false
	}

	return arguments[:end], true
}
