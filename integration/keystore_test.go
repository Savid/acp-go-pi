//go:build integration

package integration

import (
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	envRunKeystore    = "ACP_GO_PI_RUN_KEYSTORE"
	keystoreEnvFile   = "/run/acp-go-pi-keystore/env"
	keystoreRoundTrip = "/usr/local/bin/roundtrip.sh"
	keystoreProbePath = "/usr/local/bin/residence.test"
)

// requireRunKeystore gates the tier on both env vars.
func requireRunKeystore(t *testing.T) {
	t.Helper()
	requireRunIntegration(t)

	if os.Getenv(envRunKeystore) != "1" {
		t.Skipf("set %s=1 to run the keystore credential-residence tier", envRunKeystore)
	}
}

func requireKeystoreRuntime(t *testing.T) {
	t.Helper()
	requireRunKeystore(t)

	// The tier fails rather than skips once its gate is set: a silently green
	// residence suite is worse than a red one.
	if _, err := exec.LookPath("docker"); err != nil {
		t.Fatalf("%s=1 requires a container runtime: %v", envRunKeystore, err)
	}
}

// TestKeystoreLinuxCredentialResidence runs the residence matrix against a live
// Secret Service. pi's credential storage admits a file backend and an
// in-memory one and nothing else, so the claim under test is an identity: the
// store under PI_CODING_AGENT_DIR answers the same way whether or not a secret
// service is on the box. Only running the read path beside a real service
// establishes it, and a container's session bus does not cross the host
// boundary, so the read path runs inside the fixture.
func TestKeystoreLinuxCredentialResidence(t *testing.T) {
	requireKeystoreRuntime(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	container := startKeystoreFixture(ctx, t)

	if err := container.CopyFileToContainer(ctx, buildResidenceProbe(t), keystoreProbePath, 0o755); err != nil {
		t.Fatalf("copy residence probe: %v", err)
	}

	// Both Linux configurations run in this one container and differ by exactly
	// one thing: whether the session bus that reaches the Secret Service is
	// exported. A run that exercises one side of that fork proves nothing about
	// the identity the matrix claims.
	for _, configuration := range []struct {
		name string
		bus  bool
	}{
		{name: "keystore-absent"},
		{name: "keystore-present", bus: true},
	} {
		t.Run(configuration.name, func(t *testing.T) {
			runResidenceMatrix(ctx, t, container, configuration.bus)
		})
	}
}

// runResidenceMatrix executes the probe in one configuration and requires it to
// have reported a pass. An exit status alone goes green on a skip, which is the
// silent success this tier exists to prevent.
func runResidenceMatrix(ctx context.Context, t *testing.T, container testcontainers.Container, bus bool) {
	t.Helper()

	script := "export " + envRunIntegration + "=1 " + envRunKeystore + "=1; "
	if bus {
		script += ". " + keystoreEnvFile + "; export DBUS_SESSION_BUS_ADDRESS; "
	}

	script += "exec " + keystoreProbePath + " -test.v -test.run '^TestKeystoreResidenceMatrix$'"

	// The raw exec stream is frame-multiplexed: every read carries an eight-byte
	// header, so an unmultiplexed reader interleaves those bytes into the logs.
	code, output, err := container.Exec(ctx, []string{"/bin/sh", "-c", script}, tcexec.Multiplexed())
	if err != nil {
		t.Fatalf("run residence matrix: %v", err)
	}

	logs, readErr := io.ReadAll(output)
	if readErr != nil {
		t.Fatalf("read residence output: %v", readErr)
	}

	t.Log(string(logs))

	if code != 0 {
		t.Fatalf("residence matrix exited %d", code)
	}

	if !strings.Contains(string(logs), "--- PASS: TestKeystoreResidenceMatrix") {
		t.Fatal("the residence matrix did not run in this configuration")
	}
}

// TestKeystoreLinuxArtifactCarriesNoSecretServiceClient pins the mechanism
// behind that identity from this repo's own side: the adapter compiled for
// Linux links no Secret Service client, so a live service has no code path to
// reach whatever it offers.
func TestKeystoreLinuxArtifactCarriesNoSecretServiceClient(t *testing.T) {
	requireRunKeystore(t)

	binary := filepath.Join(t.TempDir(), "acp-go-pi-linux")

	build := exec.CommandContext(t.Context(), "go", "build", "-o", binary, "./cmd/acp-go-pi")
	build.Dir = repoRoot()
	build.Env = append(os.Environ(), "GOOS=linux", "GOARCH="+runtime.GOARCH, "CGO_ENABLED=0")

	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build the Linux artifact: %v: %s", err, output)
	}

	contents, err := os.ReadFile(binary) // #nosec G304 -- the path is this test's own temp dir.
	if err != nil {
		t.Fatalf("read the Linux artifact: %v", err)
	}

	for _, symbol := range []string{"libsecret", "org.freedesktop.secrets", "gnome-keyring"} {
		if strings.Contains(string(contents), symbol) {
			t.Fatalf("the Linux artifact carries %q", symbol)
		}
	}
}

// startKeystoreFixture builds and starts the container this tier runs inside.
// The base image is pinned by digest in the fixture's own Dockerfile, and
// KeepImage keeps the build off the clock of every test after the first.
func startKeystoreFixture(ctx context.Context, t *testing.T) testcontainers.Container {
	t.Helper()

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			FromDockerfile: testcontainers.FromDockerfile{
				Context:    filepath.Join(".", "keystore"),
				Dockerfile: "Dockerfile",
				KeepImage:  true,
			},
			// Readiness is a store/lookup round trip executed in the container.
			// A log line and a bus-name check both report ready against a
			// service that answers no lookup.
			WaitingFor: wait.ForExec([]string{keystoreRoundTrip}).WithStartupTimeout(3 * time.Minute),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("start keystore fixture: %v", err)
	}

	t.Cleanup(func() {
		if err := container.Terminate(context.WithoutCancel(ctx)); err != nil {
			t.Errorf("terminate keystore fixture: %v", err)
		}
	})

	return container
}

// buildResidenceProbe compiles the package that owns the credential read path
// for the fixture's platform, under the tag that guards the matrix. The tests it
// carries cannot run on the host: one needs a live Secret Service beside it, the
// other needs Linux's own launcher resolution. GOWORK=off keeps the probe built
// from this module's own requirements.
func buildResidenceProbe(t *testing.T) string {
	t.Helper()

	out := filepath.Join(t.TempDir(), "residence.test")

	command := exec.CommandContext(t.Context(), "go", "test", "-c", "-tags=integration", "-o", out, "./internal/pi")
	command.Dir = ".."
	command.Env = append(os.Environ(), "GOWORK=off", "GOOS=linux", "GOARCH="+runtime.GOARCH, "CGO_ENABLED=0")

	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("build residence probe: %v: %s", err, output)
	}

	return out
}
