//go:build integration

package integration

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// TestKeystoreLinuxLoginNeverExecsABrowserLauncher settles the launcher shim's
// non-Darwin half. Which program opens a URL is a property of the platform:
// this repo is developed on machines where `open` is the answer, so `xdg-open`
// and its Debian alternatives are the names no local run ever resolves. The
// proof therefore runs the real launch path against a real Linux PATH, inside
// the container tier, where those names are what a login would reach.
func TestKeystoreLinuxLoginNeverExecsABrowserLauncher(t *testing.T) {
	requireKeystoreRuntime(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	container := startKeystoreFixture(ctx, t)

	if err := container.CopyFileToContainer(ctx, buildLinuxPiProbe(t), keystoreProbePath, 0o755); err != nil {
		t.Fatalf("copy launcher probe: %v", err)
	}

	code, output, err := container.Exec(
		ctx,
		[]string{keystoreProbePath, "-test.v", "-test.run", "^TestLoginNeverExecsABrowserLauncher$"},
		tcexec.Multiplexed(),
	)
	if err != nil {
		t.Fatalf("run launcher probe: %v", err)
	}

	logs, readErr := io.ReadAll(output)
	if readErr != nil {
		t.Fatalf("read launcher probe output: %v", readErr)
	}

	t.Log(string(logs))

	if code != 0 {
		t.Fatalf("launcher probe exited %d", code)
	}

	if !strings.Contains(string(logs), "PASS") {
		t.Fatalf("the launcher probe did not report PASS: %s", logs)
	}

	// A selector that matched nothing also exits 0 and prints PASS.
	if !strings.Contains(string(logs), "--- PASS: TestLoginNeverExecsABrowserLauncher") {
		t.Fatalf("the launcher proof did not run inside the fixture: %s", logs)
	}
}
