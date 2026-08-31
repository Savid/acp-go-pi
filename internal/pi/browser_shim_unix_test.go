//go:build !windows

package pi

import (
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func writeScript(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "fake-pi")
	require.NoError(t, os.WriteFile(path, []byte("#!/bin/sh\n"+body+"\n"), 0o700))

	return path
}

func startScriptProcess(t *testing.T, spec LaunchSpec) *Process {
	t.Helper()
	process, err := StartOrdinaryProcess(t.Context(), spec)
	require.NoError(t, err)

	return process
}

func TestNewBrowserShimWritesExecutableNoOps(t *testing.T) {
	t.Parallel()

	parent := t.TempDir()

	shim, err := NewBrowserShim(parent)
	require.NoError(t, err)

	dir := browserShimDirIn(t, parent)
	require.True(t, strings.HasPrefix(filepath.Base(dir), browserShimPrefix))

	info, err := os.Stat(dir)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o700), info.Mode().Perm())

	for _, name := range browserLauncherNames {
		entry, statErr := os.Stat(filepath.Join(dir, name))
		require.NoError(t, statErr)
		require.NotZero(t, entry.Mode().Perm()&0o100)
	}

	require.NoError(t, shim.Remove())
	require.NoDirExists(t, dir)
}

func TestLaunchSpecBrowserShimEnvironmentIsCanonicalAndSubtractive(t *testing.T) {
	shim, err := NewBrowserShim(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, shim.Remove()) })

	environment := (LaunchSpec{
		AgentDir:    "/agent",
		BrowserShim: shim,
		BaseEnvironment: map[string]string{
			"PATH": "/usr/bin", "HOME": "/native/home", "BASH_ENV": "/tmp/bash",
			"ENV": "/tmp/sh", "NODE_OPTIONS": "--require=/tmp/node.js", "LD_PRELOAD": "/tmp/ld.so",
			"DYLD_INSERT_LIBRARIES": "/tmp/dyld.dylib",
		},
	}).Environ()

	require.IsIncreasing(t, environment)
	joined := strings.Join(environment, "\n")
	for _, forbidden := range []string{"BASH_ENV=", "ENV=", "NODE_OPTIONS=", "LD_PRELOAD=", "DYLD_INSERT_LIBRARIES="} {
		require.NotContains(t, joined, forbidden)
	}
	require.Contains(t, environment, "BROWSER="+filepath.Join(shim.shim.dir, browserLauncherNames[0]))
	require.Contains(t, environment, "PATH="+shim.shim.dir+string(os.PathListSeparator)+"/usr/bin")
}

func TestNewBrowserShimFailures(t *testing.T) {
	wantErr := errors.New("injected browser shim failure")

	t.Run("directory creation fails", func(t *testing.T) {
		restoreBrowserShimSeams(t)

		browserShimMkdirTemp = func(string, string) (string, error) { return "", wantErr }

		_, err := NewBrowserShim(t.TempDir())
		require.ErrorIs(t, err, wantErr)
	})

	t.Run("launcher write fails and discards the directory", func(t *testing.T) {
		restoreBrowserShimSeams(t)

		parent := t.TempDir()
		browserShimWriteFile = func(string, []byte, os.FileMode) error { return wantErr }

		_, err := NewBrowserShim(parent)
		require.ErrorIs(t, err, wantErr)
		require.Empty(t, browserShimGlob(t, parent))
	})
}

func restoreBrowserShimSeams(t *testing.T) {
	t.Helper()

	mkdirTemp := browserShimMkdirTemp
	writeFile := browserShimWriteFile

	t.Cleanup(func() {
		browserShimMkdirTemp = mkdirTemp
		browserShimWriteFile = writeFile
	})
}

func browserShimGlob(t *testing.T, parent string) []string {
	t.Helper()

	matches, err := filepath.Glob(filepath.Join(parent, browserShimPrefix+"*"))
	require.NoError(t, err)

	return matches
}

func browserShimDirIn(t *testing.T, parent string) string {
	t.Helper()

	matches := browserShimGlob(t, parent)
	require.Len(t, matches, 1)

	return matches[0]
}

// browserLauncherRun is the shell body a fake harness runs: it resolves and
// execs every launcher name in turn, printing the path each name resolved to
// before handing it a URL. Every name is exercised because which one opens the
// operator's desktop is a property of the platform, not of the harness.
func browserLauncherRun() string {
	steps := make([]string, 0, len(browserLauncherNames)*2)

	for _, name := range browserLauncherNames {
		steps = append(steps, "command -v "+name, name+` "https://example.invalid/"`)
	}

	return strings.Join(steps, "\n")
}

// TestLoginNeverExecsABrowserLauncher is the proof the shim exists for: a pi
// process that execs a browser launcher by bare name never reaches a real one.
// The probe directory holds launchers that record every invocation, the control
// run proves those recorders work, and the launch that follows must leave the
// record empty.
func TestLoginNeverExecsABrowserLauncher(t *testing.T) {
	probe := t.TempDir()
	marker := filepath.Join(t.TempDir(), "launched")

	for _, name := range browserLauncherNames {
		body := "#!/bin/sh\necho \"$0 $*\" >> " + marker + "\nexit 0\n"
		require.NoError(t, os.WriteFile(filepath.Join(probe, name), []byte(body), 0o700))
	}

	control := exec.Command("/bin/sh", "-c", browserLauncherRun())
	control.Env = []string{"PATH=" + probe}
	require.NoError(t, control.Run())

	recorded, err := os.ReadFile(marker) // #nosec G304 -- the path is this test's own temp dir.
	require.NoError(t, err)

	for _, name := range browserLauncherNames {
		require.Contains(t, string(recorded), filepath.Join(probe, name))
	}

	require.NoError(t, os.Remove(marker))

	// The child inherits PATH from this process, so the probe is what an
	// unshimmed launch would find.
	t.Setenv("PATH", probe+string(os.PathListSeparator)+os.Getenv("PATH"))

	parent := t.TempDir()
	shim, err := NewBrowserShim(parent)
	require.NoError(t, err)

	script := writeScript(t, browserLauncherRun())
	process := startScriptProcess(t, LaunchSpec{
		ExecutablePath: script,
		AgentDir:       t.TempDir(),
		BrowserShim:    shim,
	})

	t.Cleanup(func() { _ = process.Close() })
	resolvedResult := make(chan struct {
		data []byte
		err  error
	}, 1)
	go func() {
		data, readErr := io.ReadAll(process.Stdout())
		resolvedResult <- struct {
			data []byte
			err  error
		}{data: data, err: readErr}
	}()

	select {
	case <-process.Exited():
	case <-time.After(10 * time.Second):
		t.Fatal("fake pi did not exit")
	}

	require.NoError(t, process.WaitErr())

	require.NoFileExists(t, marker)

	// The launchers the child did resolve were the shim's, not something further
	// along PATH that simply happened to be missing.
	resolved := <-resolvedResult
	require.NoError(t, resolved.err)

	dir := browserShimDirIn(t, parent)

	want := make([]string, 0, len(browserLauncherNames))
	for _, name := range browserLauncherNames {
		want = append(want, filepath.Join(dir, name))
	}

	require.Equal(t, want, strings.Fields(string(resolved.data)))

	require.NoError(t, shim.Remove())
}
