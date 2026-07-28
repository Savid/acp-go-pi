//go:build !windows

package pi

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

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

	control := exec.Command("/bin/sh", "-c", `open "https://example.invalid/"`)
	control.Env = []string{"PATH=" + probe}
	require.NoError(t, control.Run())
	require.FileExists(t, marker)
	require.NoError(t, os.Remove(marker))

	// The child inherits PATH from this process, so the probe is what an
	// unshimmed launch would find.
	t.Setenv("PATH", probe+string(os.PathListSeparator)+os.Getenv("PATH"))

	parent := t.TempDir()
	shim, err := NewBrowserShim(parent)
	require.NoError(t, err)

	script := writeScript(t, `command -v open; open "https://example.invalid/"`)
	process := startScriptProcess(t, LaunchSpec{
		ExecutablePath: script,
		AgentDir:       t.TempDir(),
		BrowserShim:    shim,
	})

	t.Cleanup(func() { _ = process.Close() })

	select {
	case <-process.Exited():
	case <-time.After(10 * time.Second):
		t.Fatal("fake pi did not exit")
	}

	require.NoError(t, process.WaitErr())

	require.NoFileExists(t, marker)

	// The launcher the child did resolve was the shim's, not something further
	// along PATH that simply happened to be missing.
	resolved := make([]byte, 512)
	read, _ := process.Stdout().Read(resolved)
	require.Equal(t, filepath.Join(browserShimDirIn(t, parent), browserLauncherNames[0]), strings.TrimSpace(string(resolved[:read])))

	require.NoError(t, shim.Remove())
}
