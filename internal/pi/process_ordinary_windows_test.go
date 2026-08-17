//go:build windows

package pi

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

// windowsExecutableChildMarker puts a copied test binary into
// environment-reporting mode. It travels in argv after the test flag
// terminator rather than in the environment: the environment is the thing
// under test here, so a carrier living in it would be an input the child
// reports back, and a new operator-visible variable is exactly what the
// family's enumerated integration namespace does not admit.
const windowsExecutableChildMarker = "acp-go-pi-report-windows-environment"

type windowsExecutableChildEnvironment struct {
	Path     string `json:"path"`
	PathExt  string `json:"pathExt"`
	Provider string `json:"provider"`
}

func TestOrdinaryWindowsExecutableResolutionExecutesSuppliedPATHEXTChild(t *testing.T) {
	if slices.Contains(os.Args, windowsExecutableChildMarker) {
		_ = json.NewEncoder(os.Stdout).Encode(windowsExecutableChildEnvironment{
			Path:     os.Getenv("PATH"),
			PathExt:  os.Getenv("PATHEXT"),
			Provider: os.Getenv("PROVIDER_KEY"),
		})
		os.Exit(0)
	}

	sourcePath, err := os.Executable()
	require.NoError(t, err)

	source, err := os.Open(sourcePath)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, source.Close()) })

	dir := t.TempDir()
	targetPath := filepath.Join(dir, "pi.exe")
	target, err := os.Create(targetPath)
	require.NoError(t, err)
	_, err = io.Copy(target, source)
	require.NoError(t, err)
	require.NoError(t, target.Close())

	hostileDir := t.TempDir()
	t.Setenv("PATH", hostileDir)
	t.Setenv("PATHEXT", ".NOPE")
	supplied := []string{
		"Path=" + hostileDir,
		"PATH=" + dir,
		"PathExt=.NOPE",
		"PATHEXT=.EXE;.CMD",
		"Provider_Key=agent",
		"PROVIDER_KEY=session",
	}
	resolved, err := lookPathInOrdinaryEnvironment("pi", supplied)
	require.NoError(t, err)
	require.Equal(t, targetPath, resolved)

	command := exec.Command(resolved,
		"-test.run=^TestOrdinaryWindowsExecutableResolutionExecutesSuppliedPATHEXTChild$",
		"--", windowsExecutableChildMarker,
	)
	command.Env = slices.Clone(supplied)
	var output bytes.Buffer
	command.Stdout = &output
	require.NoError(t, command.Run())

	var child windowsExecutableChildEnvironment
	require.NoError(t, json.Unmarshal(output.Bytes(), &child))
	require.Equal(t, windowsExecutableChildEnvironment{
		Path:     dir,
		PathExt:  ".EXE;.CMD",
		Provider: "session",
	}, child)
}

func TestWindowsEnvironmentCompositionIsCaseInsensitiveLastWins(t *testing.T) {
	environment := ComposeEnvironment(
		map[string]string{"Path": "base-path", "Provider_Key": "base"},
		map[string]string{"PATH": "agent-path", "PROVIDER_KEY": "agent"},
		map[string]string{"path": "session-path", "provider_key": "session"},
		map[string]string{"Path": "managed-path", "Provider_Key": "managed"},
	)
	require.Equal(t, map[string]string{
		"PATH":         "managed-path",
		"PROVIDER_KEY": "managed",
	}, environment)
}
