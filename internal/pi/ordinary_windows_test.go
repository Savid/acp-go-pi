//go:build windows

package pi

import (
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

const windowsChildArgument = "acp-go-pi-windows-child"

func TestOrdinaryWindowsExecutableResolutionExecutesSuppliedPATHEXTChild(t *testing.T) {
	if slices.Contains(os.Args, windowsChildArgument) {
		marker := os.Getenv("ACP_GO_PI_WINDOWS_MARKER")
		require.NotEmpty(t, marker)
		require.NoError(t, os.WriteFile(marker, []byte("executed"), 0o600))

		return
	}

	executable, err := os.Executable()
	require.NoError(t, err)
	directory := t.TempDir()
	child := filepath.Join(directory, "pi.PROBE")
	data, err := os.ReadFile(executable)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(child, data, 0o700))

	resolved, err := lookPathInOrdinaryEnvironment("pi", []string{
		"PATH=" + directory,
		"PATHEXT=.PROBE",
	})
	require.NoError(t, err)
	require.Equal(t, child, resolved)

	marker := filepath.Join(t.TempDir(), "executed")
	command := exec.Command(resolved, "-test.run=^TestOrdinaryWindowsExecutableResolutionExecutesSuppliedPATHEXTChild$", "--", windowsChildArgument)
	command.Env = append(os.Environ(), "ACP_GO_PI_WINDOWS_MARKER="+marker)
	require.NoError(t, command.Run())
	require.FileExists(t, marker)
}

func TestWindowsEnvironmentCompositionIsCaseInsensitiveLastWins(t *testing.T) {
	composed := ComposeEnvironment(
		map[string]string{"Path": "first", "Provider_Key": "one"},
		map[string]string{"PATH": "second", "provider_key": "two"},
	)

	require.Equal(t, map[string]string{"PATH": "second", "PROVIDER_KEY": "two"}, composed)
}
