package piacp

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPathValidationHelpers(t *testing.T) {
	require.Error(t, validateRequiredAbsolutePath("cwd", ""))
	require.Error(t, validateRequiredAbsolutePath("cwd", "relative"))
	require.NoError(t, validateRequiredAbsolutePath("cwd", "/absolute"))
	require.NoError(t, validateOptionalAbsolutePath("cwd", nil))
	empty := ""
	require.NoError(t, validateOptionalAbsolutePath("cwd", &empty))
	relative := "relative"
	require.Error(t, validateOptionalAbsolutePath("cwd", &relative))
	absolute := "/absolute"
	require.NoError(t, validateOptionalAbsolutePath("cwd", &absolute))
	require.Error(t, validateAbsolutePaths("paths", []string{""}))
	require.Error(t, validateAbsolutePaths("paths", []string{"relative"}))
	require.NoError(t, validateAbsolutePaths("paths", []string{"/one", "/two"}))
	require.Error(t, validateSessionStartPaths("", nil))
	require.Error(t, validateSessionStartPaths("/cwd", []string{"relative"}))
	require.NoError(t, validateSessionStartPaths("/cwd", []string{"/also"}))
}
