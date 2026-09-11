package pi

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCheckMinimumVersion(t *testing.T) {
	t.Parallel()

	require.NoError(t, CheckMinimumVersion("0.84.4", MinimumVersion))
	require.NoError(t, CheckMinimumVersion("v0.80.6-beta", "0.80.6"))
	require.NoError(t, CheckMinimumVersion("1.0", "0.99.99"))
	require.Error(t, CheckMinimumVersion("0.80.5", "0.80.6"))
	require.Error(t, CheckMinimumVersion("abc", "0.80.6"))
	require.Error(t, CheckMinimumVersion("0.80.6", "x"))
	require.Error(t, CheckMinimumVersion("", "0.80.6"))
}

func TestProbeVersion(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	script := filepath.Join(dir, "pi")
	require.NoError(t, os.WriteFile(script, []byte("#!/bin/sh\necho 1.2.3\n"), 0o700))

	version, err := ProbeVersion(context.Background(), script, []string{"PATH=/usr/bin:/bin"})
	require.NoError(t, err)
	require.Equal(t, "1.2.3", version)

	empty := filepath.Join(dir, "empty")
	require.NoError(t, os.WriteFile(empty, []byte("#!/bin/sh\n"), 0o700))
	_, err = ProbeVersion(context.Background(), empty, nil)
	require.ErrorContains(t, err, "empty output")

	_, err = ProbeVersion(context.Background(), filepath.Join(dir, "missing"), nil)
	require.Error(t, err)
}
