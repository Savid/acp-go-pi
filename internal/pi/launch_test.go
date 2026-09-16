package pi

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLaunchArgs(t *testing.T) {
	t.Parallel()

	require.Equal(t, []string{modeFlag, modeRPC}, Launch{}.Args())
	require.Equal(t,
		[]string{modeFlag, modeRPC, "-e", "/a.ts", "-e", "/b.ts", "--session", "/s.jsonl"},
		Launch{ExtensionPaths: []string{"/a.ts", "/b.ts"}, SessionPath: "/s.jsonl"}.Args(),
	)
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
