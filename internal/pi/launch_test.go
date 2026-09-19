package pi

import (
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
