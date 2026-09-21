package piacp

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestScratchDir(t *testing.T) {
	t.Parallel()

	parent := filepath.Join(t.TempDir(), "nested", "scratch")
	agent := NewAgent(WithScratchDir(parent))

	dir, err := agent.scratchDir("ext", "abc")
	require.NoError(t, err)
	require.Equal(t, filepath.Join(parent, "acp-go-pi-ext-abc"), dir)
	require.DirExists(t, parent)

	agent = NewAgent()
	dir, err = agent.scratchDir("ext", "abc")
	require.NoError(t, err)
	require.Equal(t, "acp-go-pi-ext-abc", filepath.Base(dir))
}
