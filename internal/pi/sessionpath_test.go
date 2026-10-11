package pi

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSessionLayout(t *testing.T) {
	t.Parallel()

	lookup := func(values map[string]string) func(string) (string, bool) {
		return func(key string) (string, bool) {
			value, ok := values[key]

			return value, ok
		}
	}

	require.Equal(t, "/explicit", AgentDir("/explicit", lookup(map[string]string{EnvAgentDir: "/env"})))
	require.Equal(t, "/env", AgentDir("", lookup(map[string]string{EnvAgentDir: "/env"})))
	require.Equal(t, filepath.Join("/home/me", ".pi", "agent"), AgentDir("", lookup(map[string]string{EnvHome: "/home/me"})))

	require.Equal(t, "/agent/sessions/--home-me-proj--", SessionDir("/agent", "/home/me/proj"))

	stamp := time.Date(2026, 9, 11, 1, 2, 3, 456000000, time.UTC)
	require.Equal(t, "/agent/sessions/--w--/2026-09-11T01-02-03-456Z_abc.jsonl", SessionFile("/agent", "/w", "abc", stamp))
}

func TestParseHeader(t *testing.T) {
	t.Parallel()

	header, ok := ParseHeader([]byte(`{"type":"session","version":3,"id":"abc","timestamp":"t","cwd":"/w"}`))
	require.True(t, ok)
	require.Equal(t, "abc", header.ID)

	_, ok = ParseHeader([]byte(`{"type":"message"}`))
	require.False(t, ok)
}
