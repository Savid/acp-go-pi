package pi

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseModelRef(t *testing.T) {
	t.Parallel()

	ref, err := ParseModelRef("openai/gpt/nested")
	require.NoError(t, err)
	require.Equal(t, ModelRef{Provider: "openai", ID: "gpt/nested"}, ref)

	for _, bad := range []string{"", "openai", "/x", "x/"} {
		_, err := ParseModelRef(bad)
		require.Error(t, err, bad)
	}

	levels := ThinkingLevels()
	levels[0] = "changed"
	require.Equal(t, ThinkingLevelOff, ThinkingLevels()[0])
	require.Equal(t, "a/b", Model{Provider: "a", ID: "b"}.Ref())
}
