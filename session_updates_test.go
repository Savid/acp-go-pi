package piacp

import (
	"strings"
	"testing"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"

	"github.com/savid/acp-go-pi/internal/pi"
)

func TestAvailableCommandsMapping(t *testing.T) {
	commands := availableCommandsFromNative([]pi.SlashCommand{
		{Name: "good", Description: "ok"},
		{Name: ""}, {Name: "bad/name"}, {Name: "bad name"}, {Name: "bad\x00"}, {Name: "bad\u200e"},
		{Name: string([]byte{0xff})},
	})
	require.Len(t, commands, 1)
	require.True(t, availableCommandsEqual(commands, cloneAvailableCommands(commands)))
	require.False(t, availableCommandsEqual(commands, nil))
	other := cloneAvailableCommands(commands)
	other[0].Description = "different"
	require.False(t, availableCommandsEqual(commands, other))
	require.Nil(t, cloneAvailableCommands(nil))

	unstructured := acp.UnstructuredCommandInput{Hint: "hint"}
	input := acp.AvailableCommandInput{Unstructured: &unstructured}
	withInput := []acp.AvailableCommand{{Name: "input", Input: &input}}
	clone := cloneAvailableCommands(withInput)
	clone[0].Input.Unstructured.Hint = "changed"
	require.Equal(t, "hint", withInput[0].Input.Unstructured.Hint)
}

func TestLiveSessionTitleNormalization(t *testing.T) {
	require.Equal(t, "short title", normalizeLiveSessionTitle("  short   title  "))
	require.Equal(t, "", normalizeLiveSessionTitle(" \n\t "))
	long := strings.Repeat("x", 300)
	require.LessOrEqual(t, len(normalizeLiveSessionTitle(long)), liveSessionTitleMaxRunes)
	prompt := []acp.ContentBlock{acp.TextBlock(" first "), acp.TextBlock("second")}
	require.Equal(t, "first", liveSessionTitleFromPrompt(prompt))
	require.Empty(t, liveSessionTitleFromPrompt(nil))
}
