package piacp

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPiOptionsMetaAndStrictParsing(t *testing.T) {
	options := NewPiOptions(
		WithPiModel("fake/model"),
		WithPiEnv(map[string]string{"TOKEN": "value"}),
		WithPiThinkingLevel("high"),
		WithPiPermission("ask"),
		WithPiAutoRetry(true),
	)
	meta := options.Meta()
	parsed, err := piOptionsFromMeta(meta)
	require.NoError(t, err)
	require.Equal(t, options, parsed)

	options.Env["TOKEN"] = "changed"
	require.Equal(t, "value", parsed.Env["TOKEN"])

	valid := []map[string]any{
		nil,
		{"foreign": map[string]any{"anything": true}},
		{piMetaKey: map[string]any{}},
		{piMetaKey: map[string]any{metaRawEventKey: map[string]any{}}},
		{piMetaKey: map[string]any{metaRawEventKey: map[string]any{metaRawEventEnabledKey: true}}},
		{piMetaKey: map[string]any{metaOptionsKey: map[string]any{metaEnvKey: map[string]string{"A": "b"}}}},
		{piMetaKey: map[string]any{metaOptionsKey: map[string]any{metaEnvKey: map[string]any{"A": "b"}}}},
		{piMetaKey: map[string]any{metaOptionsKey: map[string]any{metaAutoRetryKey: false}}},
	}
	for _, value := range valid {
		_, err := piOptionsFromMeta(value)
		require.NoError(t, err)
	}

	invalid := []map[string]any{
		{piMetaKey: nil},
		{piMetaKey: "bad"},
		{piMetaKey: map[string]any{"deleted": true}},
		{piMetaKey: map[string]any{metaRawEventKey: true}},
		{piMetaKey: map[string]any{metaRawEventKey: map[string]any{"deleted": true}}},
		{piMetaKey: map[string]any{metaRawEventKey: map[string]any{metaRawEventEnabledKey: "yes"}}},
		{piMetaKey: map[string]any{metaOptionsKey: true}},
		{piMetaKey: map[string]any{metaOptionsKey: map[string]any{"deleted": true}}},
		{piMetaKey: map[string]any{metaOptionsKey: map[string]any{metaModelKey: true}}},
		{piMetaKey: map[string]any{metaOptionsKey: map[string]any{metaModelKey: "invalid"}}},
		{piMetaKey: map[string]any{metaOptionsKey: map[string]any{metaEnvKey: true}}},
		{piMetaKey: map[string]any{metaOptionsKey: map[string]any{metaEnvKey: map[string]any{"A": true}}}},
		{piMetaKey: map[string]any{metaOptionsKey: map[string]any{metaEnvKey: map[string]any{"1BAD": "x"}}}},
		{piMetaKey: map[string]any{metaOptionsKey: map[string]any{metaEnvKey: map[string]any{"PATH": "x"}}}},
		{piMetaKey: map[string]any{metaOptionsKey: map[string]any{metaOutputSchemaKey: true}}},
		{piMetaKey: map[string]any{metaOptionsKey: map[string]any{metaOutputSchemaKey: map[string]any{"type": "object"}}}},
		{piMetaKey: map[string]any{metaOptionsKey: map[string]any{metaThinkingLevelKey: true}}},
		{piMetaKey: map[string]any{metaOptionsKey: map[string]any{metaThinkingLevelKey: "bad"}}},
		{piMetaKey: map[string]any{metaOptionsKey: map[string]any{metaPermissionKey: true}}},
		{piMetaKey: map[string]any{metaOptionsKey: map[string]any{metaPermissionKey: "bad"}}},
		{piMetaKey: map[string]any{metaOptionsKey: map[string]any{metaAutoRetryKey: "yes"}}},
	}
	for _, value := range invalid {
		_, err := piOptionsFromMeta(value)
		require.Error(t, err)
	}
}

func TestEnvironmentValidation(t *testing.T) {
	valid := []string{"A", "_A", "a1", "A_B_2"}
	for _, name := range valid {
		require.True(t, validEnvName(name))
		require.False(t, blockedEnvKey(name))
	}

	for _, name := range []string{"", "1A", "A-B", "A B", "é"} {
		require.False(t, validEnvName(name))
	}
	for _, name := range []string{"PATH", "path", "NODE_OPTIONS", "BASH_ENV", "ENV", "LD_PRELOAD", "dyld_insert_libraries"} {
		require.True(t, blockedEnvKey(name))
	}
}

func TestPiOptionsMeta(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		options PiOptions
		want    map[string]any
	}{
		{
			name:    "empty options",
			options: PiOptions{},
			want:    map[string]any{"pi": map[string]any{"options": map[string]any{}}},
		},
		{
			name: "all supported fields",
			options: PiOptions{
				Model:         "openai/gpt-4o",
				Env:           map[string]string{"K": "V"},
				OutputSchema:  map[string]any{"type": "object"},
				ThinkingLevel: "high",
				Permission:    "allow",
				AutoRetry:     true,
			},
			want: map[string]any{"pi": map[string]any{"options": map[string]any{
				"model":         "openai/gpt-4o",
				"env":           map[string]string{"K": "V"},
				"outputSchema":  map[string]any{"type": "object"},
				"thinkingLevel": "high",
				"permission":    "allow",
				"autoRetry":     true,
			}}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, test.want, test.options.Meta())
		})
	}
}

func TestPiOptionsMetaClonesMaps(t *testing.T) {
	t.Parallel()

	options := PiOptions{
		Env:          map[string]string{"K": "V"},
		OutputSchema: map[string]any{"type": "object"},
	}

	meta := options.Meta()

	piMeta, ok := meta["pi"].(map[string]any)
	require.True(t, ok)

	values, ok := piMeta["options"].(map[string]any)
	require.True(t, ok)

	envClone, ok := values["env"].(map[string]string)
	require.True(t, ok)

	envClone["K"] = "mutated"
	require.Equal(t, "V", options.Env["K"])
}
