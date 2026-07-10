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

func TestRawMessageConfigAndMarkers(t *testing.T) {
	require.False(t, rawMessageConfigFromMeta(nil).Enabled())
	require.False(t, rawMessageConfigFromMeta(map[string]any{piMetaKey: "bad"}).Enabled())
	require.False(t, rawMessageConfigFromMeta(map[string]any{piMetaKey: map[string]any{metaRawEventKey: true}}).Enabled())
	require.True(t, rawMessageConfigFromMeta(map[string]any{piMetaKey: map[string]any{
		metaRawEventKey: map[string]any{metaRawEventEnabledKey: true},
	}}).Enabled())

	marker, marked := rawEventMarker(map[string]any{"value": "small"})
	require.False(t, marked)
	require.Nil(t, marker)

	marker, marked = rawEventMarker(map[string]any{"value": make(chan int)})
	require.True(t, marked)
	require.Equal(t, rawEventReasonUnserializable, marker[rawEventFieldReason])
	require.NotContains(t, marker, rawEventFieldSizeBytes)

	marker, marked = rawEventMarker(map[string]any{"value": string(make([]byte, rawEventMaxBytes))})
	require.True(t, marked)
	require.Equal(t, rawEventReasonOversize, marker[rawEventFieldReason])
	require.Greater(t, marker[rawEventFieldSizeBytes], rawEventMaxBytes)
}
