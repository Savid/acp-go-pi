package piacp

import (
	"maps"
	"testing"

	"github.com/savid/acp-go-core/wire"

	"github.com/stretchr/testify/require"
)

func TestPiOptionsMetaRoundTrip(t *testing.T) {
	t.Parallel()

	options := NewPiOptions(
		WithPiModel("fake/vision"),
		WithPiEnv(map[string]string{"A": "1"}),
		WithPiExtraPathDirs("/bin"),
		WithPiThinkingLevel("high"),
		WithPiPermission("allow"),
		WithPiAutoRetry(true),
	)

	meta := options.Meta()
	parsed, err := parseSessionMeta(meta)
	require.Nil(t, err)
	require.Equal(t, options, parsed.options)
	require.True(t, parsed.presentEnv)
	require.True(t, parsed.presentExtraPathDirs)

	options.Env["A"] = "changed"
	require.Equal(t, "1", parsed.options.Env["A"])

	require.NoError(t, ValidatePiSessionMeta(meta))
	require.NoError(t, ValidatePiSessionMeta(nil))
	require.Error(t, ValidatePiSessionMeta(map[string]any{"pi": "x"}))
}

func TestParseSessionMetaRawEvents(t *testing.T) {
	t.Parallel()

	parsed, err := parseSessionMeta(map[string]any{"pi": map[string]any{"rawEvent": map[string]any{"enabled": true}}})
	require.Nil(t, err)
	require.True(t, parsed.rawEvents)

	_, err = parseSessionMeta(map[string]any{"pi": map[string]any{"rawEvent": map[string]any{"enabled": "yes"}}})
	require.NotNil(t, err)

	_, err = parseSessionMeta(map[string]any{"pi": map[string]any{"rawEvent": true}})
	require.NotNil(t, err)
}

func TestParseSessionMetaShapes(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		meta  map[string]any
		field string
	}{
		{"options not object", map[string]any{"pi": map[string]any{"options": 1}}, "_meta.pi.options"},
		{"model type", map[string]any{"pi": map[string]any{"options": map[string]any{"model": 1}}}, "_meta.pi.options.model"},
		{"env type", map[string]any{"pi": map[string]any{"options": map[string]any{"env": 1}}}, "_meta.pi.options.env"},
		{"env value type", map[string]any{"pi": map[string]any{"options": map[string]any{"env": map[string]any{"A": 1}}}}, "_meta.pi.options.env.A"},
		{"dirs type", map[string]any{"pi": map[string]any{"options": map[string]any{"extraPathDirs": "x"}}}, "_meta.pi.options.extraPathDirs"},
		{"dir element type", map[string]any{"pi": map[string]any{"options": map[string]any{"extraPathDirs": []any{1}}}}, "_meta.pi.options.extraPathDirs[0]"},
		{"empty thinking", map[string]any{"pi": map[string]any{"options": map[string]any{"thinkingLevel": ""}}}, "_meta.pi.options.thinkingLevel"},
		{"autoRetry type", map[string]any{"pi": map[string]any{"options": map[string]any{"autoRetry": "yes"}}}, "_meta.pi.options.autoRetry"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := parseSessionMeta(tc.meta)
			require.NotNil(t, err)
			require.Equal(t, tc.field, requestErrorData(t, err)["field"])
		})
	}

	parsed, err := parseSessionMeta(map[string]any{"pi": map[string]any{"options": map[string]any{"env": map[string]string{"A": "1"}, "extraPathDirs": []string{"/x"}}}})
	require.Nil(t, err)
	require.Equal(t, map[string]string{"A": "1"}, parsed.options.Env)
	require.Equal(t, []string{"/x"}, parsed.options.ExtraPathDirs)
}

func TestCloneAnyAndMerge(t *testing.T) {
	t.Parallel()

	base := map[string]any{"a": map[string]any{"x": 1}, "list": []any{1}, "strs": []string{"a"}}
	merged := wire.MergeMap(base, map[string]any{"a": map[string]any{"y": 2}, "b": 3})
	require.Equal(t, map[string]any{"a": map[string]any{"x": 1, "y": 2}, "b": 3, "list": []any{1}, "strs": []string{"a"}}, merged)
	require.Nil(t, wire.CloneMap(nil))
	require.Nil(t, maps.Clone(map[string]string(nil)))
}
