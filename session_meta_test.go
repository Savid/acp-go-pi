package piacp

import (
	"os"
	"strings"
	"testing"

	"github.com/savid/acp-go-pi/internal/pi"
	"github.com/stretchr/testify/require"
)

func TestPiOptionsMetaAndStrictParsing(t *testing.T) {
	options := NewPiOptions(
		WithPiModel("fake/model"),
		WithPiEnv(map[string]string{"TOKEN": "value"}),
		WithPiExtraPathDirs("/opt/shim/bin"),
		WithPiThinkingLevel("high"),
		WithPiPermission("ask"),
		WithPiAutoRetry(true),
	)
	meta := options.Meta()
	parsed, err := piOptionsFromMeta(meta)
	require.NoError(t, err)
	require.Equal(t, options, parsed)

	options.Env["TOKEN"] = "changed"
	options.ExtraPathDirs[0] = "/opt/changed/bin"
	require.Equal(t, "value", parsed.Env["TOKEN"])
	require.Equal(t, []string{"/opt/shim/bin"}, parsed.ExtraPathDirs)

	valid := []map[string]any{
		nil,
		{"foreign": map[string]any{"anything": true}},
		{piMetaKey: map[string]any{}},
		{piMetaKey: map[string]any{metaRawEventKey: map[string]any{}}},
		{piMetaKey: map[string]any{metaRawEventKey: map[string]any{metaRawEventEnabledKey: true}}},
		{piMetaKey: map[string]any{metaOptionsKey: map[string]any{metaEnvKey: map[string]string{"A": "b"}}}},
		{piMetaKey: map[string]any{metaOptionsKey: map[string]any{metaEnvKey: map[string]any{"A": "b"}}}},
		{piMetaKey: map[string]any{metaOptionsKey: map[string]any{metaAutoRetryKey: false}}},
		{piMetaKey: map[string]any{metaOptionsKey: map[string]any{metaExtraPathDirsKey: []any{"/opt/bin"}}}},
		{piMetaKey: map[string]any{metaOptionsKey: map[string]any{metaExtraPathDirsKey: []string{"/opt/bin", "/srv/bin"}}}},
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
		{piMetaKey: map[string]any{metaOptionsKey: map[string]any{metaOutputSchemaKey: true}}},
		{piMetaKey: map[string]any{metaOptionsKey: map[string]any{metaOutputSchemaKey: map[string]any{}}}},
		{piMetaKey: map[string]any{metaOptionsKey: map[string]any{metaOutputSchemaKey: map[string]any{"type": "object"}}}},
		{piMetaKey: map[string]any{metaOptionsKey: map[string]any{metaThinkingLevelKey: true}}},
		{piMetaKey: map[string]any{metaOptionsKey: map[string]any{metaThinkingLevelKey: ""}}},
		{piMetaKey: map[string]any{metaOptionsKey: map[string]any{metaPermissionKey: true}}},
		{piMetaKey: map[string]any{metaOptionsKey: map[string]any{metaPermissionKey: "bad"}}},
		{piMetaKey: map[string]any{metaOptionsKey: map[string]any{metaAutoRetryKey: "yes"}}},
		{piMetaKey: map[string]any{metaOptionsKey: map[string]any{metaExtraPathDirsKey: "/opt/bin"}}},
		{piMetaKey: map[string]any{metaOptionsKey: map[string]any{metaExtraPathDirsKey: []any{true}}}},
		{piMetaKey: map[string]any{metaOptionsKey: map[string]any{metaExtraPathDirsKey: []any{""}}}},
		{piMetaKey: map[string]any{metaOptionsKey: map[string]any{metaExtraPathDirsKey: []any{"relative/bin"}}}},
		{piMetaKey: map[string]any{metaOptionsKey: map[string]any{
			metaExtraPathDirsKey: []any{"/opt/bin" + string(os.PathListSeparator) + "/srv/bin"},
		}}},
	}
	for _, value := range invalid {
		_, err := piOptionsFromMeta(value)
		require.Error(t, err)
	}
}

// TestPiOptionsThinkingLevelPassesThrough pins that establishment metadata
// judges the member's shape and nothing else. Only the empty string names no
// level; whitespace names one pi may not know, which is pi's to answer for.
func TestPiOptionsThinkingLevelPassesThrough(t *testing.T) {
	for _, level := range []string{"registry-unknown", " high ", "\t"} {
		options, err := piOptionsFromMeta(map[string]any{
			piMetaKey: map[string]any{
				metaOptionsKey: map[string]any{metaThinkingLevelKey: level},
			},
		})
		require.NoError(t, err)
		require.Equal(t, level, options.ThinkingLevel)
	}
}

func TestEnvironmentValidation(t *testing.T) {
	valid := []string{"A", "_A", "a1", "A_B_2"}
	for _, name := range valid {
		require.True(t, validEnvName(name))
		require.False(t, blockedAgentEnvKey(name))
		require.False(t, blockedSessionEnvKey(name))
	}

	for _, name := range []string{"", "1A", "A-B", "A B", "é"} {
		require.False(t, validEnvName(name))
	}

	for _, name := range []string{"NODE_OPTIONS", "BASH_ENV", "ENV", "LD_PRELOAD", "dyld_insert_libraries", "ACP_GO_PI_INTERNAL_DARWIN_LAUNCH", "acp_go_pi_internal_turn_supervisor", pi.EnvExtraPathDirs, strings.ToLower(pi.EnvExtraPathDirs)} {
		require.True(t, blockedAgentEnvKey(name))
		require.True(t, blockedSessionEnvKey(name))
	}

	// The dedicated ordered option is the only session PATH authority, while
	// the agent-scoped environment is where the static base PATH is set.
	for _, name := range []string{"PATH", "Path"} {
		require.False(t, blockedAgentEnvKey(name))
		require.True(t, blockedSessionEnvKey(name))
	}

	require.NoError(t, validateEnvironment(map[string]string{"PATH": "/base/bin"}, optionFieldEnv, blockedAgentEnvKey))
	requireUnsupportedField(t, validateEnvironment(map[string]string{"PATH": "/raw/bin"}, metaOptionPath(metaEnvKey), blockedSessionEnvKey), metaOptionPath(metaEnvKey)+".PATH")
	requireUnsupportedField(t, validateEnvironment(map[string]string{pi.EnvExtraPathDirs: "/attacker/bin"}, metaOptionPath(metaEnvKey), blockedSessionEnvKey), metaOptionPath(metaEnvKey)+"."+pi.EnvExtraPathDirs)
	requireUnsupportedField(t, validateEnvironment(map[string]string{"1A": "x"}, optionFieldEnv, blockedAgentEnvKey), optionFieldEnv+".1A")

	originalPlatform := agentRuntimePlatform
	agentRuntimePlatform = windowsPlatform
	t.Cleanup(func() { agentRuntimePlatform = originalPlatform })
	requireUnsupportedField(
		t,
		validateEnvironment(map[string]string{"Provider_Key": "session", "PROVIDER_KEY": "agent"}, metaOptionPath(metaEnvKey), blockedSessionEnvKey),
		metaOptionPath(metaEnvKey)+".Provider_Key",
	)
}

func TestExtraPathDirsValidation(t *testing.T) {
	t.Parallel()

	require.NoError(t, validateExtraPathDirs(nil, metaExtraPathDirsKey))
	require.NoError(t, validateExtraPathDirs([]string{"/opt/bin", "/srv/bin"}, metaExtraPathDirsKey))

	for _, dirs := range [][]string{
		{""},
		{"relative/bin"},
		{"./bin"},
		{"/opt/bin", "relative/bin"},
		{"/opt/bin" + string(os.PathListSeparator) + "/srv/bin"},
	} {
		require.Error(t, validateExtraPathDirs(dirs, metaExtraPathDirsKey))
	}

	requireUnsupportedField(
		t,
		validateExtraPathDirs([]string{"/opt/bin", "relative/bin"}, metaExtraPathDirsKey),
		metaExtraPathDirsKey+"[1]",
	)
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
			name:    "present empty output schema",
			options: PiOptions{OutputSchema: map[string]any{}},
			want: map[string]any{"pi": map[string]any{"options": map[string]any{
				"outputSchema": map[string]any{},
			}}},
		},
		{
			name: "all supported fields",
			options: PiOptions{
				Model:         "openai/gpt-4o",
				Env:           map[string]string{"K": "V"},
				ExtraPathDirs: []string{"/opt/bin"},
				OutputSchema:  map[string]any{"type": "object"},
				ThinkingLevel: "high",
				Permission:    "allow",
				AutoRetry:     true,
			},
			want: map[string]any{"pi": map[string]any{"options": map[string]any{
				"model":         "openai/gpt-4o",
				"env":           map[string]string{"K": "V"},
				"extraPathDirs": []string{"/opt/bin"},
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
		Env:           map[string]string{"K": "V"},
		ExtraPathDirs: []string{"/opt/bin"},
		OutputSchema:  map[string]any{"type": "object"},
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

	dirsClone, ok := values["extraPathDirs"].([]string)
	require.True(t, ok)

	dirsClone[0] = "/opt/mutated"
	require.Equal(t, "/opt/bin", options.ExtraPathDirs[0])
}
