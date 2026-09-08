package piacp

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/savid/acp-go-pi/internal/pi"
)

func simulateSessionEnvPlatform(t *testing.T, platform string) {
	t.Helper()

	previous := pi.Platform
	t.Cleanup(func() { pi.Platform = previous })

	pi.Platform = platform
}

func envMeta(env map[string]any) map[string]any {
	return map[string]any{piMetaKey: map[string]any{metaOptionsKey: map[string]any{metaEnvKey: env}}}
}

func TestSessionEnvAcceptsEveryStructurallyValidName(t *testing.T) {
	simulateSessionEnvPlatform(t, "linux")

	env := map[string]any{
		"https_proxy":         "",
		"no_proxy":            "",
		"WAGIE_API_URL":       "http://127.0.0.1:1",
		"BASH_FUNC_x%%":       "() { :; }",
		"1A":                  "leading digit",
		"path":                "/not/the/search/path",
		"env":                 "/not/the/shell/init",
		"ld_preload":          "/not/the/loader",
		"pi_coding_agent_dir": "/not/the/state/root",
		"PI_OFFLINE":          "0",
	}

	options, err := piOptionsFromMeta(envMeta(env))
	require.NoError(t, err)
	require.Len(t, options.Env, len(env))
	require.Equal(t, "", options.Env["https_proxy"])
	require.NoError(t, ValidatePiSessionMeta(envMeta(env)))
	require.NoError(t, validateEnvironment(map[string]string{"PATH": "/base/bin", "path": "/own"}, optionFieldEnv, blockedAgentEnvKey))
	require.NoError(t, validateEnvironment(map[string]string{envKeyAgentDir: "/ignored", "PI_OFFLINE": "0"}, optionFieldEnv, blockedAgentEnvKey))
}

func TestSessionEnvRejectsStateRootBeforeNativeLaunch(t *testing.T) {
	for _, test := range []struct {
		platform string
		key      string
	}{
		{"linux", "PI_CODING_AGENT_DIR"},
		{"windows", "PI_CODING_AGENT_DIR"},
		{"windows", "pi_coding_agent_dir"},
		{"windows", "Pi_Coding_Agent_Dir"},
	} {
		t.Run(test.platform+"/"+test.key, func(t *testing.T) {
			simulateSessionEnvPlatform(t, test.platform)
			client := newStubPiClient()
			agent := newStubClientAgent(t, client)
			starts := 0
			agent.startPiProcess = func(context.Context, pi.LaunchSpec) (piProcess, piClient, error) {
				starts++

				return newStubProcess(false), client, nil
			}

			request := NewSessionRequest(testCwd, WithSessionPiOptions(NewPiOptions(
				WithPiEnv(map[string]string{test.key: absTestPath("caller", "state")}),
			)))
			field := metaOptionPath(metaEnvKey) + "." + test.key
			requireUnsupportedField(t, ValidatePiSessionMeta(request.Meta), field)
			_, err := agent.NewSession(t.Context(), request)
			requireUnsupportedField(t, err, field)
			require.Zero(t, starts)
		})
	}
}

func TestSessionEnvRefusesStructurallyInvalidEntries(t *testing.T) {
	tests := []struct {
		name string
		env  map[string]any
		key  string
	}{
		{"empty name", map[string]any{"": "x"}, ""},
		{"name carries an equals sign", map[string]any{"A=B": "x"}, "A=B"},
		{"name carries a NUL", map[string]any{"A\x00B": "x"}, "A\x00B"},
		{"value carries a NUL", map[string]any{"A": "x\x00y"}, "A"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := piOptionsFromMeta(envMeta(test.env))
			requireUnsupportedField(t, err, metaOptionPath(metaEnvKey)+"."+test.key)
		})
	}
}

func TestSessionEnvRefusesBlockedNamesUnderThePlatformIdentity(t *testing.T) {
	blocked := []string{envKeyPath, envKeyNodeOptions, envKeyBashEnv, envKeyEnv, "LD_PRELOAD", "DYLD_INSERT_LIBRARIES"}
	folded := []string{privateEnvPrefix + "CONTROL", strings.ToLower(privateEnvPrefix) + "control", pi.EnvExtraPathDirs, strings.ToLower(pi.EnvExtraPathDirs)}

	for _, platform := range []string{"linux", "windows"} {
		simulateSessionEnvPlatform(t, platform)

		for _, key := range append(blocked, folded...) {
			_, err := piOptionsFromMeta(envMeta(map[string]any{key: "x"}))
			requireUnsupportedField(t, err, metaOptionPath(metaEnvKey)+"."+key)
		}

		for _, key := range folded {
			require.True(t, blockedAgentEnvKey(key), key)
		}

		require.False(t, blockedAgentEnvKey(envKeyPath))
	}

	simulateSessionEnvPlatform(t, "windows")

	for _, key := range []string{"path", "Node_Options", "bash_env", "env", "ld_preload", "dyld_insert_libraries"} {
		_, err := piOptionsFromMeta(envMeta(map[string]any{key: "x"}))
		requireUnsupportedField(t, err, metaOptionPath(metaEnvKey)+"."+key)
	}
}

func TestSessionEnvReportsTheFirstKeyInSortedOrder(t *testing.T) {
	simulateSessionEnvPlatform(t, "linux")

	_, err := piOptionsFromMeta(envMeta(map[string]any{
		"ZZ_LAST":   "x\x00y",
		"AA_FIRST=": "x",
		"MM_MID":    "x",
	}))
	requireUnsupportedField(t, err, metaOptionPath(metaEnvKey)+".AA_FIRST=")
}

func TestSessionEnvRefusesTwoSpellingsOfOneWindowsVariable(t *testing.T) {
	env := map[string]any{"Https_Proxy": "a", "https_proxy": "b"}

	simulateSessionEnvPlatform(t, "linux")

	options, err := piOptionsFromMeta(envMeta(env))
	require.NoError(t, err)
	require.Len(t, options.Env, 2)

	simulateSessionEnvPlatform(t, "windows")

	_, err = piOptionsFromMeta(envMeta(env))
	require.Equal(t, ambiguousField(metaOptionPath(metaEnvKey)+".https_proxy"), err)
	require.Equal(t, ambiguousField(metaOptionPath(metaEnvKey)+".https_proxy"), ValidatePiSessionMeta(envMeta(env)))
}

func TestValidatePiSessionMetaMirrorsTheSessionParser(t *testing.T) {
	require.NoError(t, ValidatePiSessionMeta(nil))
	require.NoError(t, ValidatePiSessionMeta(NewPiOptions(
		WithPiEnv(map[string]string{"https_proxy": "", "WAGIE_API_TOKEN": "bearer"}),
		WithPiExtraPathDirs(absTestPath("session", "bin")),
	).Meta()))
	requireUnsupportedField(t, ValidatePiSessionMeta(envMeta(map[string]any{envKeyPath: "/bin"})), metaOptionPath(metaEnvKey)+"."+envKeyPath)
	requireUnsupportedField(t, ValidatePiSessionMeta(map[string]any{piMetaKey: map[string]any{metaOptionsKey: map[string]any{metaExtraPathDirsKey: []any{"relative"}}}}), metaOptionPath(metaExtraPathDirsKey)+"[0]")
}
